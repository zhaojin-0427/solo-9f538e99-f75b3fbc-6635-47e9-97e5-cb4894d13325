"""港区集装箱过闸记录查询网页（Flask + SQLite）。

并发与一致性设计：
- 一次查询 = 一个读事务：批次指针、布隆过滤器、原件行来自同一已提交快照。
- 更正 = 整份替换：一个写事务内版本 +1、删旧行、写新行、作废旧过滤器。
- 过滤器后台构建：构建期间同批照常更正/查询；提交时仅当批次仍停留在
  构建时的版本才启用（条件更新），迟到的旧索引直接丢弃，不覆盖新版。
- 构建失败只标记状态，查询自动回退为逐行扫描原件。
"""
from __future__ import annotations

import hashlib
import os
import re
import threading
import time
from datetime import datetime, timezone

from flask import Flask, flash, g, jsonify, redirect, render_template, request, url_for

from bloom import BloomFilter
from csvio import (
    MAX_ROWS,
    CsvError,
    normalize_container,
    parse_correction_csv,
    parse_csv,
    parse_utc_bound,
)
from db import connect, init_db

DATABASE = os.environ.get(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "gate.db"),
)
MAX_FILTER_BYTES = 2048      # 布隆过滤器上限 2 KiB
MAX_UPLOAD_BYTES = 1_000_000 # 上传文件体积上限（5000 行 CSV 远低于此）
MAX_BUILD_DELAY = 10.0       # 测试钩子：构建前人为延迟上限（秒）

BATCH_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-only-secret-change-me")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def get_db():
    if "db" not in g:
        g.db = connect(DATABASE)
    return g.db


@app.teardown_appcontext
def close_db(_exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------- 页面

@app.get("/")
def index():
    rows = get_db().execute(
        """
        SELECT b.*, f.m_bits, f.k_hashes, f.items, length(f.blob) AS nbytes
        FROM batches b
        LEFT JOIN filters f
               ON f.batch_id = b.id AND f.version = b.filter_version AND f.status = 'ready'
        ORDER BY b.id
        """
    ).fetchall()
    return render_template(
        "index.html", batches=rows, max_rows=MAX_ROWS, max_filter=MAX_FILTER_BYTES
    )


@app.post("/batches")
def upload_batch():
    """按批号上传 CSV；同批号即更正，整份替换。"""
    batch_id = (request.form.get("batch") or "").strip()
    if not BATCH_RE.fullmatch(batch_id):
        flash(f"批号非法（允许字母/数字/_/-，1~64 字符）: {batch_id!r}", "error")
        return redirect(url_for("index"))
    f = request.files.get("file")
    if f is None:
        flash("缺少 CSV 文件", "error")
        return redirect(url_for("index"))
    data = f.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        flash(f"批次 {batch_id} 上传被拒绝：文件超过 {MAX_UPLOAD_BYTES} 字节", "error")
        return redirect(url_for("index"))
    try:
        rows = parse_csv(data)  # 先整体校验，再落库
    except CsvError as e:
        flash(f"批次 {batch_id} 上传被拒绝：{e}", "error")
        return redirect(url_for("index"))

    db = get_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        db.execute(
            """
            INSERT INTO batches (id, version, row_count, uploaded_at,
                                 filter_version, filter_status, building_version, filter_error)
            VALUES (?, 1, ?, ?, NULL, 'none', NULL, NULL)
            ON CONFLICT(id) DO UPDATE SET
                version          = batches.version + 1,
                row_count        = excluded.row_count,
                uploaded_at      = excluded.uploaded_at,
                filter_version   = NULL,
                filter_status    = 'none',
                building_version = NULL,
                filter_error     = NULL
            """,
            (batch_id, len(rows), utcnow()),
        )
        version = db.execute(
            "SELECT version FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()["version"]
        db.execute("DELETE FROM rows WHERE batch_id = ?", (batch_id,))
        db.executemany(
            "INSERT INTO rows (batch_id, batch_version, line_no, container, gate_time_utc)"
            " VALUES (?, ?, ?, ?, ?)",
            [(batch_id, version, i, c, t) for i, (c, t) in enumerate(rows, start=1)],
        )
        # 旧版本过滤器一律清除（若有正在进行的旧构建，其提交会被条件更新拒绝）
        db.execute(
            "DELETE FROM filters WHERE batch_id = ? AND version <> ?", (batch_id, version)
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    flash(
        f"批次 {batch_id} 已上传：v{version}，{len(rows)} 行（整份替换，旧过滤器已失效）",
        "ok",
    )
    return redirect(url_for("index"))


# ---------------------------------------------------------------- 按时段更正

def _bound_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _row_dt(gate_time_utc: str) -> datetime:
    """库内归一化时刻（…Z）转回可比较的 UTC datetime。"""
    return datetime.fromisoformat(gate_time_utc.replace("Z", "+00:00"))


def _fetch_segment(db, batch_id: str, start: datetime, end: datetime) -> list[dict]:
    """在一个读事务快照内取出 [start, end) 的原件行（起点计入、终点不计入）。"""
    db.execute("BEGIN")
    try:
        rows = db.execute(
            "SELECT line_no, container, gate_time_utc FROM rows"
            " WHERE batch_id = ? AND batch_version = (SELECT version FROM batches WHERE id = ?)"
            " ORDER BY gate_time_utc, line_no",
            (batch_id, batch_id),
        ).fetchall()
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    lo, hi = start, end
    return [
        dict(r) for r in rows if lo <= _row_dt(r["gate_time_utc"]) < hi
    ]


def _segment_digest(batch_id: str, start_iso: str, end_iso: str,
                    segment_rows: list[dict]) -> str:
    """页面所见区间内容的指纹：按 (箱号, UTC时刻) 分组计数，忽略行序。

    指纹同时绑定批号与 UTC 起止时刻；提交时与库中最新区间重算值逐组核对。
    """
    groups: dict[tuple[str, str], int] = {}
    for r in segment_rows:
        key = (r["container"], r["gate_time_utc"])
        groups[key] = groups.get(key, 0) + 1
    h = hashlib.sha256()
    h.update(f"segment|{batch_id}|{start_iso}|{end_iso}\n".encode("utf-8"))
    for (container, gate_time), count in sorted(groups.items()):
        h.update(f"{container}|{gate_time}|{count}\n".encode("utf-8"))
    return h.hexdigest()


@app.get("/segment")
def segment_dispatch():
    """首页表单入口：校验批号与 UTC 起止时刻后跳转到规范的时段查看页。"""
    batch_id = (request.args.get("batch") or "").strip()
    if not BATCH_RE.fullmatch(batch_id):
        flash(f"批号非法（允许字母/数字/_/-，1~64 字符）: {batch_id!r}", "error")
        return redirect(url_for("index"))
    try:
        start = parse_utc_bound(request.args.get("start", ""), "起点时刻")
        end = parse_utc_bound(request.args.get("end", ""), "终点时刻")
    except CsvError as e:
        flash(str(e), "error")
        return redirect(url_for("index"))
    if not start < end:
        flash("时段非法：起点必须早于终点（起点计入、终点不计入）", "error")
        return redirect(url_for("index"))
    return redirect(
        url_for(
            "segment_view",
            batch_id=batch_id,
            start=_bound_iso(start),
            end=_bound_iso(end),
        )
    )


@app.get("/batches/<batch_id>/segment")
def segment_view(batch_id: str):
    """查看所选时段 [start, end) 的原件行，并给出提交更正所需的区间指纹。"""
    try:
        start = parse_utc_bound(request.args.get("start", ""), "起点时刻")
        end = parse_utc_bound(request.args.get("end", ""), "终点时刻")
    except CsvError as e:
        flash(str(e), "error")
        return redirect(url_for("index"))
    if not start < end:
        flash("时段非法：起点必须早于终点（起点计入、终点不计入）", "error")
        return redirect(url_for("index"))
    db = get_db()
    batch = db.execute(
        "SELECT id, version, row_count FROM batches WHERE id = ?", (batch_id,)
    ).fetchone()
    if batch is None:
        flash(f"批次 {batch_id} 不存在，请先上传", "error")
        return redirect(url_for("index"))
    start_iso, end_iso = _bound_iso(start), _bound_iso(end)
    seg = _fetch_segment(db, batch_id, start, end)
    digest = _segment_digest(batch_id, start_iso, end_iso, seg)
    return render_template(
        "segment.html",
        batch=dict(batch),
        start_iso=start_iso,
        end_iso=end_iso,
        seg=seg,
        digest=digest,
        max_rows=MAX_ROWS,
        max_upload=MAX_UPLOAD_BYTES,
    )


@app.post("/batches/<batch_id>/correction")
def correct_segment(batch_id: str):
    """按时段整段替换：校验 → 区间指纹比对 → 单事务合并升版。

    任何一步失败都在事务外/回滚后返回，流水与索引均不改动。
    """
    if not BATCH_RE.fullmatch(batch_id):
        flash(f"批号非法: {batch_id!r}", "error")
        return redirect(url_for("index"))
    try:
        start = parse_utc_bound(request.form.get("start", ""), "起点时刻")
        end = parse_utc_bound(request.form.get("end", ""), "终点时刻")
    except CsvError as e:
        flash(str(e), "error")
        return redirect(url_for("index"))
    if not start < end:
        flash("时段非法：起点必须早于终点（起点计入、终点不计入）", "error")
        return redirect(url_for("index"))
    digest = (request.form.get("digest") or "").strip()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        flash("缺少有效的查看凭证，请先重新查看所选时段再提交", "error")
        return redirect(url_for("index"))
    f = request.files.get("file")
    if f is None:
        flash("缺少 CSV 文件", "error")
        return redirect(url_for("index"))
    data = f.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        flash(f"更正被拒绝：文件超过 {MAX_UPLOAD_BYTES} 字节", "error")
        return redirect(url_for("index"))
    try:
        new_rows, header_seen = parse_correction_csv(data)  # 先整体校验
    except CsvError as e:
        flash(f"批次 {batch_id} 更正被拒绝：{e}", "error")
        return redirect(url_for("index"))

    start_iso, end_iso = _bound_iso(start), _bound_iso(end)
    # 越界行整份拒收（起点计入、终点不计入）
    for lineno, _container, gate_time in new_rows:
        ts = _row_dt(gate_time)
        if not (start <= ts < end):
            flash(
                f"批次 {batch_id} 更正被拒绝：第 {lineno} 行时刻 {gate_time}"
                f" 越出所选时段 [{start_iso}, {end_iso})，整份拒收",
                "error",
            )
            return redirect(url_for("index"))

    db = get_db()
    try:
        db.execute("BEGIN IMMEDIATE")
        batch = db.execute(
            "SELECT version, row_count FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if batch is None:
            db.execute("ROLLBACK")
            flash(f"批次 {batch_id} 不存在，请先上传", "error")
            return redirect(url_for("index"))
        old_version = batch["version"]
        current = db.execute(
            "SELECT line_no, container, gate_time_utc FROM rows"
            " WHERE batch_id = ? AND batch_version = ? ORDER BY line_no",
            (batch_id, old_version),
        ).fetchall()
        lo, hi = start, end
        inside = [dict(r) for r in current if lo <= _row_dt(r["gate_time_utc"]) < hi]
        outside = [dict(r) for r in current if not (lo <= _row_dt(r["gate_time_utc"]) < hi)]

        # 提交时比较页面所见与库中最新区间内容（按箱号+UTC时刻逐组核对条数，忽略行序）
        latest_digest = _segment_digest(batch_id, start_iso, end_iso, inside)
        if latest_digest != digest:
            db.execute("ROLLBACK")
            flash(
                "所选时段在您查看后已发生变化，为避免覆盖他人更正，请重新查看该时段后再提交",
                "error",
            )
            return redirect(
                url_for(
                    "segment_view", batch_id=batch_id, start=start_iso, end=end_iso
                )
            )

        merged = outside + [
            {"container": c, "gate_time_utc": t} for _ln, c, t in new_rows
        ]
        if len(merged) > MAX_ROWS:
            db.execute("ROLLBACK")
            flash(
                f"批次 {batch_id} 更正被拒绝：合并后共 {len(merged)} 行"
                f"（区间外保留 {len(outside)} 行 + 替换 {len(new_rows)} 行），"
                f"超过单批上限 {MAX_ROWS} 行；流水未改动",
                "error",
            )
            return redirect(url_for("index"))

        new_version = old_version + 1
        db.execute(
            "UPDATE batches SET version = ?, row_count = ?, uploaded_at = ?,"
            " filter_version = NULL, filter_status = 'none',"
            " building_version = NULL, filter_error = NULL WHERE id = ?",
            (new_version, len(merged), utcnow(), batch_id),
        )
        db.execute("DELETE FROM rows WHERE batch_id = ?", (batch_id,))
        db.executemany(
            "INSERT INTO rows (batch_id, batch_version, line_no, container, gate_time_utc)"
            " VALUES (?, ?, ?, ?, ?)",
            [
                (batch_id, new_version, i, r["container"], r["gate_time_utc"])
                for i, r in enumerate(merged, start=1)
            ],
        )
        # 旧布隆过滤器随升版同时失效（若有旧构建在途，其条件提交会被拒绝）
        db.execute(
            "DELETE FROM filters WHERE batch_id = ? AND version <> ?",
            (batch_id, new_version),
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise

    if not new_rows and header_seen:
        flash(
            f"批次 {batch_id} 时段 [{start_iso}, {end_iso}) 已清空：升版 v{new_version}，"
            f"现共 {len(merged)} 行（区间外流水保留，旧过滤器已失效）",
            "ok",
        )
    else:
        flash(
            f"批次 {batch_id} 时段更正成功：v{old_version} → v{new_version}，"
            f"替换 {len(inside)} 行为 {len(new_rows)} 行，区间外保留 {len(outside)} 行，"
            f"现共 {len(merged)} 行（旧过滤器已失效）",
            "ok",
        )
    return redirect(
        url_for("segment_view", batch_id=batch_id, start=start_iso, end=end_iso)
    )


@app.post("/batches/<batch_id>/filter")
def build_filter(batch_id: str):
    """为当前版本原件后台构建布隆过滤器（≤2 KiB）。

    表单字段 delay 为测试钩子：构建前人为延迟秒数，用于演练
    “建索引与更正交错”的场景，上限 MAX_BUILD_DELAY。
    """
    delay = 0.0
    raw_delay = (request.form.get("delay") or "").strip()
    if raw_delay:
        try:
            delay = min(MAX_BUILD_DELAY, max(0.0, float(raw_delay)))
        except ValueError:
            delay = 0.0

    db = get_db()
    db.execute("BEGIN IMMEDIATE")
    try:
        row = db.execute(
            "SELECT version, building_version FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if row is None:
            db.execute("ROLLBACK")
            flash(f"批次 {batch_id} 不存在", "error")
            return redirect(url_for("index"))
        version = row["version"]
        if row["building_version"] is not None:
            db.execute("ROLLBACK")
            flash(f"批次 {batch_id} 已有构建任务在进行（v{row['building_version']}）", "error")
            return redirect(url_for("index"))
        db.execute(
            "UPDATE batches SET building_version = ?, filter_status = 'building',"
            " filter_error = NULL WHERE id = ?",
            (version, batch_id),
        )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise

    t = threading.Thread(
        target=_build_worker,
        args=(DATABASE, batch_id, version, delay),
        daemon=True,
        name=f"build-{batch_id}-v{version}",
    )
    t.start()
    flash(f"批次 {batch_id} v{version} 布隆过滤器开始后台构建", "ok")
    return redirect(url_for("index"))


@app.get("/query")
def query():
    raw = request.args.get("container", "")
    wants_json = request.args.get("format") == "json"
    try:
        container = normalize_container(raw)
    except CsvError as e:
        if wants_json:
            return jsonify({"error": str(e)}), 400
        flash(str(e), "error")
        return redirect(url_for("index"))
    result = run_query(get_db(), container)
    if wants_json:
        return jsonify(result)
    return render_template("query.html", r=result)


@app.get("/api/batches")
def api_batches():
    rows = get_db().execute(
        "SELECT id, version, row_count, uploaded_at, filter_status, filter_version,"
        " building_version, filter_error FROM batches ORDER BY id"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


# ---------------------------------------------------------------- 查询核心

def run_query(db, container: str) -> dict:
    """按箱号查询：列出命中行与实际扫描过的批次。

    整个查询在一个读事务内完成 —— 批次版本、过滤器、原件行均来自
    同一已提交快照；期间发生的更正/建索引对本查询不可见。
    """
    db.execute("BEGIN")
    try:
        batches = db.execute(
            "SELECT id, version, row_count, filter_version FROM batches ORDER BY id"
        ).fetchall()
        scanned, skipped, matches = [], [], []
        for b in batches:
            bid, ver = b["id"], b["version"]
            entry = {"batch": bid, "version": ver}
            bf = None
            if b["filter_version"] == ver:
                frow = db.execute(
                    "SELECT m_bits, k_hashes, blob FROM filters"
                    " WHERE batch_id = ? AND version = ? AND status = 'ready'",
                    (bid, ver),
                ).fetchone()
                if frow is not None:
                    bf = BloomFilter(frow["m_bits"], frow["k_hashes"], bytes(frow["blob"]))
            if bf is not None and container not in bf:
                # 同版过滤器判定不存在 → 跳过原件
                skipped.append(entry)
                continue
            # 无同版过滤器（未建/构建中/已失败）或过滤器不能排除 → 逐行核对原件
            hits = db.execute(
                "SELECT line_no, gate_time_utc FROM rows"
                " WHERE batch_id = ? AND batch_version = ? AND container = ?"
                " ORDER BY line_no",
                (bid, ver, container),
            ).fetchall()
            entry["rows_scanned"] = b["row_count"]
            scanned.append(entry)
            for h in hits:
                matches.append(
                    {
                        "batch": bid,
                        "version": ver,
                        "line_no": h["line_no"],
                        "container": container,
                        "gate_time_utc": h["gate_time_utc"],
                    }
                )
        db.execute("COMMIT")
    except Exception:
        db.execute("ROLLBACK")
        raise
    return {
        "container": container,
        "matches": matches,
        "scanned": scanned,
        "skipped": skipped,
    }


# ---------------------------------------------------------------- 后台构建

def _build_worker(db_path: str, batch_id: str, version: int, delay: float) -> None:
    try:
        if delay > 0:
            time.sleep(delay)  # 测试钩子：模拟慢构建
        conn = connect(db_path)
        try:
            # 1) 同一已提交快照内读出该版本全部箱号
            conn.execute("BEGIN")
            try:
                containers = [
                    r["container"]
                    for r in conn.execute(
                        "SELECT container FROM rows"
                        " WHERE batch_id = ? AND batch_version = ? ORDER BY line_no",
                        (batch_id, version),
                    )
                ]
            finally:
                conn.execute("COMMIT")
            if not containers:
                raise RuntimeError("该版本没有可索引的行（可能已被更正替换）")
            # 2) 纯内存构建，不占用数据库
            bf = BloomFilter.build(containers)
            if bf.nbytes > MAX_FILTER_BYTES:
                raise RuntimeError(f"过滤器 {bf.nbytes} 字节超过 2 KiB 上限")
            # 3) 条件提交：批次仍停留在该版本才启用；迟到的旧索引直接丢弃
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE batches SET filter_version = ?, filter_status = 'ready',"
                    " building_version = NULL, filter_error = NULL"
                    " WHERE id = ? AND version = ? AND building_version = ?",
                    (version, batch_id, version, version),
                )
                if cur.rowcount == 1:
                    conn.execute(
                        "INSERT OR REPLACE INTO filters"
                        " (batch_id, version, m_bits, k_hashes, items, blob, built_at, status, error)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, 'ready', NULL)",
                        (batch_id, version, bf.m_bits, bf.k, len(containers), bf.bits, utcnow()),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    except Exception as exc:  # 构建失败不影响原件查询，仅标记状态
        _mark_build_failed(db_path, batch_id, version, exc)


def _mark_build_failed(db_path: str, batch_id: str, version: int, exc: Exception) -> None:
    try:
        conn = connect(db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "UPDATE batches SET filter_status = 'failed', building_version = NULL,"
                    " filter_error = ? WHERE id = ? AND version = ?",
                    (f"{type(exc).__name__}: {exc}", batch_id, version),
                )
                if cur.rowcount == 1:
                    conn.execute(
                        "INSERT OR REPLACE INTO filters"
                        " (batch_id, version, m_bits, k_hashes, items, blob, built_at, status, error)"
                        " VALUES (?, ?, 0, 0, 0, X'', ?, 'failed', ?)",
                        (batch_id, version, utcnow(), str(exc)),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        finally:
            conn.close()
    except Exception:
        app.logger.exception("标记构建失败时出错（批次 %s v%s）", batch_id, version)


# ---------------------------------------------------------------- 启动

def main() -> None:
    init_db(DATABASE)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")), threaded=True)


init_db(DATABASE)  # 覆盖 `python app.py` 与 WSGI 容器两种启动方式

if __name__ == "__main__":
    main()
