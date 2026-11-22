#!/usr/bin/env python3
"""端到端验证脚本：直接以 Flask test_client 驱动应用（后台构建线程真实并发）。

运行：.venv/bin/python tests/test_flow.py
"""
from __future__ import annotations

import io
import os
import re
import sqlite3
import sys
import tempfile
import threading
import time

APP_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "app"))
sys.path.insert(0, APP_DIR)

TMP = tempfile.mkdtemp(prefix="gate-test-")
os.environ["DATABASE_PATH"] = os.path.join(TMP, "test.db")

import app as gate  # noqa: E402
from bloom import BloomFilter  # noqa: E402

client = gate.app.test_client()
CHECKS = []


def check(name, cond, detail=""):
    CHECKS.append((name, bool(cond)))
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail and not cond else ""))


def csv_bytes(pairs, header=None):
    lines = ([header] if header else []) + [f"{c},{t}" for c, t in pairs]
    return ("\n".join(lines) + "\n").encode()


def upload(batch, pairs=None, header=None, raw=None):
    data = raw if raw is not None else csv_bytes(pairs, header)
    return client.post(
        "/batches",
        data={"batch": batch, "file": (io.BytesIO(data), "f.csv")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def build(batch, delay=0.0):
    return client.post(
        f"/batches/{batch}/filter", data={"delay": str(delay)}, follow_redirects=True
    )


def query(container):
    return client.get(f"/query?container={container}&format=json").get_json()


def batch_info(batch):
    for b in client.get("/api/batches").get_json():
        if b["id"] == batch:
            return b
    return None


def wait_for(pred, timeout=10.0, what=""):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(0.05)
    raise AssertionError(f"超时等待: {what}")


def url_contains(resp, fragment):
    return fragment in resp.headers.get("Location", "")


def wait_build_done(batch):
    wait_for(
        lambda: batch_info(batch)["filter_status"] in ("ready", "failed"),
        what=f"{batch} 构建结束",
    )


def db_one(sql, args=()):
    conn = sqlite3.connect(os.environ["DATABASE_PATH"])
    try:
        return conn.execute(sql, args).fetchone()
    finally:
        conn.close()


T = "2026-11-22T08:30:00Z"

# ---------------------------------------------------------------- 1. 基本上传与查询
r = upload("B1", [(f"C{i:04d}", T) for i in range(1, 51)], header="箱号,过闸时刻(UTC)")
check("上传 B1（50 行，带中文表头）", "已上传" in r.get_data(as_text=True) and "v1" in r.get_data(as_text=True))
r = upload("B2", [(f"C{i:04d}", T) for i in range(40, 91)], header="container,gate_time_utc")
check("上传 B2（51 行，带英文表头）", "已上传" in r.get_data(as_text=True))

q = query("C0045")
check("无过滤器时查询：两批都扫描", {s["batch"] for s in q["scanned"]} == {"B1", "B2"} and not q["skipped"])
check("命中 B1、B2 各一行", {(m["batch"], m["line_no"]) for m in q["matches"]} == {("B1", 45), ("B2", 6)})
q = query("C9999")
check("查询不存在箱号：无命中、全部扫描", q["matches"] == [] and len(q["scanned"]) == 2)

# ---------------------------------------------------------------- 2. 建过滤器后查询
build("B1")
build("B2")
wait_build_done("B1")
wait_build_done("B2")
check("两批过滤器就绪", batch_info("B1")["filter_status"] == "ready" and batch_info("B2")["filter_status"] == "ready")

q = query("C9999")
check("过滤器判定不存在 → 两批都跳过、零命中", q["matches"] == [] and {s["batch"] for s in q["skipped"]} == {"B1", "B2"} and not q["scanned"])

# C0001 只在 B1；用同一算法预判 B2 过滤器是否放行（避免误报导致断言偶然失败）
bf_b2 = BloomFilter.build([f"C{i:04d}" for i in range(40, 91)])
expect_b2_scanned = "C0001" in bf_b2
q = query("C0001")
scanned = {s["batch"] for s in q["scanned"]}
check("命中行来自 B1 且只有一行", [(m["batch"], m["line_no"]) for m in q["matches"]] == [("B1", 1)])
check("B1 被扫描", "B1" in scanned)
check("B2 按过滤器结果跳过或扫描", ("B2" in scanned) == expect_b2_scanned)

# ---------------------------------------------------------------- 3. 整份替换更正
r = upload("B1", [(f"D{i:04d}", T) for i in range(1, 11)])
info = batch_info("B1")
check("更正后版本 v2、10 行", info["version"] == 2 and info["row_count"] == 10)
check("更正后旧过滤器失效", info["filter_status"] == "none" and info["filter_version"] is None)
q = query("C0001")
check("旧箱号查不到", q["matches"] == [])
q = query("D0005")
check("新箱号命中 B1 v2 第 5 行", [(m["batch"], m["version"], m["line_no"]) for m in q["matches"]] == [("B1", 2, 5)])
check("B1 无同版过滤器 → 扫描原件", any(s["batch"] == "B1" for s in q["scanned"]))

# ---------------------------------------------------------------- 4. 交错：建索引期间更正，迟到旧索引不得覆盖新版
r = upload("B4", [(f"E{i:04d}", T) for i in range(1, 31)])
build("B4", delay=1.2)  # 慢构建（测试钩子）
check("构建状态为 building", batch_info("B4")["filter_status"] == "building")
time.sleep(0.2)
r = upload("B4", [(f"F{i:04d}", T) for i in range(1, 21)])  # 构建期间更正 → v2
info = batch_info("B4")
check("建索引期间照常更正（v2 生效）", info["version"] == 2 and info["row_count"] == 20)
q = query("F0010")
check("建索引期间照常查询（命中新行）", [(m["batch"], m["version"]) for m in q["matches"]] == [("B4", 2)])
time.sleep(2.0)  # 等迟到的 v1 构建落地
info = batch_info("B4")
check("迟到旧索引未覆盖新版（filter_version 仍为空）",
      info["filter_version"] is None and info["filter_status"] == "none", str(info))
check("迟到的旧过滤器未入库", db_one("SELECT COUNT(*) FROM filters WHERE batch_id='B4'")[0] == 0)
q = query("E0001")
check("旧版本数据查不到", q["matches"] == [])
q = query("F0010")
check("更正后数据仍可查", len(q["matches"]) == 1)
build("B4")  # 为 v2 重新构建
wait_build_done("B4")
info = batch_info("B4")
check("v2 过滤器就绪且同版", info["filter_status"] == "ready" and info["filter_version"] == 2)
q = query("F9999")
check("v2 过滤器跳过不存在的箱号", any(s["batch"] == "B4" for s in q["skipped"]))

# ---------------------------------------------------------------- 5. 构建失败仍可查原件
orig_build = gate.BloomFilter.build
def boom(cls, items):
    raise RuntimeError("模拟构建失败")
gate.BloomFilter.build = classmethod(boom)
try:
    build("B1")
    wait_for(lambda: batch_info("B1")["filter_status"] == "failed", what="B1 构建失败标记")
finally:
    gate.BloomFilter.build = orig_build
info = batch_info("B1")
check("B1 构建标记为失败", info["filter_status"] == "failed" and info["filter_error"])
q = query("D0003")
check("构建失败仍可查原件（回退逐行扫描）",
      [(m["batch"], m["line_no"]) for m in q["matches"]] == [("B1", 3)]
      and any(s["batch"] == "B1" for s in q["scanned"]))
build("B1")  # 失败后可重试
wait_build_done("B1")
check("失败后重试成功", batch_info("B1")["filter_status"] == "ready")

# ---------------------------------------------------------------- 6. 行数上限与 CSV 校验
big = [(f"G{i:05d}", T) for i in range(1, 5001)]
r = upload("B6", big)
check("5000 行整批接受", "5000 行" in r.get_data(as_text=True))
r = upload("B7", big + [("G5001", T)])
check("5001 行整份拒绝", "超过单批上限" in r.get_data(as_text=True) and batch_info("B7") is None)
r = upload("B7", raw=b"ONLYONE,2026-11-22T08:30:00Z,EXTRA\n")
check("列数错误整份拒绝", "应为 2 列" in r.get_data(as_text=True))
r = upload("B7", raw="CSQU3054383,不是时间\n".encode())
check("时刻非法整份拒绝", "无法解析" in r.get_data(as_text=True))
r = upload("B7", raw=b"\n\n\n")
check("空文件拒绝", "没有数据行" in r.get_data(as_text=True))
r = client.post("/batches", data={"batch": "bad batch!", "file": (io.BytesIO(b"A,2026-11-22T00:00:00Z\n"), "x.csv")},
                content_type="multipart/form-data", follow_redirects=True)
check("批号非法拒绝", "批号非法" in r.get_data(as_text=True))

# ---------------------------------------------------------------- 7. 时区归一与过滤器体积
upload("B8", [("TZ0001", "2026-11-22T18:30:00+08:00"), ("TZ0002", "2026-11-22 10:30:00")])
q = query("TZ0001")
check("带时区时刻归一为 UTC", q["matches"][0]["gate_time_utc"] == "2026-11-22T10:30:00Z", str(q["matches"]))
q = query("TZ0002")
check("无时区时刻按 UTC", q["matches"][0]["gate_time_utc"] == "2026-11-22T10:30:00Z")

build("B6")
wait_build_done("B6")
nbytes = db_one("SELECT length(blob) FROM filters WHERE batch_id='B6'")[0]
check("5000 行批次过滤器 ≤ 2 KiB", 0 < nbytes <= 2048, f"nbytes={nbytes}")

# 布隆过滤器单元性质：无假阴性；误报率有界
bf = BloomFilter.build([f"G{i:05d}" for i in range(1, 5001)])
check("过滤器无假阴性（5000 项全命中）", all(f"G{i:05d}" in bf for i in range(1, 5001)))
fp = sum(1 for i in range(5001, 5201) if f"G{i:05d}" in bf)
check("误报率有界（抽 200 个非成员 < 50%）", fp < 100, f"fp={fp}/200")

# ---------------------------------------------------------------- 8. 并发压力：更正×建索引×查询交错，校验快照一致性
upload("B9", [(f"S{i:04d}", T) for i in range(1, 81)])
stop = threading.Event()
errors = []

def corrector():
    n = 0
    c = gate.app.test_client()
    while not stop.is_set():
        n += 1
        try:
            data = csv_bytes([(f"S{(i + n) % 80:04d}", T) for i in range(80)])
            c.post("/batches", data={"batch": "B9", "file": (io.BytesIO(data), "f.csv")},
                   content_type="multipart/form-data")
        except Exception as e:  # noqa: BLE001
            errors.append(f"corrector: {e}")
        time.sleep(0.02)

def builder():
    c = gate.app.test_client()
    while not stop.is_set():
        try:
            c.post("/batches/B9/filter", data={})
        except Exception as e:  # noqa: BLE001
            errors.append(f"builder: {e}")
        time.sleep(0.05)

threads = [threading.Thread(target=corrector), threading.Thread(target=builder)]
for t in threads:
    t.start()

consistent = True
try:
    for _ in range(60):
        q = query("S0005")
        seen = {}
        for s in q["scanned"] + q["skipped"]:
            if s["batch"] in seen:  # 同一批在一次查询中出现两次 → 快照不一致
                consistent = False
            seen[s["batch"]] = s["version"]
        scanned_v = {s["batch"]: s["version"] for s in q["scanned"]}
        skipped_b = {s["batch"] for s in q["skipped"]}
        for m in q["matches"]:
            # 命中只能来自被扫描的批次，且版本必须与扫描条目一致
            if m["batch"] not in scanned_v or m["version"] != scanned_v[m["batch"]]:
                consistent = False
            if m["batch"] in skipped_b:
                consistent = False
        time.sleep(0.03)
finally:
    stop.set()
    for t in threads:
        t.join()

check("交错压力下无内部异常", not errors, "; ".join(errors[:3]))
check("60 次查询均满足快照一致性（命中⊆已扫描、版本一致、批次不重复）", consistent)
info = batch_info("B9")
check("压力后 B9 状态收敛（非 building）", info["filter_status"] != "building", str(info))

# ---------------------------------------------------------------- 9. 按时段更正（相交/不相交时段交错提交）
def iso(hour, minute=0):
    return f"2026-11-22T{hour:02d}:{minute:02d}:00Z"


def seg_page(batch, start, end):
    return client.get(f"/batches/{batch}/segment?start={start}&end={end}",
                      follow_redirects=True)


def seg_dispatch(batch, start, end):
    return client.get("/segment", query_string={"batch": batch, "start": start, "end": end},
                      follow_redirects=False)


def seg_digest(batch, start, end):
    r = seg_page(batch, start, end)
    m = re.search(r'name="digest" value="([0-9a-f]{64})"', r.get_data(as_text=True))
    assert m, r.get_data(as_text=True)[:300]
    return m.group(1)


def correct(batch, start, end, digest, pairs=None, header=None, raw=None):
    data = raw if raw is not None else csv_bytes(pairs or [], header)
    return client.post(
        f"/batches/{batch}/correction",
        data={"start": start, "end": end, "digest": digest,
              "file": (io.BytesIO(data), "f.csv")},
        content_type="multipart/form-data",
        follow_redirects=True,
    )


def db_rows(batch):
    conn = sqlite3.connect(os.environ["DATABASE_PATH"])
    try:
        return conn.execute(
            "SELECT container, gate_time_utc FROM rows"
            " WHERE batch_id=? AND batch_version=(SELECT version FROM batches WHERE id=?)"
            " ORDER BY gate_time_utc, line_no", (batch, batch)
        ).fetchall()
    finally:
        conn.close()


# 9.0 准备批次 R1：8 行，时刻 01:00..08:00
base = [(f"R{i}", iso(i)) for i in range(1, 9)]
upload("R1", base)
check("时段更正基线：R1 v1 共 8 行", batch_info("R1")["version"] == 1 and batch_info("R1")["row_count"] == 8)

# 9.1 查看页与首页入口
r = seg_page("R1", iso(2), iso(5))
txt = r.get_data(as_text=True)
table = txt.split("<h2>该时段当前记录", 1)[1].split("</table>", 1)[0]
check("查看页表格列出区间内 3 行（02/03/04 点，左闭右开）",
      all(x in table for x in ("R2", "R3", "R4")) and "R5" not in table
      and "R1</td>" not in table and iso(5) not in table)
r = seg_dispatch("R1", iso(2), iso(5))
check("首页 /segment 入口 302 到规范查看页", r.status_code == 302 and "/batches/R1/segment" in r.headers["Location"])
r = seg_dispatch("R1", iso(5), iso(2))
check("起点≥终点被拒绝（重定向回首页）", r.status_code == 302 and url_contains(r, "/"))
r = seg_dispatch("R1", "2026-11-22T02:00:00", iso(5))  # 无时区
check("边界时刻缺时区标识被拒绝", r.status_code == 302)
r = seg_dispatch("R1", "2026-11-22T02:00:00+08:00", iso(5))  # 非 UTC 时区
check("非 UTC 时区边界被拒绝", r.status_code == 302)
r = client.get("/batches/R1/segment",
               query_string={"start": "2026-11-22T02:00:00+00:00",
                             "end": "2026-11-22T05:00:00+00:00"})
check("+00:00 形式的 UTC 边界可正常查看", r.status_code == 200 and "该时段当前记录" in r.get_data(as_text=True))
r = seg_page("NOPE", iso(0), iso(9))
check("不存在批次的查看页回到首页并报错", r.status_code == 200 and "不存在" in r.get_data(as_text=True))
d0 = seg_digest("R1", iso(2), iso(5))
check("区间指纹为 64 位十六进制", re.fullmatch(r"[0-9a-f]{64}", d0) is not None)

# 9.2 越界行整份拒收，失败不改动流水/版本
before = db_rows("R1")
r = correct("R1", iso(2), iso(5), d0, [("R2", iso(2)), ("R9", iso(5))])  # 05:00 == 终点，越界
check("终点时刻行判越界、整份拒收", "越出所选时段" in r.get_data(as_text=True))
r = correct("R1", iso(2), iso(5), d0, [("R2", iso(1)), ("R3", iso(3))])  # 01:00 < 起点，越界
check("起点之前行判越界、整份拒收", "越出所选时段" in r.get_data(as_text=True))
check("拒收后流水与版本均未改动", db_rows("R1") == before and batch_info("R1")["version"] == 1)

# 9.3 合法相交时段更正：替换 [02,05)，区间外保留
r = correct("R1", iso(2), iso(5), d0, [("N2", iso(2)), ("N3", iso(3, 30)), ("N4", iso(4))])
check("相交时段更正成功并升版 v2", "时段更正成功" in r.get_data(as_text=True) and batch_info("R1")["version"] == 2)
expect = [("R1", iso(1)), ("N2", iso(2)), ("N3", iso(3, 30)), ("N4", iso(4)),
          ("R5", iso(5)), ("R6", iso(6)), ("R7", iso(7)), ("R8", iso(8))]
check("合并结果 = 区间外最新流水 + 替换行（共 8 行）", db_rows("R1") == expect, str(db_rows("R1")))
check("升版后旧布隆过滤器失效", batch_info("R1")["filter_status"] == "none" and batch_info("R1")["filter_version"] is None)
q = query("R3")
check("查箱读到合并结果：旧行 R3 已消失", q["matches"] == [])
q = query("N3")
check("查箱读到合并结果：新行 N3 命中 v2",
      [(m["batch"], m["version"], m["gate_time_utc"]) for m in q["matches"]] == [("R1", 2, iso(3, 30))])

# 9.4 陈旧指纹：时段已被改动，提交被要求重新查看
r = correct("R1", iso(2), iso(5), d0, [("X2", iso(2))])
check("页面所见已过期 → 拒绝并提示重新查看", "已发生变化" in r.get_data(as_text=True) and "重新查看" in r.get_data(as_text=True))
check("过期提交不改动流水", db_rows("R1") == expect and batch_info("R1")["version"] == 2)

# 9.5 不相交时段交错提交：对一个始终未被触碰的时段，跨多轮不相交升版旧凭证仍有效；
#     一旦该时段内容真的变化，旧凭证立即失效
d2 = seg_digest("R1", iso(5), iso(7))  # [05,07) = R5,R6（本小节全程不直接改它）
r = correct("R1", iso(7), iso(9), seg_digest("R1", iso(7), iso(9)),
            [("N7", iso(7)), ("N8", iso(8))])  # 不相交时段 [07,09) 先改 → v3
check("不相交时段更正成功 v3", batch_info("R1")["version"] == 3)
r = correct("R1", iso(1), iso(2), seg_digest("R1", iso(1), iso(2)), [("M1", iso(1))])  # → v4
check("第二轮不相交更正成功 v4", batch_info("R1")["version"] == 4)
r = correct("R1", iso(2), iso(5), seg_digest("R1", iso(2), iso(5)),
            [("N2", iso(2)), ("N3", iso(3, 30)), ("N4", iso(4))])  # 仍不相交 → v5
check("第三轮不相交更正成功 v5", batch_info("R1")["version"] == 5)
r = correct("R1", iso(5), iso(7), d2, [("N5", iso(5)), ("N6", iso(6))])
check("时段内容始终未变 → 多轮升版后旧凭证依旧有效（v6）",
      "时段更正成功" in r.get_data(as_text=True) and batch_info("R1")["version"] == 6)
# 直接篡改库内容模拟“时段已变”，同一份旧凭证必须被拒
conn = sqlite3.connect(os.environ["DATABASE_PATH"])
try:
    conn.execute("UPDATE rows SET container='TAMPERED' WHERE container='N5'")
    conn.commit()
finally:
    conn.close()
r = correct("R1", iso(5), iso(7), d2, [("N5", iso(5))])
check("所选时段已变 → 拒绝并提示重新查看", "已发生变化" in r.get_data(as_text=True))
check("被拒后篡改内容保留、版本不动", batch_info("R1")["version"] == 6 and
      any(c == "TAMPERED" for c, _t in db_rows("R1")))
d3 = seg_digest("R1", iso(5), iso(7))  # 重新查看拿到新凭证
r = correct("R1", iso(5), iso(7), d3, [("N5", iso(5)), ("N6", iso(6))])
check("重新查看后提交成功（v7），区间外流水保留",
      "时段更正成功" in r.get_data(as_text=True) and batch_info("R1")["version"] == 7)
expect = [("M1", iso(1)), ("N2", iso(2)), ("N3", iso(3, 30)), ("N4", iso(4)),
          ("N5", iso(5)), ("N6", iso(6)), ("N7", iso(7)), ("N8", iso(8))]
check("多轮交错更正后的完整合并结果正确", db_rows("R1") == expect, str(db_rows("R1")))

# 9.6 同组多条重复行按条数核对（忽略行序）：两行同箱号同时刻
d4 = seg_digest("R1", iso(2), iso(3))
r = correct("R1", iso(2), iso(3), d4, [("DUP", iso(2, 30)), ("DUP", iso(2, 30))])
check("重复 (箱号,时刻) 组按两条接受", batch_info("R1")["version"] == 8)
q = query("DUP")
check("重复行均可查到（2 条）", len(q["matches"]) == 2 and all(m["version"] == 8 for m in q["matches"]))
d5 = seg_digest("R1", iso(2), iso(3))
r = correct("R1", iso(2), iso(3), d5, [("DUP", iso(2, 30))])  # 2 条 → 1 条
check("同组条数变化能被指纹识别并接受新内容", batch_info("R1")["version"] == 9)
q = query("DUP")
check("该组现在只剩 1 条", len(q["matches"]) == 1)

# 9.7 只有表头的 CSV 清空所选时段；空文件仍拒绝
d6 = seg_digest("R1", iso(5), iso(7))  # N5,N6
r = correct("R1", iso(5), iso(7), d6, header="箱号,UTC过闸时刻")
check("只有表头的 CSV 清空时段并升版", "已清空" in r.get_data(as_text=True) and batch_info("R1")["version"] == 10)
rows_now = db_rows("R1")
check("清空后该时段无记录、区间外保留（8-2=6 行）",
      [t for _c, t in rows_now].count(iso(5)) == 0 and [t for _c, t in rows_now].count(iso(6)) == 0
      and ("M1", iso(1)) in rows_now and ("N8", iso(8)) in rows_now and len(rows_now) == 6)
r = correct("R1", iso(5), iso(7), seg_digest("R1", iso(5), iso(7)), raw=b"\n\n\n")
check("仅空行的更正文件被拒绝", "至少要包含表头" in r.get_data(as_text=True))
check("拒绝清空操作不改动版本", batch_info("R1")["version"] == 10)
# 9.8 合并后 5000 行上限：清空大区间后再尝试超限合并
big_batch = "R2"
upload(big_batch, [(f"P{i:05d}", iso(1)) for i in range(1, 5001)])  # 5000 行同一时刻
dg = seg_digest(big_batch, iso(0), iso(2))
r = correct(big_batch, iso(0), iso(2), dg,
            [(f"P{i:05d}", iso(1)) for i in range(1, 5000)] + [("Q1", iso(1))])  # 5000 替换 → 合并 5000
check("合并后恰 5000 行接受", batch_info(big_batch)["version"] == 2 and batch_info(big_batch)["row_count"] == 5000)
dg = seg_digest(big_batch, iso(3), iso(4))  # 空区间
r = correct(big_batch, iso(3), iso(4), dg, [("Q2", iso(3))])  # 5000 + 1 = 5001
check("合并后 5001 行整份拒绝且流水不动",
      "超过单批上限" in r.get_data(as_text=True) and batch_info(big_batch)["row_count"] == 5000
      and batch_info(big_batch)["version"] == 2)
q = query("Q2")
check("超限拒绝后查不到新行", q["matches"] == [])
# 先清空一个含 1 行的时段腾出名额，再补 1 行 → 恰好 5000
dg = seg_digest(big_batch, iso(0), iso(2))
r = correct(big_batch, iso(0), iso(2), dg,
            [(f"P{i:05d}", iso(1)) for i in range(1, 5000)])  # 4999 行
check("先清空 1 行名额（4999+0=4999）", batch_info(big_batch)["row_count"] == 4999)
dg = seg_digest(big_batch, iso(3), iso(4))
r = correct(big_batch, iso(3), iso(4), dg, [("Q2", iso(3))])
check("腾出名额后补行成功，回到 5000 行", batch_info(big_batch)["row_count"] == 5000)
q = query("Q2")
check("新行可查、版本正确", [(m["batch"], m["version"]) for m in q["matches"]] == [(big_batch, 4)])

# 9.9 更正后重建过滤器，查箱立即走新索引
build("R1")
wait_build_done("R1")
check("更正后可重建同版过滤器", batch_info("R1")["filter_status"] == "ready"
      and batch_info("R1")["filter_version"] == batch_info("R1")["version"])
q = query("ZZZ9999")
check("新过滤器可跳过不可能命中的批次", any(s["batch"] == "R1" for s in q["skipped"]))
q = query("N8")
check("经合并保留的行仍能命中", len(q["matches"]) == 1 and q["matches"][0]["batch"] == "R1")

# 9.10 更正与整批上传交错：整批上传后旧更正凭证失效
d7 = seg_digest("R1", iso(1), iso(2))
v_before = batch_info("R1")["version"]
upload("R1", [("W1", iso(1)), ("W2", iso(2))])  # 整份替换 → 版本跳跃
check("整批上传后版本 +1", batch_info("R1")["version"] == v_before + 1)
r = correct("R1", iso(1), iso(2), d7, [("W9", iso(1))])
check("整批上传后旧时段凭证被拒", "已发生变化" in r.get_data(as_text=True))
check("被拒后整批上传的内容不动", db_rows("R1") == [("W1", iso(1)), ("W2", iso(2))])
d8 = seg_digest("R1", iso(1), iso(2))
r = correct("R1", iso(1), iso(2), d8, [("W9", iso(1, 30))])
check("重新查看后可在整批替换的新版本上继续时段更正",
      db_rows("R1") == [("W9", iso(1, 30)), ("W2", iso(2))])

# 9.11 凭证缺失/伪造被拒
r = correct("R1", iso(1), iso(2), "deadbeef", [("W0", iso(1))])
check("非法指纹凭证被拒", "查看凭证" in r.get_data(as_text=True))
r = client.post("/batches/R1/correction",
                data={"start": iso(1), "end": iso(2), "digest": seg_digest("R1", iso(1), iso(2))},
                follow_redirects=True)  # 不带文件
check("缺少 CSV 文件被拒", "缺少 CSV" in r.get_data(as_text=True))

# ---------------------------------------------------------------- 汇总
failed = [n for n, ok in CHECKS if not ok]
print(f"\n{len(CHECKS) - len(failed)}/{len(CHECKS)} 项通过")
if failed:
    print("失败项:", *failed, sep="\n  - ")
    sys.exit(1)
print("全部通过")
