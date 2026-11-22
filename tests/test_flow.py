#!/usr/bin/env python3
"""端到端验证脚本：直接以 Flask test_client 驱动应用（后台构建线程真实并发）。

运行：.venv/bin/python tests/test_flow.py
"""
from __future__ import annotations

import io
import os
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

# ---------------------------------------------------------------- 汇总
failed = [n for n, ok in CHECKS if not ok]
print(f"\n{len(CHECKS) - len(failed)}/{len(CHECKS)} 项通过")
if failed:
    print("失败项:", *failed, sep="\n  - ")
    sys.exit(1)
print("全部通过")
