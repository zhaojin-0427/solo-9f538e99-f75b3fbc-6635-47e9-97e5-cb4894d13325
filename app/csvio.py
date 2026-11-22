"""CSV 原件解析与校验：两列（箱号, UTC过闸时刻），整体校验，全部通过才允许替换。

表头识别规则（仅第一行）：第二列不是合法时刻，且第一列是常见表头词或不符合箱号格式，
则视为表头跳过；否则按数据行处理并逐行校验，任一非法即整份拒绝。
"""
from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone

MAX_ROWS = 5000
CONTAINER_RE = re.compile(r"^[A-Z0-9][A-Z0-9._-]{0,63}$")
HEADER_WORDS = {
    "container", "container_id", "containerid", "container_no", "cntr", "cntr_no",
    "id", "no", "箱号", "箱编号", "集装箱号",
}


class CsvError(ValueError):
    """CSV 内容非法（整份拒绝）。"""


def normalize_container(raw: str) -> str:
    c = (raw or "").strip().upper()
    if not CONTAINER_RE.fullmatch(c):
        raise CsvError(f"箱号非法（允许字母/数字/.-_，1~64 字符）: {raw!r}")
    return c


def parse_gate_time(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        raise CsvError("过闸时刻为空")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        raise CsvError(
            f"过闸时刻无法解析（需 ISO 8601，如 2026-11-22T08:30:00Z）: {raw!r}"
        )
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # 约定：无时区按 UTC
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _looks_like_header(container_raw: str, time_raw: str) -> bool:
    try:
        parse_gate_time(time_raw)
        return False  # 第二列是合法时刻 → 数据行
    except CsvError:
        pass
    c = (container_raw or "").strip()
    return c.lower() in HEADER_WORDS or not CONTAINER_RE.fullmatch(c.upper())


def parse_csv(data: bytes) -> list[tuple[str, str]]:
    """返回 [(container, gate_time_utc), ...]；任何一行非法都整份拒绝。"""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise CsvError(f"文件须为 UTF-8 编码: {e}")
    rows: list[tuple[str, str]] = []
    for lineno, record in enumerate(csv.reader(io.StringIO(text)), start=1):
        if not record or all(not c.strip() for c in record):
            continue  # 跳过空行
        if len(record) != 2:
            raise CsvError(f"第 {lineno} 行应为 2 列（箱号,UTC过闸时刻），实际 {len(record)} 列")
        container_raw, time_raw = record
        if lineno == 1 and _looks_like_header(container_raw, time_raw):
            continue
        try:
            rows.append((normalize_container(container_raw), parse_gate_time(time_raw)))
        except CsvError as e:
            raise CsvError(f"第 {lineno} 行: {e}") from e
    if not rows:
        raise CsvError("CSV 中没有数据行")
    if len(rows) > MAX_ROWS:
        raise CsvError(f"共 {len(rows)} 行，超过单批上限 {MAX_ROWS} 行")
    return rows
