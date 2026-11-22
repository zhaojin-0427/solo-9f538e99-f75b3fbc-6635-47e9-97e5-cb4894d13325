"""CSV 原件解析与校验：两列（箱号, UTC过闸时刻），整体校验，全部通过才允许替换。

表头识别规则（仅第一行）：第二列不是合法时刻，且第一列是常见表头词或不符合箱号格式，
则视为表头跳过；否则按数据行处理并逐行校验，任一非法即整份拒绝。

- parse_csv：整批上传用，至少要有一条数据行，行数 ≤ MAX_ROWS。
- parse_csv_rows / parse_correction_csv：按时段更正用，允许“只有表头”的 CSV
  （此时返回空数据，表示清空所选时段）；同样整体校验、越界与超限由调用方判定。
返回三元组 (line_no, container, gate_time_utc)，保留 CSV 内原始行号便于报错。
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


def parse_utc_bound(raw: str, field: str) -> datetime:
    """解析时段更正的 UTC 边界时刻；要求显式 UTC（Z 或 +00:00），避免时区歧义。"""
    s = (raw or "").strip()
    if not s:
        raise CsvError(f"{field}不能为空")
    try:
        dt = (
            datetime.fromisoformat(s[:-1] + "+00:00") if s.endswith("Z")
            else datetime.fromisoformat(s)
        )
    except ValueError:
        raise CsvError(
            f"{field}无法解析（需 ISO 8601 UTC，如 2026-11-22T08:30:00Z）: {raw!r}"
        )
    if dt.tzinfo is None:
        raise CsvError(f"{field}必须带 UTC 时区标识（Z 或 +00:00）: {raw!r}")
    if dt.utcoffset().total_seconds() != 0:
        raise CsvError(f"{field}必须是 UTC 时刻（Z 或 +00:00），不接受其他时区: {raw!r}")
    return dt.astimezone(timezone.utc)


def _looks_like_header(container_raw: str, time_raw: str) -> bool:
    try:
        parse_gate_time(time_raw)
        return False  # 第二列是合法时刻 → 数据行
    except CsvError:
        pass
    c = (container_raw or "").strip()
    return c.lower() in HEADER_WORDS or not CONTAINER_RE.fullmatch(c.upper())


def parse_csv_rows(data: bytes) -> tuple[list[tuple[int, str, str]], bool]:
    """整体解析 CSV，返回 (rows, header_seen)；任何一行非法都整份拒绝。

    rows 元素为 (CSV 行号, 归一化箱号, 归一化 UTC 时刻)。
    空文件/仅空行时 header_seen=False、rows 为空。
    """
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as e:
        raise CsvError(f"文件须为 UTF-8 编码: {e}")
    rows: list[tuple[int, str, str]] = []
    header_seen = False
    for lineno, record in enumerate(csv.reader(io.StringIO(text)), start=1):
        if not record or all(not c.strip() for c in record):
            continue  # 跳过空行
        if len(record) != 2:
            raise CsvError(f"第 {lineno} 行应为 2 列（箱号,UTC过闸时刻），实际 {len(record)} 列")
        container_raw, time_raw = record
        if lineno == 1 and _looks_like_header(container_raw, time_raw):
            header_seen = True
            continue
        try:
            rows.append((lineno, normalize_container(container_raw), parse_gate_time(time_raw)))
        except CsvError as e:
            raise CsvError(f"第 {lineno} 行: {e}") from e
    return rows, header_seen


def parse_csv(data: bytes) -> list[tuple[str, str]]:
    """整批上传：返回 [(container, gate_time_utc), ...]；任何一行非法都整份拒绝。"""
    rows, _ = parse_csv_rows(data)
    if not rows:
        raise CsvError("CSV 中没有数据行")
    if len(rows) > MAX_ROWS:
        raise CsvError(f"共 {len(rows)} 行，超过单批上限 {MAX_ROWS} 行")
    return [(c, t) for _, c, t in rows]


def parse_correction_csv(data: bytes) -> tuple[list[tuple[int, str, str]], bool]:
    """按时段更正用：允许只有表头的 CSV（返回空 rows，用于清空时段）。

    仅含空行（连表头都没有）仍视为无效文件；替换行数本身也不得超过 MAX_ROWS。
    """
    rows, header_seen = parse_csv_rows(data)
    if not rows and not header_seen:
        raise CsvError("CSV 为空：更正文件至少要包含表头（只有表头即清空所选时段）")
    if len(rows) > MAX_ROWS:
        raise CsvError(f"替换数据共 {len(rows)} 行，超过单批上限 {MAX_ROWS} 行")
    return rows, header_seen
