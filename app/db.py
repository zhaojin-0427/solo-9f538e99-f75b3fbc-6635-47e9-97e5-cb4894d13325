"""SQLite 连接与表结构。

- batches: 每个批号的当前版本、行数、过滤器指针（filter_version/building_version）
- rows:    原件行，按 (batch_id, batch_version, line_no) 存放；更正即整批换版本
- filters: 布隆过滤器，按 (batch_id, version) 存放；只有与批次当前版本同版才被使用

连接统一开 WAL：读写互不阻塞，读事务拿到一致快照。
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id               TEXT PRIMARY KEY,
    version          INTEGER NOT NULL,
    row_count        INTEGER NOT NULL,
    uploaded_at      TEXT NOT NULL,
    filter_version   INTEGER,                   -- 当前可用过滤器对应的批次版本
    filter_status    TEXT NOT NULL DEFAULT 'none',  -- none/building/ready/failed
    building_version INTEGER,                   -- 正在构建过滤器的版本
    filter_error     TEXT
);

CREATE TABLE IF NOT EXISTS rows (
    batch_id      TEXT NOT NULL,
    batch_version INTEGER NOT NULL,
    line_no       INTEGER NOT NULL,
    container     TEXT NOT NULL,
    gate_time_utc TEXT NOT NULL,
    PRIMARY KEY (batch_id, batch_version, line_no)
);

CREATE INDEX IF NOT EXISTS idx_rows_container ON rows (container);

CREATE TABLE IF NOT EXISTS filters (
    batch_id  TEXT NOT NULL,
    version   INTEGER NOT NULL,
    m_bits    INTEGER NOT NULL,
    k_hashes  INTEGER NOT NULL,
    items     INTEGER NOT NULL,
    blob      BLOB NOT NULL,
    built_at  TEXT NOT NULL,
    status    TEXT NOT NULL,                    -- ready / failed
    error     TEXT,
    PRIMARY KEY (batch_id, version)
);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # isolation_level=None：显式管理事务，保证“一次查询一个快照”可控
    conn = sqlite3.connect(db_path, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db(db_path: str) -> None:
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()
