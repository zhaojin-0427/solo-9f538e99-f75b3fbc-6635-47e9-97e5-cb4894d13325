"""纯标准库布隆过滤器，位图不超过 2 KiB。

哈希方案：sha256 拆成两个 128 位整数做双重哈希（Kirsch-Mitzenmacher），
h_i(x) = (h1 + i*h2) mod m。位图大小按元素数约 8 bit/项取值，封顶 2048 字节。
"""
from __future__ import annotations

import hashlib
import math
from typing import Iterable

MAX_BYTES = 2048  # 2 KiB 上限
ALGO_VERSION = "sha256dbl1"


def _positions(item: str, m_bits: int, k: int):
    digest = hashlib.sha256(item.encode("utf-8")).digest()
    h1 = int.from_bytes(digest[:16], "big")
    h2 = int.from_bytes(digest[16:], "big") | 1  # 避免 h2 为 0 退化
    for i in range(k):
        yield (h1 + i * h2) % m_bits


class BloomFilter:
    __slots__ = ("m_bits", "k", "bits")

    def __init__(self, m_bits: int, k: int, bits: bytes):
        self.m_bits = m_bits
        self.k = k
        self.bits = bits

    @classmethod
    def build(cls, items: Iterable[str]) -> "BloomFilter":
        items = list(items)
        n = max(1, len(items))
        m_bits = min(MAX_BYTES * 8, max(64, n * 8))
        m_bits -= m_bits % 8
        k = max(1, round(m_bits / n * math.log(2)))
        bits = bytearray(m_bits // 8)
        for item in items:
            for pos in _positions(item, m_bits, k):
                bits[pos >> 3] |= 1 << (pos & 7)
        return cls(m_bits, k, bytes(bits))

    def __contains__(self, item: str) -> bool:
        bits = self.bits
        for pos in _positions(item, self.m_bits, self.k):
            if not (bits[pos >> 3] >> (pos & 7)) & 1:
                return False
        return True

    @property
    def nbytes(self) -> int:
        return len(self.bits)
