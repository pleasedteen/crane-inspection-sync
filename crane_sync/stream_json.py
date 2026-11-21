"""流式 JSON 对象解析（仅标准库）。

同一秒集中到达的巡检单正文可能有百 MiB 级，整份 ``resp.read()`` 再
``json.loads`` 会让峰值内存随响应大小线性增长。本模块按 JSON 结构增量读取：

- 顶层对象逐字段解析；
- ``orders`` 数组逐元素交给回调，处理完立即释放，不保留整页/整轮数据；
- ``entries``（游标清单）只统计条目数，不保留条目；
- 其余标量/小对象字段常规解析后返回。

峰值内存只取决于读取块大小与单条记录的大小，与响应总大小无关。
"""
from __future__ import annotations

import codecs
import json
from typing import Any, Callable

_WS = " \t\r\n"
DEFAULT_CHUNK = 64 * 1024


class StreamAborted(Exception):
    """流式解析被取消（例如同步被中止），停止继续读取。"""


class _Stream:
    def __init__(self, fp, chunk_size: int = DEFAULT_CHUNK, cancel=None):
        self._fp = fp
        self._cancel = cancel
        self._chunk = max(1, int(chunk_size))
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._json = json.JSONDecoder()
        self._buf = ""
        self._pos = 0
        self._eof = False

    # ---------- 缓冲区 ----------
    def _compact(self) -> None:
        """丢弃已消费的前缀，释放其内存。"""
        if self._pos:
            self._buf = self._buf[self._pos:]
            self._pos = 0

    def _check_cancel(self) -> None:
        if self._cancel is not None and self._cancel.is_set():
            raise StreamAborted("流式读取已取消")

    def _fill(self) -> bool:
        """再读一块数据；返回是否读到了新内容。"""
        if self._eof:
            return False
        self._check_cancel()
        if self._pos:
            self._buf = self._buf[self._pos:]
            self._pos = 0
        while True:
            data = self._fp.read(self._chunk)
            if not data:
                self._eof = True
                tail = self._decoder.decode(b"", final=True)
                if tail:
                    self._buf += tail
                return False
            text = self._decoder.decode(data)
            if text:
                self._buf += text
                return True

    def _peek(self) -> str | None:
        while True:
            while self._pos < len(self._buf) and self._buf[self._pos] in _WS:
                self._pos += 1
            if self._pos < len(self._buf):
                return self._buf[self._pos]
            if not self._fill():
                return None

    def _get(self) -> str:
        ch = self._peek()
        if ch is None:
            raise ValueError("JSON 响应意外结束")
        self._pos += 1
        return ch

    def _expect(self, ch: str) -> None:
        got = self._get()
        if got != ch:
            raise ValueError(f"JSON 响应格式错误：期望 {ch!r}，得到 {got!r}")

    def _read_more(self) -> bool:
        """追加数据但不丢弃缓冲区（当前值尚未解析完时使用）。

        每次追加已缓冲长度的一半：大记录只需 log 次重试，总扫描量与记录
        大小成正比；缓冲区最多超出当前记录约 1.5 倍，避免一次读入过多。
        """
        if self._eof:
            return False
        self._check_cancel()
        pending = len(self._buf) - self._pos
        want = max(self._chunk, pending // 2)
        while True:
            data = self._fp.read(want)
            if not data:
                self._eof = True
                tail = self._decoder.decode(b"", final=True)
                if tail:
                    self._buf += tail
                return False
            text = self._decoder.decode(data)
            if text:
                self._buf += text
                return True

    # ---------- 值 ----------
    def _read_raw_value(self) -> Any:
        """读取一个完整 JSON 值（值跨多个读块时继续增量读取）。"""
        value, _size = self._read_raw_value_sized()
        return value

    def _read_raw_value_sized(self) -> tuple[Any, int]:
        """同 ``_read_raw_value``，并返回该值的原始字符长度。"""
        if self._peek() is None:
            raise ValueError("JSON 响应意外结束")
        # 丢弃已消费前缀：缓冲区大小只与"当前值 + 读块"有关，
        # 不随整份响应增长
        self._compact()
        start = self._pos
        while True:
            try:
                value, end = self._json.raw_decode(self._buf, start)
            except json.JSONDecodeError:
                # 值还没读全：继续追加数据，不丢弃已读前缀
                if not self._read_more():
                    raise ValueError("JSON 响应意外结束或格式错误")
                continue
            if (
                end == len(self._buf)
                and not self._eof
                and self._buf[start] not in '{["'
            ):
                # 值恰好结束在缓冲区末尾：只有数字/true/false/null 可能只是
                # 前缀（如 "37" 只读到 "3"）；对象/数组/字符串的结束是确定的。
                # 再读一块确认边界。
                if self._read_more():
                    continue
            self._pos = end
            return value, end - start

    def parse_object(
        self,
        on_orders: Callable[[list[dict]], int | None] | None = None,
        orders_batch: int = 0,
        orders_batch_bytes: int = 0,
        count_entries: bool = False,
    ) -> dict[str, Any]:
        """解析顶层 JSON 对象。

        ``orders`` 数组按 ``orders_batch`` 条或 ``orders_batch_bytes``
        原始字符（先到者为准，0 表示不限制）分批交给 ``on_orders``；
        返回值中 ``orders`` 字段被替换为处理条数 ``orders_handled``。
        ``entries`` 只返回条目数 ``entries_count``。
        """
        self._expect("{")
        result: dict[str, Any] = {}
        handled = 0
        entries_count = 0
        batch: list[dict] = []
        batch_bytes = 0

        def flush() -> None:
            nonlocal handled, batch, batch_bytes
            if batch and on_orders is not None:
                self._check_cancel()
                on_orders(batch)
                handled += len(batch)
            batch = []
            batch_bytes = 0

        first = True
        while True:
            ch = self._peek()
            if ch is None:
                raise ValueError("JSON 响应意外结束（顶层对象未闭合）")
            if ch == "}":
                self._pos += 1
                break
            if not first:
                self._expect(",")
            first = False

            # 键
            if self._peek() != '"':
                raise ValueError("JSON 响应格式错误：字段名必须是字符串")
            key = self._read_raw_value()

            self._expect(":")
            if self._peek() is None:
                raise ValueError("JSON 响应意外结束（缺少字段值）")

            if key == "orders" and on_orders is not None:
                if self._peek() != "[":
                    # 非数组：按普通值解析后忽略
                    self._read_raw_value()
                    continue
                self._pos += 1
                if self._peek() == "]":
                    self._pos += 1
                    continue
                while True:
                    self._check_cancel()
                    item, raw_len = self._read_raw_value_sized()
                    # 记录已解析完成：先释放其原始文本，再交给下游入库，
                    # 入库期间不额外占用正文缓冲
                    self._compact()
                    if orders_batch > 0:
                        batch.append(item)
                        batch_bytes += raw_len
                        if (
                            len(batch) >= orders_batch
                            or (
                                orders_batch_bytes > 0
                                and batch_bytes >= orders_batch_bytes
                            )
                        ):
                            flush()
                    else:
                        # 逐条处理：整页正文不驻留内存
                        batch = [item]
                        flush()
                    ch = self._peek()
                    if ch is None:
                        raise ValueError("JSON 响应意外结束（数组未闭合）")
                    self._pos += 1  # 逗号或 ']'
                    if ch == "]":
                        break
                    if ch != ",":
                        raise ValueError(f"JSON 响应格式错误：数组中出现 {ch!r}")
                flush()
            elif key == "entries" and count_entries:
                if self._peek() != "[":
                    self._read_raw_value()
                    continue
                self._pos += 1
                if self._peek() != "]":
                    while True:
                        self._read_raw_value()
                        entries_count += 1
                        ch = self._peek()
                        if ch is None:
                            raise ValueError("JSON 响应意外结束（数组未闭合）")
                        self._pos += 1
                        if ch == "]":
                            break
                        if ch != ",":
                            raise ValueError(
                                f"JSON 响应格式错误：数组中出现 {ch!r}"
                            )
                else:
                    self._pos += 1
            else:
                result[key] = self._read_raw_value()

        if on_orders is not None:
            result["orders_handled"] = handled
        if count_entries:
            result["entries_count"] = entries_count
        return result

    def finish(self) -> None:
        """确认顶层对象之后没有多余内容。"""
        if self._peek() is not None:
            raise ValueError("JSON 响应在顶层对象之后还有多余内容")


def iter_json_object(
    fp,
    on_orders: Callable[[list[dict]], int | None] | None = None,
    orders_batch: int = 0,
    orders_batch_bytes: int = 0,
    count_entries: bool = False,
    chunk_size: int = DEFAULT_CHUNK,
    cancel=None,
) -> dict[str, Any]:
    stream = _Stream(fp, chunk_size=chunk_size, cancel=cancel)
    result = stream.parse_object(
        on_orders=on_orders,
        orders_batch=orders_batch,
        orders_batch_bytes=orders_batch_bytes,
        count_entries=count_entries,
    )
    stream.finish()
    return result
