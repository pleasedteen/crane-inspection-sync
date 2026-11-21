"""流式 JSON 解析器单元测试：跨读块边界、批处理、取消与错误处理。"""
from __future__ import annotations

import io
import json
import threading
import unittest

from crane_sync.stream_json import StreamAborted, iter_json_object


def payload() -> dict:
    return {
        "orders": [
            {
                "order_id": f"QC-{i:04d}",
                "revision": i % 3,
                "inspected_at": "2026-11-10T08:00:00",
                "gaps": [{"point": "rail", "value": 8000 + i, "unit": "um"}],
                "notes": "x" * (i * 7),
            }
            for i in range(23)
        ],
        "next_offset": None,
        "total": 23,
        "nested": {"a": [1, 2, {"b": "中文"}]},
        "manifest_version": "v1",
        "entries": [{"order_id": "a", "revision": 1}] * 5,
        "next_cursor": "v1:0:5",
    }


class StreamJsonTests(unittest.TestCase):
    def test_small_chunk_boundaries(self):
        raw = json.dumps(payload(), ensure_ascii=False).encode()
        for chunk in (1, 2, 3, 5, 17, 64, 4096):
            with self.subTest(chunk=chunk):
                got: list[dict] = []
                out = iter_json_object(
                    io.BytesIO(raw),
                    on_orders=lambda b: got.extend(b),
                    orders_batch=0,
                    count_entries=True,
                    chunk_size=chunk,
                )
                self.assertEqual(got, payload()["orders"])
                self.assertEqual(out["orders_handled"], 23)
                self.assertEqual(out["entries_count"], 5)
                self.assertIsNone(out["next_offset"])
                self.assertEqual(out["total"], 23)
                self.assertEqual(out["nested"], payload()["nested"])
                self.assertNotIn("orders", out)
                self.assertNotIn("entries", out)

    def test_batch_by_count_and_bytes(self):
        raw = json.dumps(payload()).encode()
        batches: list[int] = []
        iter_json_object(
            io.BytesIO(raw),
            on_orders=lambda b: batches.append(len(b)),
            orders_batch=4,
            chunk_size=7,
        )
        self.assertEqual(sum(batches), 23)
        self.assertTrue(all(n <= 4 for n in batches))

        batches = []
        iter_json_object(
            io.BytesIO(raw),
            on_orders=lambda b: batches.append(len(b)),
            orders_batch=1000,
            orders_batch_bytes=1024,
            chunk_size=7,
        )
        self.assertEqual(sum(batches), 23)
        self.assertGreater(len(batches), 1)

    def test_cancel_stops_reading(self):
        raw = json.dumps(payload()).encode()
        cancel = threading.Event()
        seen: list[int] = []

        def on_orders(batch):
            seen.append(len(batch))
            cancel.set()

        with self.assertRaises(StreamAborted):
            iter_json_object(
                io.BytesIO(raw),
                on_orders=on_orders,
                orders_batch=1,
                cancel=cancel,
            )
        self.assertEqual(len(seen), 1)

    def test_malformed_input_rejected(self):
        for bad in (
            b"{",
            b'{"orders": [1,2',
            b'{"a" 1}',
            b"[]",
            b'{"a":1} x',
            b'{"orders": [1 2]}',
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    iter_json_object(io.BytesIO(bad), on_orders=lambda b: 0)

    def test_no_callback_ignores_orders_like_json_loads(self):
        raw = json.dumps(payload()).encode()
        out = iter_json_object(io.BytesIO(raw), chunk_size=3)
        self.assertEqual(out["orders"], payload()["orders"])
        self.assertEqual(out["entries"], payload()["entries"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
