"""子进程探针：测量同步端峰值 RSS。

模拟源跑在测试进程内，本探针只承载同步端（采集 + 存储 + 发布），
因此 ``VmHWM`` 的增量就是同步端自身的内存开销。
"""
from __future__ import annotations

import asyncio
import json
import sys


def hwm_bytes() -> int:
    with open("/proc/self/status") as f:
        for line in f:
            if line.startswith("VmHWM:"):
                return int(line.split()[1]) * 1024
    return 0


def main() -> int:
    from crane_sync.engine import SyncConfig, SyncEngine

    payload = json.loads(sys.argv[1])
    mode = payload.pop("mode")
    engine = SyncEngine(SyncConfig(**payload))
    try:
        if mode == "idle":
            # 空载：只初始化存储与线程池，不发起任何请求
            print(json.dumps({"hwm": hwm_bytes()}))
        else:
            results = asyncio.run(engine.run())
            print(json.dumps({"hwm": hwm_bytes(), "results": results}))
    finally:
        engine.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
