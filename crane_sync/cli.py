"""命令行入口。

用法：
  python -m crane_sync serve   --config-out mock.json [--port 0]
  python -m crane_sync sync    --config config.json [--format jsonl|parquet]
  python -m crane_sync read    --state-dir ./state [--source offset] [--merged]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from pathlib import Path

from .engine import SyncConfig, SyncEngine
from .storage import Storage


def _setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def cmd_serve(args) -> int:
    from .mock_server import serve_forever

    server = serve_forever(
        {
            "host": args.host,
            "port": args.port,
            "gateway_quota": args.gateway_quota,
            "gateway_window": args.gateway_window,
            "window_quota": args.window_quota,
            "window_window": args.window_window,
            "token_ttl": args.token_ttl,
            "cursor_rotate_after": args.cursor_rotate_after,
        }
    )
    host, port = server.server_address
    base_url = f"http://{host}:{port}"
    print(f"模拟数据源已启动: {base_url}", flush=True)
    print("  偏移源    GET /offset/orders?limit=&offset=", flush=True)
    print("  游标源    GET /cursor/manifest, /cursor/orders?cursor=", flush=True)
    print("  时间窗源  GET /window/orders?from=&to=", flush=True)
    print("  令牌      POST /gateway/token", flush=True)
    if args.config_out:
        Path(args.config_out).write_text(
            json.dumps(
                {
                    "base_url": base_url,
                    "state_dir": args.state_dir,
                    "snapshot_format": args.format,
                    "page_size": args.page_size,
                    "gateway_quota": args.gateway_quota,
                    "gateway_window": args.gateway_window,
                    "window_source_quota": args.window_quota,
                    "window_source_window": args.window_window,
                    "cooldown_seconds": 0.5,
                    "window_minutes": 30,
                    "window_horizon": "2026-11-12T00:00:00",
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"同步配置已写入: {args.config_out}", flush=True)

    stop = {"flag": False}

    def _handle(_signum, _frame):
        stop["flag"] = True
        server.shutdown()

    signal.signal(signal.SIGINT, _handle)
    signal.signal(signal.SIGTERM, _handle)
    while not stop["flag"]:
        signal.pause()
    return 0


def _load_config(path: str, overrides: dict) -> SyncConfig:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    raw.update({k: v for k, v in overrides.items() if v is not None})
    allowed = {f for f in SyncConfig.__dataclass_fields__}  # type: ignore[attr-defined]
    raw = {k: v for k, v in raw.items() if k in allowed}
    return SyncConfig(**raw)


def cmd_sync(args) -> int:
    _setup_logging(args.verbose)
    overrides = {
        "state_dir": args.state_dir,
        "snapshot_format": args.format,
    }
    config = _load_config(args.config, overrides)
    engine = SyncEngine(config)
    try:
        try:
            results = asyncio.run(engine.run())
        except RuntimeError as exc:
            print(f"同步失败: {exc}", file=sys.stderr)
            return 1
    finally:
        engine.close()
    print("\n=== 同步完成，各来源独立发布快照 ===")
    for name, info in results.items():
        print(
            f"  {name:7s} 快照={info['snapshot_id']} "
            f"格式={info['format']} 记录数={info['record_count']} "
            f"文件={info['file_path']}"
        )
    return 0


def cmd_read(args) -> int:
    storage = Storage(args.state_dir)
    try:
        if args.merged or not args.source:
            rows = storage.read_merged()
            title = "合并快照（每单号仅最高修订，间隙统一毫米）"
        else:
            rows = storage.read_snapshot(args.source)
            title = f"来源 {args.source} 的最新已发布快照"
    except RuntimeError as exc:
        print(f"读取失败: {exc}", file=sys.stderr)
        return 2
    finally:
        storage.close()
    print(f"=== {title}，共 {len(rows)} 张巡检单 ===")
    for row in rows:
        gaps = "; ".join(
            f"{g['point']}={g['value_mm']}mm" for g in row["gaps"]
        )
        print(
            f"  {row['order_id']} rev={row['revision']} "
            f"来源={row['source']} 时间={row['inspected_at']} 间隙: {gaps}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crane_sync", description="岸桥巡检单三源同步")
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="启动自带的三个本地模拟 HTTP 数据源")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=0)
    p_serve.add_argument("--config-out", default=None, help="把连接配置写到该 JSON 文件")
    p_serve.add_argument("--state-dir", default="./state")
    p_serve.add_argument("--format", default="jsonl", choices=["jsonl", "parquet"])
    p_serve.add_argument("--page-size", type=int, default=10)
    p_serve.add_argument("--gateway-quota", type=int, default=12)
    p_serve.add_argument("--gateway-window", type=float, default=5.0)
    p_serve.add_argument("--window-quota", type=int, default=18)
    p_serve.add_argument("--window-window", type=float, default=5.0)
    p_serve.add_argument("--token-ttl", type=float, default=300.0)
    p_serve.add_argument("--cursor-rotate-after", type=int, default=None)
    p_serve.set_defaults(func=cmd_serve)

    p_sync = sub.add_parser("sync", help="执行一轮同步并发布各来源快照")
    p_sync.add_argument("--config", required=True)
    p_sync.add_argument("--state-dir", default=None)
    p_sync.add_argument("--format", default=None, choices=["jsonl", "parquet"])
    p_sync.add_argument("-v", "--verbose", action="store_true")
    p_sync.set_defaults(func=cmd_sync)

    p_read = sub.add_parser("read", help="读取已发布快照")
    p_read.add_argument("--state-dir", required=True)
    p_read.add_argument("--source", default=None, help="只读某个来源，如 offset")
    p_read.add_argument("--merged", action="store_true", help="跨来源合并，仅保留最高修订")
    p_read.set_defaults(func=cmd_read)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
