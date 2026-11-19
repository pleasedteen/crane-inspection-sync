#!/usr/bin/env python3
"""一键自动化测试入口：自带模拟源，80 秒内输出可读结果。

用法：python3 run_tests.py
"""
import sys
import time
import unittest

if __name__ == "__main__":
    start = time.monotonic()
    print("=" * 68)
    print("岸桥巡检单三源同步 —— 自动化测试（自带本地模拟 HTTP 数据源）")
    print("=" * 68)
    loader = unittest.TestLoader()
    suite = loader.discover(start_dir="tests", pattern="test_*.py")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    elapsed = time.monotonic() - start
    print("-" * 68)
    print(
        f"测试结束：运行 {result.testsRun} 项，"
        f"失败 {len(result.failures)}，错误 {len(result.errors)}，"
        f"用时 {elapsed:.1f} 秒（预算 80 秒）"
    )
    sys.exit(0 if result.wasSuccessful() else 1)
