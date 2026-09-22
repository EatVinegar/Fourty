r"""Probe whether the Eastmoney index interface is available.

This script calls ``stock_zh_index_spot_em(symbol="沪深重要指数")`` exactly
once. AKShare normally retries failed requests up to three times, so this
script temporarily replaces its retry helper with a single-attempt request.

Run only with the project environment:

    D:\code\Fourty\.venv\python.exe scripts\check_eastmoney.py
"""

from __future__ import annotations

import sys
import time
from datetime import datetime
from typing import Any


PROBE_SYMBOL = "沪深重要指数"
PROBE_URL = "https://33.push2.eastmoney.com/api/qt/clist/get"


def _configure_console() -> None:
    """Use UTF-8 output on Windows terminals."""
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, "reconfigure"):
            try:
                stream.reconfigure(encoding="utf-8", errors="replace")
            except (AttributeError, ValueError):
                pass


def _single_attempt_request(
    url: str,
    params: dict[str, Any] | None = None,
    timeout: int = 15,
    **_: Any,
) -> Any:
    """Perform one HTTP GET with no retry."""
    import requests

    with requests.Session() as session:
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        return response


def _classify_failure(exc: BaseException) -> str:
    """Translate the exception into a concrete likely cause."""
    try:
        import requests
    except ImportError:
        requests = None

    message = str(exc)
    lowered = message.lower()

    if "remote end closed" in lowered or "remotedisconnected" in lowered:
        return (
            "TCP 连接建立后被远端主动断开；常见原因是东方财富风控或 IP "
            "限制、网络边界拦截，或本地代理/隧道中断"
        )
    if requests is not None:
        if isinstance(exc, requests.exceptions.ProxyError):
            return "代理连接失败，请求没有到达东方财富服务器"
        if isinstance(
            exc,
            (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout),
        ):
            return "连接或读取超时"
        if isinstance(exc, requests.exceptions.HTTPError):
            response = getattr(exc, "response", None)
            status = response.status_code if response is not None else None
            if status in (403, 429):
                return f"东方财富返回 HTTP {status}，可能被限流或封禁"
            return f"东方财富返回 HTTP 错误: {status}"
        if isinstance(exc, requests.exceptions.ConnectionError):
            return "网络连接失败，可能由代理、DNS、连接重置或目标端口不可达导致"
        if isinstance(exc, requests.exceptions.JSONDecodeError):
            return "响应不是有效 JSON，接口可能返回了验证码或错误页面"

    if "10061" in lowered or "connection refused" in lowered:
        return "连接被拒绝，通常是本地代理未运行或目标端口不可达"
    if "nameresolution" in lowered or "name resolution" in lowered:
        return "DNS 解析失败"
    if "json" in lowered or "decode" in lowered:
        return "响应解析失败，接口返回内容可能不是预期 JSON"
    if "keyerror" in lowered or "none" in lowered or "diff" in lowered:
        return "接口响应结构异常，可能被风控或字段发生变化"
    return "未分类异常"


def main() -> int:
    _configure_console()
    started = datetime.now()
    start_clock = time.perf_counter()

    try:
        import akshare as ak
        import akshare.utils.func as ak_func
    except ImportError as exc:
        print(f"依赖导入失败: {exc}")
        return 1

    original_retry_helper = ak_func.request_with_retry
    ak_func.request_with_retry = _single_attempt_request

    print("东方财富指数接口可用性探查")
    print(f"调用接口: stock_zh_index_spot_em(symbol='{PROBE_SYMBOL}')")
    print(f"底层地址: {PROBE_URL}")
    print("频控策略: 单次运行仅调用一次，AKShare 内置重试已临时禁用")
    print(f"开始时间: {started:%Y-%m-%d %H:%M:%S}")
    print()

    try:
        result = ak.stock_zh_index_spot_em(symbol=PROBE_SYMBOL)
    except Exception as exc:
        elapsed = time.perf_counter() - start_clock
        print("结果: 不可用")
        print(f"异常类型: {type(exc).__module__}.{type(exc).__name__}")
        print(f"异常内容: {exc}")
        print(f"可能原因: {_classify_failure(exc)}")
        print(f"请求耗时: {elapsed:.2f} 秒")
        return 1
    finally:
        ak_func.request_with_retry = original_retry_helper

    elapsed = time.perf_counter() - start_clock
    print("结果: 可用")
    print(f"返回行数: {len(result)}")
    print(f"返回列: {list(result.columns)}")

    if result.empty:
        print("可能问题: 请求成功但返回空数据")
        return 1

    name_column = "名称" if "名称" in result.columns else None
    code_column = "代码" if "代码" in result.columns else None
    if name_column or code_column:
        columns = [column for column in (code_column, name_column) if column]
        print("样本:")
        print(result[columns].head(10).to_string(index=False))

    print(f"请求耗时: {elapsed:.2f} 秒")
    return 0


if __name__ == "__main__":
    sys.exit(main())
