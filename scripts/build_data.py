#!/usr/bin/env python3
"""
股票技术指标监控主脚本
使用akshare开源数据源，支持配置化、重试机制、推送节流
"""
import os
import sys
import json
import yaml
import requests
from datetime import datetime, timedelta, date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from technical_analysis import (
    get_stock_data,
    calculate_ma, calculate_macd, calculate_rsi,
    calculate_bollinger, calculate_kdj,
    check_signals
)

# ==================== 工具函数 ====================

def load_config(path="configs/config.yaml"):
    config_path = Path(__file__).parent.parent / path
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_trading_day(check_date=None):
    """
    【优化1】原版只有 2025~2026 部分节假日，且写死在代码里。
    改为：优先用 akshare 的交易日历接口，失败时降级到本地规则。
    这样以后不用每年手动更新节假日。
    """
    target = check_date or date.today()

    # 周末直接排除
    if target.weekday() >= 5:
        return False

    # 优先尝试 akshare 交易日历（最准确）
    try:
        import akshare as ak
        import pandas as pd
        year = str(target.year)
        trade_cal = ak.tool_trade_date_hist_sina()
        # 返回的是 DataFrame，列名为 trade_date
        trade_dates = pd.to_datetime(trade_cal["trade_date"]).dt.date.tolist()
        return target in trade_dates
    except Exception:
        pass

    # 降级：本地节假日规则
    holidays = {
        # 2025
        date(2025, 1, 1),
        date(2025, 1, 28), date(2025, 1, 29), date(2025, 1, 30),
        date(2025, 1, 31), date(2025, 2, 3), date(2025, 2, 4),
        date(2025, 4, 4),
        date(2025, 5, 1), date(2025, 5, 2),
        date(2025, 5, 31), date(2025, 6, 2),
        date(2025, 10, 1), date(2025, 10, 2), date(2025, 10, 3),
        date(2025, 10, 6), date(2025, 10, 7), date(2025, 10, 8),
        # 2026
        date(2026, 1, 1),
        date(2026, 2, 17), date(2026, 2, 18), date(2026, 2, 19),
        date(2026, 2, 20), date(2026, 2, 23), date(2026, 2, 24),
        date(2026, 4, 6),
        date(2026, 5, 1),
        date(2026, 6, 19),
        date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
        date(2026, 10, 6), date(2026, 10, 7), date(2026, 10, 8),
    }
    return target not in holidays


def get_last_real_trading_date():
    """
    【优化2】新增：自动往前找最近一个真实交易日（最多回溯7天）。
    原版没有这个函数，非交易日只能靠已有文件，首次运行永远是空。
    """
    for i in range(1, 8):
        candidate = date.today() - timedelta(days=i)
        if is_trading_day(candidate):
            return candidate
    return date.today() - timedelta(days=1)


def now_cn():
    return datetime.utcnow() + timedelta(hours=8)


def should_push(symbol, throttle_minutes):
    state_path = Path(__file__).parent.parent / "data" / "state.json"
    state = {}
    if state_path.exists():
        with open(state_path, "r", encoding="utf-8") as f:
            try:
                state = json.load(f)
            except json.JSONDecodeError:
                # 【优化3】原版没有异常处理，state.json 损坏会直接崩溃
                state = {}
    last = state.get(symbol)
    now = now_cn().timestamp()
    if last and now - last < throttle_minutes * 60:
        return False
    state[symbol] = now
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    return True


def push_feishu(text, webhook=None):
    webhook = webhook or os.getenv("FEISHU_WEBHOOK")
    if not webhook:
        return {"status": "skip", "reason": "no webhook configured"}
    payload = {"msg_type": "text", "content": {"text": text}}
    try:
        r = requests.post(webhook, json=payload, timeout=10)
        return {"status": r.status_code, "ok": r.status_code == 200}
    except Exception as e:
        return {"status": "error", "msg": str(e)}


def write_signals_json(data_dir, alerts, is_last_trading=False, note=None):
    """
    【优化4】原版有 ensure_signals_json（只写空占位）和散落的 json.dump，
    统一成一个函数，所有写入 signals.json 的地方都走这里，避免字段遗漏。
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "signals":          alerts or [],
        "update_time":      now_cn().isoformat(),
        "is_last_trading":  is_last_trading,
        "note":             note or "",
        # 【优化5】新增 price_history，供前端画迷你折线图用（后面处理股票时填充）
        # 这里先占位，实际数据在 process_stock() 里写入每个 alert
    }
    with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_run_summary(ok, fail, alerts_count=0, note=None):
    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    summary = {
        "time":          now_cn().isoformat(),
        "ok":            ok,
        "fail":          fail,
        "alerts_count":  alerts_count,
        # 【优化6】新增成功率字段，前端统计卡片可以直接用
        "success_rate":  f"{len(ok)}/{len(ok)+len(fail)}" if (ok or fail) else "0/0",
        "note":          note,
    }
    with open(data_dir / "latest_run.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)


def get_last_trading_data(data_dir):
    signals_path = data_dir / "signals.json"
    if not signals_path.exists():
        return None
    try:
        with open(signals_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # 只保留有真实数据的（排除纯占位）
        if data.get("note") != "non-trading-day" and data.get("signals") is not None:
            return data
    except Exception:
        pass
    return None


def process_stock(stock, history_days, signals_cfg):
    """
    【优化7】原版主循环里每只股票的处理逻辑全部堆在 main() 里，
    抽成独立函数后：单只股票异常不影响其他，也方便后续并发改造。
    返回 (result_dict | None, error_str | None)
    """
    code = stock.get("code")
    name = stock.get("name", code)

    try:
        df = get_stock_data(code, days=history_days)
        df = calculate_ma(df)
        df = calculate_macd(df)
        df = calculate_rsi(df)
        df = calculate_bollinger(df)
        df = calculate_kdj(df)

        result = check_signals(df, code, name, config=signals_cfg)

        if result:
            # 【优化5 续】把近20日收盘价写入，供前端迷你图使用
            result["price_history"] = (
                df["close"].tail(20).round(2).tolist()
            )
            # 【优化8】补充成交量数据，前端可展示量能柱
            result["volume_history"] = (
                df["volume"].tail(20).astype(int).tolist()
            )

        return result, None

    except Exception as e:
        return None, str(e)


# ==================== 主流程 ====================

def main():
    print("=" * 60)
    print("🚀 股票技术指标监控系统")
    print("=" * 60)

    # 加载配置
    try:
        cfg = load_config()
        print("✅ 配置加载成功")
    except Exception as e:
        print(f"❌ 配置加载失败: {e}")
        return

    data_dir = Path(__file__).parent.parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    # -------- 非交易日处理 --------
    if cfg.get("runtime", {}).get("use_trading_calendar", True):
        if not is_trading_day():
            print("📅 非交易日，尝试保留上一交易日数据")

            last_data = get_last_trading_data(data_dir)

            if last_data:
                # 有历史数据：标记后原样保留
                last_data["note"]            = "non-trading-day"
                last_data["is_last_trading"] = True
                with open(data_dir / "signals.json", "w", encoding="utf-8") as f:
                    json.dump(last_data, f, ensure_ascii=False, indent=2)
                print(f"  ✅ 已保留上一交易日数据，时间: {last_data.get('update_time')}")

            else:
                # 【优化2 续】没有历史文件时，主动拉取最近交易日数据
                print("  ⚠️ 无历史数据，尝试主动拉取最近交易日数据...")
                last_trade_date = get_last_real_trading_date()
                print(f"  📅 最近交易日: {last_trade_date}")

                # 临时把 use_trading_calendar 关掉，复用下面的正常流程
                cfg["runtime"]["use_trading_calendar"] = False
                cfg["_non_trading_fallback"] = True  # 标记来源

            write_run_summary(ok=[], fail=[], note="non-trading-day")

            # 如果有历史数据直接返回，否则继续往下跑一次真实拉取
            if last_data:
                return
        else:
            print("📅 交易日，正常执行")

    # -------- 正常交易日流程 --------
    watchlist    = cfg.get("watchlist", [])
    runtime_cfg  = cfg.get("runtime", {})
    push_cfg     = cfg.get("push", {})
    signals_cfg  = cfg.get("signals", {})

    throttle_minutes = push_cfg.get("throttle_minutes", 30)
    strong_only      = push_cfg.get("strong_signal_only", True)
    history_days     = runtime_cfg.get("history_days", 60)

    is_fallback = cfg.get("_non_trading_fallback", False)

    print(f"📊 监控股票数: {len(watchlist)}")
    print(f"⏱️  推送节流: {throttle_minutes} 分钟")
    print(f"📈 历史数据: {history_days} 天")
    print("-" * 60)

    alerts   = []
    ok_list  = []
    fail_list = []

    for stock in watchlist:
        code = stock.get("code")
        name = stock.get("name", code)
        if not code:
            continue

        print(f"\n📈 处理 {name}({code})...")

        result, error = process_stock(stock, history_days, signals_cfg)

        if error:
            print(f"  ❌ 失败: {error}")
            fail_list.append({"code": code, "name": name, "error": error})
            continue

        ok_list.append({"code": code, "name": name})

        if result:
            alerts.append(result)
            sig_count = len(result["signals"])
            print(f"  ✅ 发现 {sig_count} 个信号，趋势: {result['trend']}")

            # 飞书推送
            should_send = (
                result["trend"] == "强势"
                or any(s["strength"] == "强" for s in result["signals"])
            ) if strong_only else True

            if should_send and should_push(code, throttle_minutes):
                sig_texts = [
                    f"[{s['indicator']}] {s['type']}: {s['desc']}"
                    for s in result["signals"]
                ]
                msg = (
                    f"🚨 股票信号提醒\n\n"
                    f"📈 {name} ({code})\n"
                    f"💰 现价: ¥{result['price']}\n"
                    f"📊 趋势: {result['trend']}\n"
                    f"🔔 信号:\n"
                    + "".join(f"  • {t}\n" for t in sig_texts)
                    + f"\n⏰ {now_cn().strftime('%Y-%m-%d %H:%M')}\n"
                    f"📊 RSI: {result.get('rsi', 0):.1f} | "
                    f"MACD: {result.get('macd', 0):.4f}"
                )
                push_result = push_feishu(msg)
                status = "✅ 推送成功" if push_result.get("ok") else f"⚠️ 推送失败: {push_result}"
                print(f"  📱 飞书: {status}")
        else:
            print(f"  ℹ️  无信号")

    # -------- 写入结果 --------
    note = "non-trading-fallback" if is_fallback else None

    write_signals_json(
        data_dir,
        alerts,
        is_last_trading=is_fallback,
        note=note
    )

    write_run_summary(
        ok=ok_list,
        fail=fail_list,
        alerts_count=len(alerts),
        note=note
    )

    # -------- 汇总输出 --------
    print("\n" + "=" * 60)
    print(f"✅ 完成！成功: {len(ok_list)} | 失败: {len(fail_list)} | 信号: {len(alerts)}")
    if fail_list:
        print("❌ 失败列表:")
        for f in fail_list:
            print(f"   {f['name']}({f['code']}): {f['error']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
