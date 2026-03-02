"""
技术指标计算模块
支持：均线、MACD、RSI、布林带、KDJ，带重试机制
数据源：iFinD HTTP API（纯 requests，无需安装 SDK）
"""

import os
import time
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta


# ==================== iFinD HTTP 客户端（内聚在本文件）====================

_BASE_URL    = "https://quantapi.51ifind.com/api/v1"
_TOKEN_CACHE = {"access_token": None, "expires_at": 0}


def _get_access_token(force_refresh=False):
    """
    用 refresh_token 换取 access_token（进程内缓存 6 天）。
    GitHub Actions 每次运行都是新进程，会自动重新获取，无需担心缓存问题。
    """
    now = time.time()
    if (
        not force_refresh
        and _TOKEN_CACHE["access_token"]
        and now < _TOKEN_CACHE["expires_at"]
    ):
        return _TOKEN_CACHE["access_token"]

    refresh_token = os.environ.get("IFIND_REFRESH_TOKEN", "").strip()
    if not refresh_token:
        raise RuntimeError(
            "❌ 环境变量 IFIND_REFRESH_TOKEN 未配置，"
            "请在 GitHub Secrets 中添加该变量"
        )

    url     = f"{_BASE_URL}/get_access_token"
    headers = {
        "Content-Type":  "application/json",
        "refresh_token": refresh_token,
    }
    try:
        resp = requests.post(url, headers=headers, timeout=15)
        resp.raise_for_status()
        body = resp.json()
    except Exception as e:
        raise RuntimeError(f"❌ 获取 access_token 网络异常: {e}")

    token = body.get("data", {}).get("access_token")
    if not token:
        raise RuntimeError(f"❌ 获取 access_token 失败，接口返回: {body}")

    _TOKEN_CACHE["access_token"] = token
    _TOKEN_CACHE["expires_at"]   = now + 6 * 24 * 3600
    return token


def _ifind_post(endpoint, payload, access_token):
    """通用 iFinD HTTP POST 请求"""
    url     = f"{_BASE_URL}/{endpoint}"
    headers = {
        "Content-Type":    "application/json",
        "access_token":    access_token,
        "Accept-Encoding": "gzip,deflate",
    }
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _fmt_code(code: str) -> str:
    """'600519' → '600519.SH'，'000858' → '000858.SZ'"""
    code = code.strip()
    if "." in code:
        return code
    if code.startswith("6") or code.startswith("5"):
        return f"{code}.SH"
    return f"{code}.SZ"


def _parse_history_response(result: dict, fmt_code: str) -> pd.DataFrame:
    """
    解析 cmd_history_quotation 返回的 JSON。
    兼容 tables / data 两种顶层结构。
    """
    errorcode = result.get("errorcode", -1)
    if errorcode != 0:
        raise ValueError(
            f"iFinD 接口错误 (errorcode={errorcode}): "
            f"{result.get('errmsg', '未知错误')}"
        )

    tables = result.get("tables") or result.get("data", {})
    table  = tables.get("table", {})
    stock  = table.get(fmt_code, {})

    if not stock:
        raise ValueError(
            f"返回数据中找不到 {fmt_code}，"
            f"实际 keys: {list(table.keys())}"
        )

    time_list  = stock.get("time",   [])
    open_list  = stock.get("open",   [])
    high_list  = stock.get("high",   [])
    low_list   = stock.get("low",    [])
    close_list = stock.get("close",  [])
    vol_list   = stock.get("volume", [])

    if not time_list:
        raise ValueError(f"{fmt_code} 返回时间序列为空")

    return pd.DataFrame({
        "date":   time_list,
        "open":   open_list,
        "high":   high_list,
        "low":    low_list,
        "close":  close_list,
        "volume": vol_list,
    })


# ==================== 数据获取 ====================

def get_stock_data(code, period="daily", count=120):
    """
    从 iFinD HTTP API 获取 A 股历史 K 线数据（前复权）。

    Args:
        code:   str，股票代码，如 '600519' 或 '600519.SH'
        period: str，'daily' / 'weekly' / 'monthly'
        count:  int，需要的 K 线条数

    Returns:
        DataFrame，index 为 pd.DatetimeIndex，
        columns: open, high, low, close, volume（均为 float）
    """
    period_map = {"daily": "D", "weekly": "W", "monthly": "M"}
    interval   = period_map.get(period, "D")

    fetch_count = count + 30
    end_date    = datetime.now()
    day_buffer  = {"D": 2.0, "W": 10.0, "M": 40.0}.get(interval, 2.0)
    start_date  = end_date - timedelta(days=int(fetch_count * day_buffer) + 60)

    start_str = start_date.strftime("%Y-%m-%d")
    end_str   = end_date.strftime("%Y-%m-%d")
    fmt_code  = _fmt_code(code)

    payload = {
        "codes":      fmt_code,
        "indicators": "open,high,low,close,volume",
        "startdate":  start_str,
        "enddate":    end_str,
        "functionpara": {
            "Interval": interval,    # D / W / M
            "CPS":      "2",         # 前复权（分红再投），最适合技术分析
            "Fill":     "Previous",  # 非交易日沿用前值
            "Currency": "RMB",
        },
    }

    MAX_RETRIES  = 3
    RETRY_DELAYS = [15, 30, 60]
    last_error   = None
    result       = None

    for attempt in range(MAX_RETRIES):
        try:
            # 首次失败后强制刷新 token，防止 token 恰好在运行中过期
            access_token = _get_access_token(force_refresh=(attempt > 0))
            result = _ifind_post("cmd_history_quotation", payload, access_token)
            break
        except Exception as e:
            last_error = e
            if attempt < MAX_RETRIES - 1:
                wait = RETRY_DELAYS[attempt]
                print(f"  ⚠️  [{code}] 第 {attempt+1} 次请求失败，{wait}s 后重试… ({e})")
                time.sleep(wait)
            else:
                raise RuntimeError(
                    f"❌ 获取 [{code}] 数据失败，已重试 {MAX_RETRIES} 次。"
                    f"最后错误: {last_error}"
                ) from last_error

    # 解析返回数据
    df = _parse_history_response(result, fmt_code)

    df["date"] = pd.to_datetime(df["date"])
    df = (
        df.sort_values("date")
          .drop_duplicates("date")
          .reset_index(drop=True)
          .set_index("date")
    )

    cols = [c for c in ["open", "high", "low", "close", "volume"] if c in df.columns]
    df   = df[cols].apply(pd.to_numeric, errors="coerce").dropna(how="all")

    if df.empty:
        raise ValueError(f"❌ [{code}] 清洗后数据为空，请检查代码或日期范围")
    if len(df) < 20:
        raise ValueError(
            f"❌ [{code}] 数据严重不足（仅 {len(df)} 条），无法计算技术指标"
        )
    if len(df) < 60:
        print(f"  ⚠️  [{code}] 数据较少（{len(df)} 条），MA60 等长周期指标精度下降")

    return df.tail(count)


# ==================== 技术指标计算 ====================

def calculate_ma(data, windows=None, periods=None):
    """
    计算移动平均线

    参数：
        windows: 主参数，list，如 [5, 10, 20, 60]
        periods: 兼容别名，与 windows 等价
    """
    effective = windows if windows is not None else periods
    if effective is None:
        effective = [5, 10, 20, 60]

    for w in effective:
        data[f'MA{w}'] = data['close'].rolling(window=w, min_periods=1).mean()
    return data


def calculate_macd(data, fast=12, slow=26, signal=9):
    """计算 MACD 指标（DIF / DEA / MACD 柱）"""
    ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow, adjust=False).mean()

    data['DIF']  = ema_fast - ema_slow
    data['DEA']  = data['DIF'].ewm(span=signal, adjust=False).mean()
    data['MACD'] = 2 * (data['DIF'] - data['DEA'])
    return data


def calculate_rsi(data, period=14):
    """
    计算 RSI 相对强弱指数
    使用标准 Wilder EWM（com=period-1）
    """
    delta    = data['close'].diff()
    gain     = delta.where(delta > 0, 0.0)
    loss     = -delta.where(delta < 0, 0.0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period, adjust=False).mean()

    rs          = avg_gain / avg_loss.replace(0, np.nan)
    data['RSI'] = 100 - (100 / (1 + rs))
    return data


def calculate_bollinger(data, window=None, num_std=None, period=None, std_dev=None):
    """
    计算布林带（含宽度和 %B）

    参数：
        window / period:   均线窗口期，默认 20
        num_std / std_dev: 标准差倍数，默认 2.0
    """
    effective_window = window  or period  or 20
    effective_std    = num_std or std_dev or 2.0

    mid = data['close'].rolling(window=effective_window, min_periods=1).mean()
    std = data['close'].rolling(window=effective_window, min_periods=1).std()

    data['BOLL_MID']   = mid
    data['BOLL_STD']   = std
    data['BOLL_UPPER'] = mid + std * effective_std
    data['BOLL_LOWER'] = mid - std * effective_std

    band_width = (data['BOLL_UPPER'] - data['BOLL_LOWER']).replace(0, np.nan)
    data['BOLL_WIDTH'] = band_width / data['BOLL_MID']
    data['BOLL_PCT_B'] = (data['close'] - data['BOLL_LOWER']) / band_width
    return data


def calculate_kdj(data, fastk_period=None, signal_period=None, n=None, m1=None, m2=None):
    """
    计算 KDJ 指标

    参数：
        fastk_period / n:    RSV 窗口期，默认 9
        signal_period / m1:  K 线平滑周期，默认 3
        m2:                  D 线平滑周期，默认与 m1 相同
    """
    effective_n  = fastk_period  or n  or 9
    effective_m1 = signal_period or m1 or 3
    effective_m2 = m2 or effective_m1

    low_list  = data['low'].rolling(window=effective_n, min_periods=1).min()
    high_list = data['high'].rolling(window=effective_n, min_periods=1).max()

    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    rsv = rsv.replace([np.inf, -np.inf], 50).fillna(50)

    data['K'] = rsv.ewm(com=effective_m1 - 1, adjust=False).mean()
    data['D'] = data['K'].ewm(com=effective_m2 - 1, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    return data


# ==================== 辅助函数 ====================

def volume_confirm(df, n=20, ratio=1.5):
    """检查最新一根 K 线是否放量"""
    if 'volume' not in df.columns or len(df) < 2:
        return False

    if len(df) < n + 1:
        print(f"  ⚠️  volume_confirm: 数据仅 {len(df)} 条，不足 {n+1} 条，均量精度下降")
        avg_vol = df['volume'].iloc[:-1].mean()
    else:
        avg_vol = df['volume'].iloc[-n - 1:-1].mean()

    if avg_vol <= 0 or pd.isna(avg_vol):
        return False

    return bool(df['volume'].iloc[-1] > avg_vol * ratio)


def _safe_round(val, digits=2):
    """安全取整，NaN / None 返回 None"""
    try:
        if val is None or pd.isna(val):
            return None
        return round(float(val), digits)
    except Exception:
        return None


def _safe_int_volume(val):
    """安全地将成交量转为 int，NaN 时返回 0"""
    try:
        if val is None or pd.isna(val):
            return 0
        return int(float(val))
    except Exception:
        return 0


# ==================== 信号检测 ====================

def check_signals(data, cfg_or_code=None, name=None, config=None):
    """
    检查交易信号

    调用方式（两种均支持）：
        check_signals(df, cfg_dict)           ← build_data.py 的调用方式
        check_signals(df, "600519", "贵州茅台") ← 旧版调用方式

    Returns:
        dict（有信号时）或 None（无信号时）
    """
    if isinstance(cfg_or_code, dict):
        cfg  = cfg_or_code
        code = cfg.get("symbol", "")
        name = cfg.get("name",   "")
    else:
        code = cfg_or_code or ""
        cfg  = config or {}

    def _get(flat_key, nested_section, nested_key, default):
        if flat_key in cfg:
            return cfg[flat_key]
        return cfg.get(nested_section, {}).get(nested_key, default)

    rsi_ob    = _get("rsi_overbought", "rsi",    "overbought", 70)
    rsi_os    = _get("rsi_oversold",   "rsi",    "oversold",   30)
    vol_ratio = _get("volume_ratio",   "volume", "ratio",      1.5)
    kdj_ob    = _get("kdj_overbought", "kdj",    "overbought", 80)
    kdj_os    = _get("kdj_oversold",   "kdj",    "oversold",   20)

    if len(data) < 2:
        return None

    latest = data.iloc[-1]
    prev   = data.iloc[-2]

    signals = []
    score   = 0

    # ── 1. 均线突破 ──────────────────────────────────────────
    if 'MA5' in data.columns:
        if prev['close'] < prev['MA5'] and latest['close'] > latest['MA5']:
            vol_ok = volume_confirm(data, ratio=vol_ratio)
            signals.append({
                'type':      '突破',
                'indicator': 'MA5',
                'desc':      '股价放量突破5日均线' if vol_ok else '股价突破5日均线',
                'strength':  '强' if vol_ok else '中等',
                'action':    '关注'
            })
            score += 2 if vol_ok else 1

    ma_cols = ['MA5', 'MA10', 'MA20']
    if all(c in data.columns for c in ma_cols):
        if latest['MA5'] > latest['MA10'] > latest['MA20']:
            signals.append({
                'type':      '多头排列',
                'indicator': 'MA',
                'desc':      'MA5>MA10>MA20，均线多头排列',
                'strength':  '中等',
                'action':    '趋势向上'
            })
            score += 1
        elif latest['MA5'] < latest['MA10'] < latest['MA20']:
            signals.append({
                'type':      '空头排列',
                'indicator': 'MA',
                'desc':      'MA5<MA10<MA20，均线空头排列',
                'strength':  '中等',
                'action':    '趋势向下'
            })
            score -= 1

    # ── 2. MACD 金叉 / 死叉 ──────────────────────────────────
    if all(c in data.columns for c in ['DIF', 'DEA', 'MACD']):
        if not pd.isna(prev['DIF']) and not pd.isna(prev['DEA']):
            macd_cross_happened = False

            if prev['DIF'] < prev['DEA'] and latest['DIF'] > latest['DEA']:
                above_zero = latest['DIF'] > 0
                signals.append({
                    'type':      '金叉',
                    'indicator': 'MACD',
                    'desc':      f'MACD金叉（{"零轴上方" if above_zero else "零轴下方"}），动能转强',
                    'strength':  '强' if above_zero else '中等',
                    'action':    '买入信号'
                })
                score += 2 if above_zero else 1
                macd_cross_happened = True

            elif prev['DIF'] > prev['DEA'] and latest['DIF'] < latest['DEA']:
                below_zero = latest['DIF'] < 0
                signals.append({
                    'type':      '死叉',
                    'indicator': 'MACD',
                    'desc':      f'MACD死叉（{"零轴下方" if below_zero else "零轴上方"}），动能转弱',
                    'strength':  '强' if below_zero else '中等',
                    'action':    '卖出信号'
                })
                score -= 2 if below_zero else 1
                macd_cross_happened = True

            if not macd_cross_happened:
                if not pd.isna(prev['MACD']) and not pd.isna(latest['MACD']):
                    if prev['MACD'] < 0 and latest['MACD'] >= 0:
                        signals.append({
                            'type':      'MACD柱翻红',
                            'indicator': 'MACD',
                            'desc':      'MACD柱由负转正，动能增强',
                            'strength':  '中等',
                            'action':    '关注'
                        })
                        score += 1
                    elif prev['MACD'] > 0 and latest['MACD'] <= 0:
                        signals.append({
                            'type':      'MACD柱翻绿',
                            'indicator': 'MACD',
                            'desc':      'MACD柱由正转负，动能减弱',
                            'strength':  '中等',
                            'action':    '观望'
                        })
                        score -= 1

    # ── 3. RSI 超买 / 超卖 ───────────────────────────────────
    if 'RSI' in data.columns and not pd.isna(latest['RSI']):
        if latest['RSI'] < rsi_os:
            signals.append({
                'type':      '超卖',
                'indicator': 'RSI',
                'desc':      f'RSI={latest["RSI"]:.1f}，进入超卖区间',
                'strength':  '中等',
                'action':    '关注反弹'
            })
            score += 1
        elif latest['RSI'] > rsi_ob:
            signals.append({
                'type':      '超买',
                'indicator': 'RSI',
                'desc':      f'RSI={latest["RSI"]:.1f}，进入超买区间',
                'strength':  '中等',
                'action':    '注意回调'
            })
            score -= 1

    # ── 4. 布林带突破 ─────────────────────────────────────────
    if all(c in data.columns for c in ['BOLL_UPPER', 'BOLL_LOWER', 'BOLL_PCT_B']):
        if not pd.isna(latest['BOLL_UPPER']):
            vol_ok = volume_confirm(data, ratio=vol_ratio)
            if latest['close'] > latest['BOLL_UPPER']:
                signals.append({
                    'type':      '放量突破上轨' if vol_ok else '突破上轨',
                    'indicator': 'BOLL',
                    'desc':      '股价放量突破布林带上轨，强势' if vol_ok else '股价突破布林带上轨',
                    'strength':  '强' if vol_ok else '中等',
                    'action':    '持有'
                })
                score += 2 if vol_ok else 1
            elif latest['close'] < latest['BOLL_LOWER']:
                signals.append({
                    'type':      '跌破下轨',
                    'indicator': 'BOLL',
                    'desc':      '股价跌破布林带下轨，超跌',
                    'strength':  '中等',
                    'action':    '关注反弹'
                })
                score -= 1

            if not pd.isna(prev['BOLL_PCT_B']) and not pd.isna(latest['BOLL_PCT_B']):
                if prev['BOLL_PCT_B'] < 0.05 and latest['BOLL_PCT_B'] >= 0.05:
                    signals.append({
                        'type':      '%B回归',
                        'indicator': 'BOLL',
                        'desc':      '布林 %B 回归，超卖后反弹',
                        'strength':  '中等',
                        'action':    '关注'
                    })
                    score += 1

    # ── 5. KDJ 金叉 / 死叉 ───────────────────────────────────
    if all(c in data.columns for c in ['K', 'D', 'J']):
        if not pd.isna(prev['K']) and not pd.isna(prev['D']):
            k_cross_up   = prev['K'] < prev['D'] and latest['K'] > latest['D']
            k_cross_down = prev['K'] > prev['D'] and latest['K'] < latest['D']

            if k_cross_up and latest['K'] < kdj_os:
                signals.append({
                    'type':      '金叉',
                    'indicator': 'KDJ',
                    'desc':      f'KDJ低位金叉(K={latest["K"]:.1f})，短线反弹信号',
                    'strength':  '强',
                    'action':    '短线关注'
                })
                score += 2
            elif k_cross_up:
                signals.append({
                    'type':      '金叉',
                    'indicator': 'KDJ',
                    'desc':      f'KDJ金叉(K={latest["K"]:.1f})',
                    'strength':  '中等',
                    'action':    '关注'
                })
                score += 1

            if k_cross_down and latest['K'] > kdj_ob:
                signals.append({
                    'type':      '死叉',
                    'indicator': 'KDJ',
                    'desc':      f'KDJ高位死叉(K={latest["K"]:.1f})，短线回调风险',
                    'strength':  '强',
                    'action':    '注意风险'
                })
                score -= 2
            elif k_cross_down:
                signals.append({
                    'type':      '死叉',
                    'indicator': 'KDJ',
                    'desc':      f'KDJ死叉(K={latest["K"]:.1f})',
                    'strength':  '中等',
                    'action':    '观望'
                })
                score -= 1

    if not signals:
        return None

    # ── 趋势判定（七档）──────────────────────────────────────
    if score >= 4:
        trend = '强势'
    elif score >= 2:
        trend = '偏强'
    elif score == 1:
        trend = '轻微偏强'
    elif score == -1:
        trend = '轻微偏弱'
    elif score <= -4:
        trend = '弱势'
    elif score <= -2:
        trend = '偏弱'
    else:
        trend = '震荡'

    close_price = _safe_round(latest['close'], 2)
    change_pct  = _safe_round(
        (latest['close'] - prev['close']) / prev['close'] * 100
        if prev['close'] != 0 else 0,
        2
    )

    if hasattr(latest.name, 'strftime'):
        date_str = latest.name.strftime('%Y-%m-%d')
    else:
        date_str = str(latest.name)[:10]

    return {
        'symbol':           code,
        'code':             code,
        'name':             name,
        'date':             date_str,
        'close':            close_price,
        'price':            close_price,
        'change_pct':       change_pct,
        'volume':           _safe_int_volume(latest.get('volume', None)),
        'ma5':              _safe_round(latest.get('MA5'),        2),
        'ma10':             _safe_round(latest.get('MA10'),       2),
        'ma20':             _safe_round(latest.get('MA20'),       2),
        'rsi':              _safe_round(latest.get('RSI'),        2),
        'dif':              _safe_round(latest.get('DIF'),        4),
        'dea':              _safe_round(latest.get('DEA'),        4),
        'macd_bar':         _safe_round(latest.get('MACD'),       4),
        'macd':             _safe_round(latest.get('MACD'),       4),
        'kdj_k':            _safe_round(latest.get('K'),          2),
        'kdj_d':            _safe_round(latest.get('D'),          2),
        'kdj_j':            _safe_round(latest.get('J'),          2),
        'boll_upper':       _safe_round(latest.get('BOLL_UPPER'), 2),
        'boll_lower':       _safe_round(latest.get('BOLL_LOWER'), 2),
        'boll_width':       _safe_round(latest.get('BOLL_WIDTH'), 4),
        'signals':          signals,
        'score':            score,
        'trend':            trend,
        'volume_confirmed': volume_confirm(data, ratio=vol_ratio),
        'price_history':    [
            round(float(v), 2)
            for v in data['close'].tail(30).tolist()
            if pd.notna(v)
        ],
    }
