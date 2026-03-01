"""
技术指标计算模块
支持：均线、MACD、RSI、布林带
"""

import pandas as pd
import numpy as np


def calculate_ma(data, periods=[5, 10, 20, 60]):
    """
    计算移动平均线
    
    Args:
        data: DataFrame with 'close' column
        periods: list of int, 均线周期列表
    
    Returns:
        DataFrame with MA columns
    """
    for period in periods:
        data[f'MA{period}'] = data['close'].rolling(window=period, min_periods=1).mean()
    return data


def calculate_macd(data, fast=12, slow=26, signal=9):
    """
    计算MACD指标
    
    Args:
        data: DataFrame with 'close' column
        fast: int, 快线周期
        slow: int, 慢线周期
        signal: int, 信号线周期
    
    Returns:
        DataFrame with DIF, DEA, MACD columns
    """
    ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = data['close'].ewm(span=slow, adjust=False).mean()
    
    data['DIF'] = ema_fast - ema_slow
    data['DEA'] = data['DIF'].ewm(span=signal, adjust=False).mean()
    data['MACD'] = 2 * (data['DIF'] - data['DEA'])
    data['MACD_HIST'] = data['MACD']
    
    return data


def calculate_rsi(data, period=14):
    """
    计算RSI相对强弱指数
    
    Args:
        data: DataFrame with 'close' column
        period: int, RSI周期
    
    Returns:
        DataFrame with RSI column
    """
    delta = data['close'].diff()
    
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    
    avg_gain = gain.rolling(window=period, min_periods=1).mean()
    avg_loss = loss.rolling(window=period, min_periods=1).mean()
    
    rs = avg_gain / avg_loss
    data['RSI'] = 100 - (100 / (1 + rs))
    
    return data


def calculate_bollinger(data, period=20, std_dev=2):
    """
    计算布林带
    
    Args:
        data: DataFrame with 'close' column
        period: int, 周期
        std_dev: float, 标准差倍数
    
    Returns:
        DataFrame with BOLL_UPPER, BOLL_MID, BOLL_LOWER columns
    """
    data['BOLL_MID'] = data['close'].rolling(window=period, min_periods=1).mean()
    data['BOLL_STD'] = data['close'].rolling(window=period, min_periods=1).std()
    
    data['BOLL_UPPER'] = data['BOLL_MID'] + (data['BOLL_STD'] * std_dev)
    data['BOLL_LOWER'] = data['BOLL_MID'] - (data['BOLL_STD'] * std_dev)
    
    data['BOLL_WIDTH'] = (data['BOLL_UPPER'] - data['BOLL_LOWER']) / data['BOLL_MID']
    data['BOLL_PERCENT'] = (data['close'] - data['BOLL_LOWER']) / (data['BOLL_UPPER'] - data['BOLL_LOWER'])
    
    return data


def calculate_kdj(data, n=9, m1=3, m2=3):
    """
    计算KDJ指标
    
    Args:
        data: DataFrame with 'high', 'low', 'close' columns
        n: int, RSV周期
        m1: int, K平滑周期
        m2: int, D平滑周期
    
    Returns:
        DataFrame with K, D, J columns
    """
    low_list = data['low'].rolling(window=n, min_periods=1).min()
    high_list = data['high'].rolling(window=n, min_periods=1).max()
    
    rsv = (data['close'] - low_list) / (high_list - low_list) * 100
    
    data['K'] = rsv.ewm(com=m1-1, adjust=False).mean()
    data['D'] = data['K'].ewm(com=m2-1, adjust=False).mean()
    data['J'] = 3 * data['K'] - 2 * data['D']
    
    return data


def check_signals(data, code, name):
    """
    检查交易信号
    
    Args:
        data: DataFrame with technical indicators
        code: str, 股票代码
        name: str, 股票名称
    
    Returns:
        dict with signal information
    """
    if len(data) < 2:
        return None
    
    latest = data.iloc[-1]
    prev = data.iloc[-2]
    
    signals = []
    score = 0  # 综合评分
    
    # 1. 均线突破/回调
    if prev['close'] < prev['MA5'] and latest['close'] > latest['MA5']:
        signals.append({
            'type': '突破',
            'indicator': 'MA5',
            'desc': '股价向上突破5日均线',
            'strength': '中等',
            'action': '关注'
        })
        score += 1
    elif prev['close'] > prev['MA5'] and latest['close'] < latest['MA5']:
        signals.append({
            'type': '回调',
            'indicator': 'MA5',
            'desc': '股价回调跌破5日均线',
            'strength': '中等',
            'action': '观望'
        })
        score -= 1
    
    # 2. 均线金叉/死叉（5日 vs 10日）
    if prev['MA5'] < prev['MA10'] and latest['MA5'] > latest['MA10']:
        signals.append({
            'type': '金叉',
            'indicator': 'MA',
            'desc': '5日线上穿10日线，短期趋势转强',
            'strength': '强',
            'action': '买入信号'
        })
        score += 2
    elif prev['MA5'] > prev['MA10'] and latest['MA5'] < latest['MA10']:
        signals.append({
            'type': '死叉',
            'indicator': 'MA',
            'desc': '5日线下穿10日线，短期趋势转弱',
            'strength': '强',
            'action': '卖出信号'
        })
        score -= 2
    
    # 3. MACD金叉/死叉
    if not pd.isna(prev['DIF']) and not pd.isna(prev['DEA']):
        if prev['DIF'] < prev['DEA'] and latest['DIF'] > latest['DEA']:
            signals.append({
                'type': '金叉',
                'indicator': 'MACD',
                'desc': 'MACD金叉，动能转强',
                'strength': '强',
                'action': '买入信号'
            })
            score += 2
        elif prev['DIF'] > prev['DEA'] and latest['DIF'] < latest['DEA']:
            signals.append({
                'type': '死叉',
                'indicator': 'MACD',
                'desc': 'MACD死叉，动能转弱',
                'strength': '强',
                'action': '卖出信号'
            })
            score -= 2
    
    # 4. RSI超买/超卖
    if not pd.isna(latest['RSI']):
        if latest['RSI'] < 30:
            signals.append({
                'type': '超卖',
                'indicator': 'RSI',
                'desc': f'RSI={latest["RSI"]:.1f}，进入超卖区间',
                'strength': '中等',
                'action': '关注反弹'
            })
            score += 1
        elif latest['RSI'] > 70:
            signals.append({
                'type': '超买',
                'indicator': 'RSI',
                'desc': f'RSI={latest["RSI"]:.1f}，进入超买区间',
                'strength': '中等',
                'action': '注意回调'
            })
            score -= 1
    
    # 5. 布林带突破
    if not pd.isna(latest['BOLL_UPPER']):
        if latest['close'] > latest['BOLL_UPPER']:
            signals.append({
                'type': '突破上轨',
                'indicator': 'BOLL',
                'desc': '股价突破布林带上轨，强势',
                'strength': '中等',
                'action': '持有/减仓'
            })
        elif latest['close'] < latest['BOLL_LOWER']:
            signals.append({
                'type': '跌破下轨',
                'indicator': 'BOLL',
                'desc': '股价跌破布林带下轨，超跌',
                'strength': '中等',
                'action': '关注反弹'
            })
    
    # 6. KDJ金叉/死叉
    if not pd.isna(prev['K']) and not pd.isna(prev['D']):
        if prev['K'] < prev['D'] and latest['K'] > latest['D'] and latest['K'] < 50:
            signals.append({
                'type': '金叉',
                'indicator': 'KDJ',
                'desc': 'KDJ低位金叉，反弹信号',
                'strength': '中等',
                'action': '关注'
            })
            score += 1
    
    if not signals:
        return None
    
    # 综合判断
    trend = '强势' if score >= 2 else ('弱势' if score <= -2 else '震荡')
    
    return {
        'code': code,
        'name': name,
        'date': str(latest.name) if hasattr(latest, 'name') else str(latest.get('date', '')),
        'price': round(latest['close'], 2),
        'change_pct': round((latest['close'] - prev['close']) / prev['close'] * 100, 2) if prev['close'] != 0 else 0,
        'volume': int(latest.get('volume', 0)),
        'ma5': round(latest['MA5'], 2),
        'ma10': round(latest['MA10'], 2),
        'ma20': round(latest['MA20'], 2),
        'rsi': round(latest['RSI'], 2) if not pd.isna(latest['RSI']) else None,
        'macd': round(latest['MACD'], 4) if not pd.isna(latest['MACD']) else None,
        'signals': signals,
        'score': score,
        'trend': trend
    }
