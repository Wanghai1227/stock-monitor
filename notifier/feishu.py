"""
飞书机器人推送模块
参考 stock-monitor 的卡片风格，适配 PosiSense 仓位报告
"""
import os
import requests
from datetime import datetime


def _position_color(pos: int) -> str:
    """仓位对应飞书卡片颜色"""
    if pos >= 80:
        return "green"
    elif pos >= 60:
        return "turquoise"
    elif pos >= 40:
        return "yellow"
    elif pos >= 20:
        return "orange"
    else:
        return "red"


def _position_label(pos: int) -> str:
    if pos >= 80:
        return "🟢 积极进攻"
    elif pos >= 60:
        return "🟡 标准持仓"
    elif pos >= 40:
        return "🟠 谨慎持仓"
    elif pos >= 20:
        return "🔴 轻仓防守"
    else:
        return "⚫ 空仓观望"


def _score_bar(score: float) -> str:
    """将 -1~+1 的得分渲染为 emoji 进度条"""
    filled = int((score + 1.0) * 5)
    filled = max(0, min(10, filled))
    return "█" * filled + "░" * (10 - filled)


def build_card(result: dict, gs: dict, gsc: dict, ash: dict, asc: dict) -> dict:
    """
    构建飞书消息卡片（card 类型）
    文档：https://open.feishu.cn/document/ukTMukTMukTM/uAjNwUjLwYDM1
    """
    pos        = result["position"]
    score      = result["composite_score"]
    vix_hit    = result["vix_override"]
    layers     = result["layer_scores"]
    now_str    = datetime.now().strftime("%Y-%m-%d %H:%M")
    color      = _position_color(pos)
    label      = _position_label(pos)

    # ── 各层得分文本 ──────────────────────────────
    layer_lines = []
    layer_name_map = {
        "gs":  "全球情绪",
        "gsc": "全球行业",
        "ash": "A股情绪",
        "asc": "A股行业",
    }
    for key, name in layer_name_map.items():
        s = layers.get(key, 0.0)
        bar = _score_bar(s)
        layer_lines.append(f"**{name}** `[{bar}]` {s:+.3f}")

    # ── 行业信息 ──────────────────────────────────
    global_strong = "、".join(gsc.get("strong", [])) or "—"
    global_weak   = "、".join(gsc.get("weak",   [])) or "—"
    ashare_strong = "、".join(asc.get("strong", [])) or "—"
    ashare_weak   = "、".join(asc.get("weak",   [])) or "—"

    # ── 全球情绪关键指标 ─────────────────────────
    gs_detail = gs.get("detail", {})
    sp500_chg  = gs_detail.get("标普500涨跌", "—")
    nasdaq_chg = gs_detail.get("纳斯达克涨跌", "—")
    vix_val    = gs_detail.get("VIX", "—")

    # ── A股情绪关键指标 ──────────────────────────
    ash_detail  = ash.get("detail", {})
    sh_chg      = ash_detail.get("上证涨跌", "—")
    sz_chg      = ash_detail.get("深证涨跌", "—")
    limit_up    = ash_detail.get("涨停数", "—")
    limit_down  = ash_detail.get("跌停数", "—")

    vix_warn = "\n⚠️ **VIX 熔断已触发，仓位已强制调整**" if vix_hit else ""

    card = {
        "msg_type": "interactive",
        "card": {
            "config": {"wide_screen_mode": True},
            "header": {
                "title": {
                    "tag":     "plain_text",
                    "content": f"📊 PosiSense 仓位报告  |  {now_str}"
                },
                "template": color
            },
            "elements": [
                # ── 主仓位 ──────────────────────────
                {
                    "tag": "div",
                    "text": {
                        "tag":     "lark_md",
                        "content": (
                            f"## 建议仓位：**{pos}%**　{label}"
                            f"{vix_warn}\n"
                            f"综合得分：`{score:+.3f}`"
                        )
                    }
                },
                {"tag": "hr"},
                # ── 各层得分 ────────────────────────
                {
                    "tag": "div",
                    "text": {
                        "tag":     "lark_md",
                        "content": "**📐 各层得分**\n" + "\n".join(layer_lines)
                    }
                },
                {"tag": "hr"},
                # ── 市场快照（双列） ─────────────────
                {
                    "tag": "column_set",
                    "flex_mode": "bisect",
                    "columns": [
                        {
                            "tag": "column",
                            "elements": [{
                                "tag": "div",
                                "text": {
                                    "tag":     "lark_md",
                                    "content": (
                                        "**🌐 全球市场**\n"
                                        f"标普500：{sp500_chg}\n"
                                        f"纳斯达克：{nasdaq_chg}\n"
                                        f"VIX：{vix_val}\n\n"
                                        f"📈 强势：{global_strong}\n"
                                        f"📉 弱势：{global_weak}"
                                    )
                                }
                            }]
                        },
                        {
                            "tag": "column",
                            "elements": [{
                                "tag": "div",
                                "text": {
                                    "tag":     "lark_md",
                                    "content": (
                                        "**🇨🇳 A股市场**\n"
                                        f"上证：{sh_chg}\n"
                                        f"深证：{sz_chg}\n"
                                        f"涨停/跌停：{limit_up} / {limit_down}\n\n"
                                        f"📈 强势：{ashare_strong}\n"
                                        f"📉 弱势：{ashare_weak}"
                                    )
                                }
                            }]
                        }
                    ]
                },
                {"tag": "hr"},
                # ── 底部备注 ────────────────────────
                {
                    "tag": "note",
                    "elements": [{
                        "tag":     "plain_text",
                        "content": "⚠️ 本报告仅供参考，不构成投资建议。股市有风险，投资需谨慎。"
                    }]
                }
            ]
        }
    }
    return card


def send_feishu(result: dict, gs: dict, gsc: dict, ash: dict, asc: dict,
                webhook: str = None) -> dict:
    """
    发送飞书卡片消息
    webhook 优先级：参数 > 环境变量 FEISHU_WEBHOOK
    """
    webhook = webhook or os.getenv("FEISHU_WEBHOOK")
    if not webhook:
        print("  ⏭️  未配置 FEISHU_WEBHOOK，跳过飞书推送")
        return {"status": "skip", "reason": "no webhook"}

    card = build_card(result, gs, gsc, ash, asc)
    try:
        resp = requests.post(webhook, json=card, timeout=10)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            print("  ✅ 飞书推送成功")
            return {"status": "ok"}
        else:
            print(f"  ⚠️  飞书推送异常: {resp.status_code} {resp.text}")
            return {"status": "error", "code": resp.status_code, "body": resp.text}
    except Exception as e:
        print(f"  ❌ 飞书推送失败: {e}")
        return {"status": "error", "msg": str(e)}
