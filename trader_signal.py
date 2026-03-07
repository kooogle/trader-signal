#!/usr/bin/env python3
"""
期货信号系统 - 天勤数据源
"""

import json
import os
import sys
import urllib.request
import urllib.parse
import random
from datetime import datetime
from typing import Dict, List, Optional

# ============== 配置区 ==============
# 天勤账号密码
TQ_ACCOUNT = "15572009997"
TQ_PASSWORD = "zp123789"

# 交易品种 - 使用天勤主力合约代码
SYMBOLS = [
    {"code": "TA0", "name": "PTA", "exchange": "CZCE"},
    {"code": "OI0", "name": "菜籽油", "exchange": "CZCE"},
    {"code": "V0", "name": "PVC", "exchange": "CZCE"},
    {"code": "P0", "name": "棕榈油", "exchange": "DCE"},
]

# 飞书配置
APP_ID = "cli_a920fdf0183a9bd1"
APP_SECRET = "yjoGDTFVobSRRrEzLx81JhGl7NWvBrrG"
RECEIVER_ID = "ou_39144282ce2b237a8f95c7c9a30037bf"

STATE_FILE = "signal_state.json"
HISTORY_FILE = "signal_history.json"

# ============== 飞书 ==============
_feishu_access_token = None

def get_feishu_access_token() -> Optional[str]:
    global _feishu_access_token
    if _feishu_access_token:
        return _feishu_access_token
    
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {"app_id": APP_ID, "app_secret": APP_SECRET}
    
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            if result.get('code') == 0:
                _feishu_access_token = result['tenant_access_token']
                return _feishu_access_token
    except Exception as e:
        print(f"获取token失败: {e}")
    return None

def send_feishu_message(open_id: str, message: str) -> bool:
    token = get_feishu_access_token()
    if not token:
        return False
    
    url = "https://open.feishu.cn/open-apis/im/v1/messages"
    params = {"receive_id_type": "open_id"}
    payload = {"receive_id": open_id, "msg_type": "text", "content": json.dumps({"text": message})}
    
    try:
        query_string = urllib.parse.urlencode(params)
        full_url = f"{url}?{query_string}"
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(full_url, data=data, headers={'Content-Type': 'application/json', 'Authorization': f'Bearer {token}'})
        with urllib.request.urlopen(req, timeout=10) as response:
            result = json.loads(response.read().decode())
            if result.get('code') == 0:
                print("✅ 飞书消息发送成功")
                return True
            else:
                print(f"❌ 发送失败: {result.get('msg')}")
    except Exception as e:
        print(f"❌ 发送失败: {e}")
    return False

# ============== 状态管理 ==============
def load_state() -> Dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {}

def save_state(state: Dict):
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f)

def load_history() -> Dict:
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r') as f:
                return json.load(f)
        except:
            pass
    return {"signals": []}

def save_history(history: Dict):
    try:
        with open(HISTORY_FILE, 'w') as f:
            json.dump(history, f)
    except Exception as e:
        print(f"保存历史记录失败: {e}")

def add_history(symbol: Dict, signal: Dict):
    history = load_history()
    code = symbol["code"]
    contract_month = code.split('.')[0][-4:]
    
    history["signals"].append({
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "symbol": f"{symbol['name']}{contract_month}",
        "type": signal["type"],
        "reason": signal["reason"],
        "price": signal["price"]
    })
    
    # 只保留最近100条
    if len(history["signals"]) > 100:
        history["signals"] = history["signals"][-100:]
    
    save_history(history)

def get_signal_key(symbol_code: str) -> str:
    return symbol_code.split('.')[0]

# ============== 天勤数据 ==============
def get_kline_from_tqsdk(symbol: str, duration: int = 300, count: int = 100) -> Optional[Dict]:
    """天勤数据"""
    try:
        from tqsdk import TqApi, TqAuth
        import time
        
        # 合约代码直接使用
        tqsdk_symbol = symbol
        
        print(f"正在连接天勤...")
        
        try:
            if TQ_ACCOUNT and TQ_PASSWORD:
                print(f"使用账号登录: {TQ_ACCOUNT}")
                api = TqApi(auth=TqAuth(TQ_ACCOUNT, TQ_PASSWORD), timeout=30)
            else:
                api = TqApi(timeout=30)
        except Exception as auth_error:
            print(f"账号登录失败: {auth_error}")
            try:
                api = TqApi(timeout=30)
            except Exception as e:
                print(f"游客登录也失败: {e}")
                return None
        
        print(f"天勤连接成功, 获取{tqsdk_symbol}数据...")
        time.sleep(1)  # 等待连接稳定
        
        try:
            print(f"请求K线数据: {tqsdk_symbol}, duration={duration}")
            klines = api.get_kline_serial(tqsdk_symbol, duration, count)
            
            close_series = klines.get('close')
            if close_series is None:
                print("K线close数据为空，尝试获取日K...")
                api.close()
                # 尝试日K
                try:
                    api2 = TqApi(timeout=30)
                    klines2 = api2.get_kline_serial(tqsdk_symbol, 86400, 30)
                    close2 = klines2.get('close')
                    vol2 = klines2.get('volume')
                    prices2 = close2.tolist() if close2 is not None else []
                    volumes2 = vol2.tolist() if vol2 is not None else []
                    api2.close()
                    print(f"日K获取到{len(prices2)}条数据")
                    return {"symbol": symbol, "prices": prices2, "volumes": volumes2, "source": "tqsdk_daily"}
                except Exception as e2:
                    print(f"日K也失败: {e2}")
                    return None
            
            prices = close_series.tolist()
            volumes = klines.get('volume', []).tolist() if klines.get('volume') is not None else []
            
            api.close()
            print(f"成功获取{len(prices)}条K线数据")
            return {"symbol": symbol, "prices": prices, "volumes": volumes, "source": "tqsdk"}
            
        except Exception as e:
            print(f"获取K线失败: {e}")
            try:
                api.close()
            except:
                pass
            return None
            
    except ImportError:
        print("天勤SDK未安装")
    except Exception as e:
        print(f"天勤错误: {e}")
    return None

def get_kline_data(symbol: str, count: int = 100) -> Dict:
    """获取K线数据"""
    data = get_kline_from_tqsdk(symbol, 300, count)
    if data:
        print(f"✅ 天勤数据: {symbol}")
        return data
    
    print(f"❌ 天勤数据获取失败: {symbol}")
    exit(1)

# ============== 技术指标 ==============
def calculate_ma(data: List[float], period: int) -> List[float]:
    result = []
    for i in range(len(data)):
        if i < period - 1:
            result.append(None)
        else:
            result.append(round(sum(data[i-period+1:i+1]) / period, 2))
    return result

def calculate_bollinger(data: List[float], period: int = 20) -> Dict:
    ma = calculate_ma(data, period)
    upper, lower = [], []
    for i in range(len(data)):
        if i < period - 1:
            upper.append(None)
            lower.append(None)
        else:
            slice_data = data[i-period+1:i+1]
            avg = sum(slice_data) / period
            std = (sum((x - avg) ** 2 for x in slice_data) / period) ** 0.5
            upper.append(round(avg + 2 * std, 2))
            lower.append(round(avg - 2 * std, 2))
    return {"middle": ma, "upper": upper, "lower": lower}

def calculate_rsi(data: List[float], period: int = 14) -> List[float]:
    result = [None]
    gains, losses = [], []
    for i in range(1, len(data)):
        change = data[i] - data[i-1]
        gains.append(change if change > 0 else 0)
        losses.append(abs(change) if change < 0 else 0)
    
    for i in range(1, len(data)):
        avg_gain = sum(gains[:i]) / i if i > 0 else 0
        avg_loss = sum(losses[:i]) / i if i > 0 else 0
        if avg_loss == 0:
            result.append(100)
        else:
            rsi = 100 - (100 / (1 + avg_gain / avg_loss))
            result.append(round(rsi, 2))
    return result

def calculate_atr(prices: List[float], period: int = 14) -> List[float]:
    if len(prices) < period + 1:
        return [None] * len(prices)
    
    tr = []
    for i in range(1, len(prices)):
        h_l = prices[i] - prices[i-1]
        h_c = abs(prices[i] - prices[i-1])
        l_c = abs(prices[i-1] - prices[i-1])
        tr.append(max(h_l, h_c, l_c))
    
    atr = []
    for i in range(len(tr)):
        if i < period - 1:
            atr.append(None)
        else:
            avg = sum(tr[i-period+1:i+1]) / period
            atr.append(round(avg, 2))
    return atr

# ============== 信号检测 ==============
def detect_signals(prices: List[float], volumes: List[float]) -> Optional[Dict]:
    if len(prices) < 30:
        return None
    
    ma5 = calculate_ma(prices, 5)
    ma20 = calculate_ma(prices, 20)
    ma60 = calculate_ma(prices, 60)
    bb = calculate_bollinger(prices)
    rsi = calculate_rsi(prices)
    atr = calculate_atr(prices)
    
    price = prices[-1]
    ma5_v, ma20_v, ma60_v = ma5[-1], ma20[-1], ma60[-1] if len(ma60) > 0 and ma60[-1] else None
    bb_m = bb["middle"][-1]
    rsi_v = rsi[-1]
    atr_v = atr[-1] if atr and atr[-1] else 0
    
    if None in [ma5_v, ma20_v, bb_m, rsi_v]:
        return None
    
    # 做多
    long_cond1 = (price > ma20_v and ma5_v > ma20_v and bb_m > bb["middle"][-2] and 35 < rsi_v < 75)
    long_cond2 = (bb["upper"][-1] and price > bb["upper"][-1] and volumes[-1] > volumes[-2] * 1.3 and (price - bb["upper"][-1]) > atr_v * 0.3)
    
    if long_cond1 or long_cond2:
        reason = "均线多头+布林中轨向上+RSI" if long_cond1 else f"突破布林上轨+放量(ATR:{atr_v})"
        return {"type": "LONG", "reason": reason, "price": price}
    
    # 做空
    short_cond1 = (price < ma20_v and ma5_v < ma20_v and bb_m < bb["middle"][-2] and 25 < rsi_v < 65)
    short_cond2 = (bb["lower"][-1] and price < bb["lower"][-1] and volumes[-1] > volumes[-2] * 1.3 and (bb["lower"][-1] - price) > atr_v * 0.3)
    
    if short_cond1 or short_cond2:
        reason = "均线空头+布林中轨向下+RSI" if short_cond1 else f"跌破布林下轨+放量(ATR:{atr_v})"
        return {"type": "SHORT", "reason": reason, "price": price}
    
    return None

# ============== 主程序 ==============
def send_signal(symbol: Dict, signal: Dict):
    emoji = "🔴" if signal["type"] == "LONG" else "🟢"
    direction = "做多" if signal["type"] == "LONG" else "做空"
    
    code = symbol["code"]
    contract_month = code.split('.')[0][-4:]
    
    message = f"""【期货信号】

{emoji} {symbol['name']}{contract_month} {direction}

原因: {signal['reason']}
价格: {signal['price']}
时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}"""
    
    send_feishu_message(RECEIVER_ID, message)

def main():
    print("="*50)
    print("期货信号系统")
    print("="*50)
    
    prev_state = load_state()
    current_state = {}
    
    for symbol in SYMBOLS:
        code = symbol["code"]
        name = symbol["name"]
        key = get_signal_key(code)
        
        print(f"\n分析: {name}")
        
        data = get_kline_data(code)
        signal = detect_signals(data["prices"], data["volumes"])
        
        current = signal["type"] if signal else "NONE"
        previous = prev_state.get(key, "NONE")
        
        if signal and current != previous:
            print(f"📢 新信号: {current}")
            send_signal(symbol, signal)
            add_history(symbol, signal)  # 记录历史
        elif signal:
            print(f"📊 保持: {current}")
        else:
            print("无信号")
        
        current_state[key] = current
    
    save_state(current_state)
    print("\n完成!")

if __name__ == "__main__":
    main()
