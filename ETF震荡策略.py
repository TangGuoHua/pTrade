# -*- coding: utf-8 -*-
# PTrade Python 模板：ETF震荡策略（RSI + 支撑/压力位 + 成交量确认）
# TODO: 用你的PTrade账户行情/下单API替换标注处

import numpy as np
import datetime

############################
# 1) 初始化
############################
def initialize(context):
    # —— 固定ETF池（示例）——
    context.stock_list = [
        '513360.SS', '159985.SZ', '513310.SS', '518880.SS',
        '513060.SS', '513120.SS', '513010.SS', '159740.SZ',
        '511380.SS', '513330.SS', '513130.SS', '513090.SS'
    ]
    set_universe(context.stock_list)
    log.info("ETF RSI震荡策略初始化完成, 交易ETF数: {}".format(len(context.stock_list)))

    # —— 区间震荡识别（参考值）——
    context.range_period = 60
    context.range_ratio_max = 0.15
    context.range_ratio_min = 0.05
    context.trend_threshold = 0.03

    # —— 资金管理 —— 
    context.max_capital = 20_000_000
    context.max_positions = 5
    context.max_capital_per_stock = 20_000

    # —— RSI参数（短周期 + 动态阈值）——
    context.rsi_short = 6
    context.rsi_buy_base = 30
    context.rsi_sell_base = 85
    context.rsi_float_range = 20  # buy/sell阈值动态调整幅度

    # —— 支撑/压力位识别 —— 
    context.support_period = 20
    context.price_threshold = 0.015  # 与现价的相对距离阈值（1.5%内）

    # —— 买入触发（至少满足3项）——
    context.intraday_high_pct = 0.02   # 开盘涨幅阈值
    context.min_high_pct = 0.015       # 最低点涨幅阈值
    context.entry_high_pct = 0.015     # 当前价涨幅阈值
    context.vol_surge_ratio = 1.5      # 成交量激增倍数

    # —— 卖出条件与持仓上限 —— 
    context.slow_rise_pct = 0.03
    context.max_holding_days = 15

    # —— 风控：止损/止盈/回撤 —— 
    context.stop_loss_pct = 0.015
    context.profit_target_pct = 0.05
    context.profit_retreat_pct = 0.01

    # —— 冷静期（技术性卖出/盈利卖出）——
    context.cooling_days_technical = 1
    context.cooling_days_profit = 2

    # —— 运行期状态缓存 —— 
    context.stock_data = {}        # 每标的的运行期状态
    context.filtered_stocks = []   # 当日可交易标的集合
    context.data_cache = {}        # 行情缓存（可选）
    context.last_filter_day = None

############################
# 2) 盘前处理
############################
def before_trading_start(context, data):
    current_date = get_trading_day()

    # 当日复用缓存池，避免重复筛选
    if context.last_filter_day == current_date:
        log.info("使用缓存ETF列表，共{}只".format(len(context.filtered_stocks)))
        return

    context.last_filter_day = current_date
    context.filtered_stocks = context.stock_list.copy()
    log.info("固定ETF列表数量: {}".format(len(context.filtered_stocks)))

    # 补录已持仓但不在池的标的，确保能在当日卖出
    for stock in list(context.stock_data.keys()):
        position = get_position(stock)
        has_position = (position is not None and getattr(position, "amount", 0) > 0)
        if has_position and stock not in context.filtered_stocks:
            context.filtered_stocks.append(stock)
        elif (not has_position) and stock not in context.filtered_stocks:
            # 清理无仓位的历史缓存
            context.stock_data.pop(stock, None)

    # 初始化/更新每标的的当日状态
    for stock in context.filtered_stocks:
        if stock not in context.stock_data:
            context.stock_data[stock] = {
                "hold_position": False,
                "can_buy": True,
                "entry_price": 0.0,
                "entry_date": None,
                "today_open": None,
                "today_low": None,
                "intraday_prices": [],
                "intraday_volumes": [],
                "day_high_price": 0.0,
                "vol_avg": None,
                "max_profit_pct": 0.0,
                "cooling_days_left": 0,
                "holding_days": 0
            }

        # 冷静期滚动
        if context.stock_data[stock]["cooling_days_left"] > 0:
            context.stock_data[stock]["cooling_days_left"] -= 1
            context.stock_data[stock]["can_buy"] = False
        else:
            context.stock_data[stock]["can_buy"] = True

        # 重置当日盘中数据
        sd = context.stock_data[stock]
        sd["today_open"] = None
        sd["today_low"] = None
        sd["intraday_prices"] = []
        sd["intraday_volumes"] = []
        sd["day_high_price"] = 0.0
        sd["vol_avg"] = None

############################
# 3) 盘中主逻辑
############################
def handle_data(context, data):
    current_dt = getattr(data, "current_dt", datetime.datetime.now())

    # 尾盘10分钟只允许卖出，禁止新开仓
    if is_last_10_minutes(current_dt):
        execute_sell_block(context, data)
        return

    # 盘中正常跟踪价量，延迟至尾盘再统一评估买入
    track_intraday_state(context, data)

    # 非尾盘不买；尾盘再集中评估买入
    if should_do_tailend_buy_check(current_dt):
        execute_buy_block_tailend(context, data)

############################
# 4) 收盘后
############################
def after_trading_end(context, data):
    # 可记录当日统计、风险状态、日志等
    log.info("收盘：总资产{:.2f}万, 现金{:.2f}万, 持仓数{}".format(
        context.portfolio.total_value/10000,
        context.portfolio.cash/10000,
        len([p for p in context.portfolio.positions.values() if p.amount > 0])
    ))
    # 可标记次日方向性信号（示例）
    for stock in context.filtered_stocks:
        if stock in context.stock_data:
            context.stock_data[stock]["market_signal"] = "bullish"

############################
# 5) 价量跟踪与计算
############################
def track_intraday_state(context, data):
    for stock in context.filtered_stocks:
        price = get_current_price(stock)        # TODO: 对接PTrade行情
        volume = get_current_volume(stock)      # TODO: 对接PTrade行情
        if price is None or volume is None:
            continue

        sd = context.stock_data[stock]
        sd["intraday_prices"].append(price)
        sd["intraday_volumes"].append(volume)

        if sd["today_open"] is None:
            sd["today_open"] = price
            sd["today_low"] = price
        sd["today_low"] = min(sd["today_low"], price)
        sd["day_high_price"] = max(sd["day_high_price"], price)

############################
# 6) 尾盘买卖执行块
############################
def execute_sell_block(context, data):
    # 仅卖出逻辑：止损、止盈、回撤、技术性卖出
    for stock, position in context.portfolio.positions.items():
        if getattr(position, "amount", 0) <= 0:
            continue

        price = get_current_price(stock)  # TODO: 对接行情
        if price is None:
            continue

        sd = context.stock_data.get(stock, {})
        entry_price = sd.get("entry_price", 0.0) or get_position_cost(position)
        if entry_price <= 0:
            continue

        profit_pct = price / entry_price - 1
        sd["max_profit_pct"] = max(sd.get("max_profit_pct", 0.0), profit_pct)

        # A. 止损
        if profit_pct <= -context.stop_loss_pct:
            place_sell_all(context, stock)  # TODO: 替换为PTrade卖出函数
            mark_cooling(sd, technical=True)
            log.info("[SELL-STOP] {} 盈亏{:.2%}".format(stock, profit_pct))
            continue

        # B. 止盈
        if profit_pct >= context.profit_target_pct:
            place_sell_all(context, stock)
            mark_cooling(sd, technical=False)  # 盈利卖出 → 较长冷静期
            log.info("[SELL-TP] {} 盈亏{:.2%}".format(stock, profit_pct))
            continue

        # C. 盈利回撤
        if sd["max_profit_pct"] - profit_pct >= context.profit_retreat_pct:
            place_sell_all(context, stock)
            mark_cooling(sd, technical=False)
            log.info("[SELL-RETREAT] {} 回撤达阈值".format(stock))
            continue

        # D. 技术性卖出（RSI过热/压力位突破缓涨）
        rsi = compute_rsi_daily(stock, context.rsi_short)  # TODO: 对接日线历史
        support, resistance = compute_sr_daily(stock, context)
        if rsi is not None:
            buy_th, sell_th = adjust_rsi_threshold(price, support, resistance, context)
            if rsi >= sell_th or is_slow_rise_sell_condition(price, resistance, rsi, sd, context):
                place_sell_all(context, stock)
                mark_cooling(sd, technical=True)
                log.info("[SELL-TECH] {} RSI={:.1f} 阈={}/{}".format(stock, rsi, buy_th, sell_th))

def execute_buy_block_tailend(context, data):
    # 头寸限制
    held_positions = len([p for p in context.portfolio.positions.values() if p.amount > 0])
    if held_positions >= context.max_positions:
        return

    # 仅评估未持仓且不在冷静期的标的
    candidates = [s for s in context.filtered_stocks
                  if (s not in context.portfolio.positions or context.portfolio.positions[s].amount == 0)
                  and context.stock_data.get(s, {}).get("can_buy", True)]

    for stock in candidates:
        sd = context.stock_data[stock]
        price = get_current_price(stock)  # TODO: 对接行情
        if price is None or not sd["intraday_prices"]:
            continue

        # 1) 计算三项涨幅条件
        today_open = sd["today_open"] or price
        today_low = sd["today_low"] or price
        cond1 = (price / today_open - 1) >= context.intraday_high_pct
        cond2 = (sd["day_high_price"] / today_low - 1) >= context.min_high_pct
        cond3 = (price / today_low - 1) >= context.entry_high_pct

        # 2) 成交量激增
        cond4 = False
        if len(sd["intraday_volumes"]) > 20:
            avg_vol = np.mean(sd["intraday_volumes"][-20:-1])
            cur_vol = sd["intraday_volumes"][-1]
            cond4 = cur_vol > avg_vol * context.vol_surge_ratio

        trigger_ok = sum([cond1, cond2, cond3, cond4]) >= 3
        if not trigger_ok:
            continue

        # 3) RSI与支撑位确认（低位买入）
        rsi = compute_rsi_daily(stock, context.rsi_short)         # TODO: 对接日线历史
        support, resistance = compute_sr_daily(stock, context)    # TODO: 对接日线高低收
        if rsi is None:
            continue
        buy_th, sell_th = adjust_rsi_threshold(price, support, resistance, context)
        near_support = (support is not None) and ((price - support) / price) < context.price_threshold
        if rsi <= buy_th and near_support:
            # 4) 资金约束与下单
            capital = min(context.max_capital_per_stock, context.portfolio.cash)
            if capital > 0:
                place_buy_value(context, stock, capital)  # TODO: 替换为PTrade买入函数
                sd["entry_price"] = price
                sd["entry_date"] = get_trading_day()
                sd["hold_position"] = True
                sd["holding_days"] = 0
                sd["max_profit_pct"] = 0.0
                log.info("[BUY] {} 价格{:.4f} RSI={:.1f}/{:.1f}".format(stock, price, rsi, buy_th))

############################
# 7) 技术指标与价格结构
############################
def calc_rsi(close_array, period):
    if close_array is None or len(close_array) < period + 1:
        return None
    deltas = np.diff(close_array)
    seed = deltas[-period:]
    ups = seed.copy()
    downs = seed.copy()
    ups[ups < 0] = 0
    downs[downs > 0] = 0
    downs = abs(downs)
    avg_up = np.mean(ups)
    avg_down = np.mean(downs)
    if avg_down == 0:
        return 100.0
    rs = avg_up / avg_down
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return float(rsi)

def find_support_resistance(close_array, high_array, low_array, period, context):
    if any(arr is None for arr in [close_array, high_array, low_array]):
        return None, None
    if len(close_array) < period or len(high_array) < period or len(low_array) < period:
        return None, None

    current_price = close_array[-1]
    recent_high = high_array[-period:]
    recent_low = low_array[-period:]

    support_candidates, resistance_candidates = [], []
    for i in range(1, len(recent_low) - 1):
        if recent_low[i] < recent_low[i-1] and recent_low[i] < recent_low[i+1]:
            support_candidates.append(recent_low[i])
    for i in range(1, len(recent_high) - 1):
        if recent_high[i] > recent_high[i-1] and recent_high[i] > recent_high[i+1]:
            resistance_candidates.append(recent_high[i])

    nearby_supports = [s for s in support_candidates
                       if s < current_price and (current_price - s) / current_price < context.price_threshold]
    nearby_resistances = [r for r in resistance_candidates
                          if r > current_price and (r - current_price) / current_price < context.price_threshold]

    support = max(nearby_supports) if nearby_supports else None
    resistance = min(nearby_resistances) if nearby_resistances else None
    return support, resistance

def is_price_breaking_resistance(current_price, resistance):
    return (resistance is not None) and (current_price > resistance)

def is_slow_rise_sell_condition(current_price, resistance, rsi, stock_data, context):
    if stock_data.get("entry_price", 0.0) == 0.0:
        return False
    profit_pct = current_price / stock_data["entry_price"] - 1
    return (rsi > context.rsi_sell_base
            and is_price_breaking_resistance(current_price, resistance)
            and profit_pct >= context.slow_rise_pct)

def adjust_rsi_threshold(current_price, support, resistance, context):
    buy_adjust = 0
    sell_adjust = 0

    if support is not None:
        support_proximity = (current_price - support) / current_price
        buy_adjust = int(context.rsi_float_range * (1 - support_proximity / context.price_threshold))

    if resistance is not None and is_price_breaking_resistance(current_price, resistance):
        breakout_strength = (current_price - resistance) / resistance
        sell_adjust = min(int(context.rsi_float_range * min(breakout_strength * 10, 1)), context.rsi_float_range)

    buy_threshold = context.rsi_buy_base + max(buy_adjust, 0)
    sell_threshold = min(context.rsi_sell_base + max(sell_adjust, 0), 95)
    return buy_threshold, sell_threshold

############################
# 8) 日线数据封装（对接处）
############################
def compute_rsi_daily(stock, period):
    closes = get_history(stock, field="close", bars=max(period + 20, 40), freq="1d")  # TODO: 对接PTrade历史
    return calc_rsi(np.array(closes, dtype=float) if closes else None, period)

def compute_sr_daily(stock, context):
    closes = get_history(stock, field="close", bars=context.support_period + 2, freq="1d")   # TODO
    highs  = get_history(stock, field="high",  bars=context.support_period + 2, freq="1d")   # TODO
    lows   = get_history(stock, field="low",   bars=context.support_period + 2, freq="1d")   # TODO
    if not (closes and highs and lows):
        return None, None
    return find_support_resistance(
        np.array(closes, dtype=float),
        np.array(highs, dtype=float),
        np.array(lows, dtype=float),
        context.support_period,
        context
    )

############################
# 9) 下单与账户工具（对接处）
############################
def place_buy_value(context, stock, value):
    # TODO: 替换为PTrade买入函数，例如：order_value(stock, value)
    log.info("模拟买入 {} 金额 {:.2f}".format(stock, value))

def place_sell_all(context, stock):
    # TODO: 替换为PTrade卖出函数，例如：order_target_value(stock, 0)
    log.info("模拟清仓卖出 {}".format(stock))

def get_position_cost(position):
    # 兼容不同数据结构的持仓成本字段
    for k in ("avg_cost", "vwap", "cost_basis"):
        if hasattr(position, k):
            return getattr(position, k)
    return 0.0

def mark_cooling(sd, technical=True):
    days = 1 if technical else 2
    sd["cooling_days_left"] = days
    sd["can_buy"] = False
    sd["hold_position"] = False
    sd["entry_price"] = 0.0
    sd["entry_date"] = None
    sd["holding_days"] = 0

############################
# 10) 辅助：是否尾盘、是否做尾盘买入评估
############################
def is_last_10_minutes(current_dt):
    # TODO: 若平台提供现成函数可直接替换
    market_close = current_dt.replace(hour=15, minute=0, second=0, microsecond=0)
    return (market_close - current_dt).total_seconds() <= 600 and (market_close - current_dt).total_seconds() >= 0

def should_do_tailend_buy_check(current_dt):
    # 尾盘窗口开始后进行一次或多次评估
    return is_last_10_minutes(current_dt)

############################
# 11) 行情占位（务必替换为PTrade实际API）
############################
def get_current_price(stock):
    # TODO: 返回最新价
    return None

def get_current_volume(stock):
    # TODO: 返回分时最新成交量
    return None

def get_history(stock, field, bars, freq="1d"):
    # TODO: 返回历史数据列表，如[float,...]
    return None

def get_trading_day():
    # TODO: 返回当前交易日（date或str）
    return datetime.date.today().isoformat()

def set_universe(stocks):
    # TODO: 对接自选池设置
    pass

def get_position(stock):
    # TODO: 返回持仓对象或None
    return None

class log:
    @staticmethod
    def info(msg): print(msg)
