import numpy as np
import datetime

"""
ETF RSI震荡交易策略：使用特定ETF列表交易
整合了RSI和支撑压力位的交易逻辑，高效版本
"""

def initialize(context):
    # 使用固定ETF列表代替选股逻辑
    context.stock_list = [
        '513360.SS',  # 示例ETF代码
        '159985.SZ',
        '513310.SS',
        '518880.SS',
        '513060.SS',
        '513120.SS',
        '513010.SS',
        '159740.SZ',
        '511380.SS',
        '513330.SS',
        '513130.SS',
        '513090.SS'
    ]
    
    # 设置股票池
    set_universe(context.stock_list)
    log.info("ETF RSI震荡策略初始化完成，交易ETF数: {}".format(len(context.stock_list)))
    
    # 区间震荡参数 - 保留仅作为参考
    context.range_period = 60      # 检查区间震荡的时间周期由30天改为60天
    context.range_ratio_max = 0.15  # 最大波动率略微调小到15%，适应ETF特性
    context.range_ratio_min = 0.05  # 最小波动率降低到5%，适应ETF波动较小的特性
    context.trend_threshold = 0.03  # 趋势强度阈值略微降低到3%，提高震荡ETF识别精度
    
    # 资金控制参数
    context.max_capital = 20000000    # 最大使用资金2000万
    context.max_positions = 5      # 最大持仓ETF数5只
    context.max_capital_per_stock = 20000  # 每只ETF最大资金20000元
    
    # RSI参数 - 为ETF调整参数
    context.rsi_short = 6
    context.rsi_buy_base = 30    # RSI基础买入阈值
    context.rsi_sell_base = 85   # RSI基础卖出阈值
    context.rsi_float_range = 20 # RSI浮动范围
    
    # 支撑压力位和其他参数
    context.support_period = 20
    context.price_threshold = 0.015
    
    # 卖出条件参数 - 调整为适合ETF的参数
    context.intraday_high_pct = 0.02
    context.min_high_pct = 0.015
    context.entry_high_pct = 0.015
    context.vol_surge_ratio = 1.5
    context.slow_rise_pct = 0.03
    context.max_holding_days = 15
    
    # 止损止盈参数 - 调整为适合ETF的参数
    context.stop_loss_pct = 0.015      # 止损1.5%
    context.profit_target_pct = 0.05  # 盈利目标5%
    context.profit_retreat_pct = 0.01 # 回撤阈值1%
    
    # 冷静期参数
    context.cooling_days_technical = 1
    context.cooling_days_profit = 2
    
    # 初始化数据
    context.stock_data = {}
    context.filtered_stocks = []
    
    # 缓存数据
    context.data_cache = {}
    context.last_filter_day = None


def before_trading_start(context, data):
    """使用固定ETF列表代替选股逻辑"""
    current_date = get_trading_day()
    
    # 如果不是新的交易日，使用缓存的ETF列表
    if context.last_filter_day == current_date:
        log.info("使用缓存的ETF列表，共{}只ETF".format(len(context.filtered_stocks)))
        return
    
    # 更新最后筛选日期
    context.last_filter_day = current_date
    
    # 使用固定的ETF列表代替筛选逻辑
    context.filtered_stocks = context.stock_list.copy()
    
    log.info("固定ETF列表数量: {}".format(len(context.filtered_stocks)))
    
    # 处理已持有ETF
    for stock in list(context.stock_data.keys()):
        position = get_position(stock)
        has_position = position is not None and position.amount > 0
        
        if has_position and stock not in context.filtered_stocks:
            context.filtered_stocks.append(stock)
        elif not has_position and stock not in context.filtered_stocks:
            del context.stock_data[stock]
    
    # 更新冷静期状态
    for stock in context.filtered_stocks:
        if stock not in context.stock_data:
            context.stock_data[stock] = {
                'hold_position': False,
                'can_buy': True,
                'entry_price': 0,
                'entry_date': None,
                'today_open': None,
                'today_low': None,
                'intraday_prices': [],
                'intraday_volumes': [],
                'day_high_price': 0,
                'vol_avg': None,
                'max_profit_pct': 0,
                'cooling_days_left': 0
            }
        
        # 检查冷静期
        if context.stock_data[stock]['cooling_days_left'] > 0:
            context.stock_data[stock]['cooling_days_left'] -= 1
            context.stock_data[stock]['can_buy'] = False
        else:
            context.stock_data[stock]['can_buy'] = True
        
        # 重置当日数据
        context.stock_data[stock]['today_open'] = None
        context.stock_data[stock]['today_low'] = None
        context.stock_data[stock]['intraday_prices'] = []
        context.stock_data[stock]['intraday_volumes'] = []
        context.stock_data[stock]['day_high_price'] = 0
        context.stock_data[stock]['vol_avg'] = None


# 判断是否在尾盘10分钟
def is_last_10_minutes(current_dt):
    hour = current_dt.hour
    minute = current_dt.minute
    return (hour == 14 and minute >= 50) or (hour == 15 and minute == 0)


# 快速计算RSI指标 - 使用numpy向量化运算
def calc_rsi(close_array, period):
    if len(close_array) < period + 1:
        return None
    
    # 使用numpy快速计算差值和增减
    deltas = np.diff(close_array)
    seed = deltas[-period:]
    
    # 使用numpy快速计算上涨和下跌
    ups = seed.copy()
    downs = seed.copy()
    ups[ups < 0] = 0
    downs[downs > 0] = 0
    downs = abs(downs)
    
    # 快速计算平均值
    avg_up = np.mean(ups)
    avg_down = np.mean(downs)
    
    if avg_down == 0:
        return 100
    
    rs = avg_up / avg_down
    rsi = 100 - (100 / (1 + rs))
    
    return rsi


# 优化版本：快速寻找支撑位和压力位
def find_support_resistance(close_array, high_array, low_array, period, context):
    if len(close_array) < period:
        return None, None
    
    # 快速找局部最高点和最低点
    current_price = close_array[-1]
    
    # 使用窗口滑动法快速找局部最低点
    support_candidates = []
    resistance_candidates = []
    
    # 只考虑最近的点位
    recent_high = high_array[-period:]
    recent_low = low_array[-period:]
    
    # 简化算法：直接找出最近的局部最低和最高价格点
    for i in range(1, len(recent_low)-1):
        if recent_low[i] < recent_low[i-1] and recent_low[i] < recent_low[i+1]:
            support_candidates.append(recent_low[i])
    
    for i in range(1, len(recent_high)-1):
        if recent_high[i] > recent_high[i-1] and recent_high[i] > recent_high[i+1]:
            resistance_candidates.append(recent_high[i])
    
    # 快速筛选接近当前价格的支撑位和压力位
    nearby_supports = [s for s in support_candidates if s < current_price and (current_price - s) / current_price < context.price_threshold]
    nearby_resistances = [r for r in resistance_candidates if r > current_price and (r - current_price) / current_price < context.price_threshold]
    
    # 如果有多个，选择最接近的
    support = max(nearby_supports) if nearby_supports else None
    resistance = min(nearby_resistances) if nearby_resistances else None
    
    return support, resistance


# 检查价格是否突破压力位
def is_price_breaking_resistance(current_price, resistance):
    return resistance is not None and current_price > resistance


# 优化版本：检查价格是否处于"盘中冲高"状态
def is_intraday_price_surging(current_price, stock_data, context):
    if len(stock_data['intraday_prices']) < 30:
        return False
    
    # 更新当日最低价和最高价
    if stock_data['today_low'] is None or current_price < stock_data['today_low']:
        stock_data['today_low'] = current_price
    
    if current_price > stock_data['day_high_price']:
        stock_data['day_high_price'] = current_price
    
    # 快速计算涨幅
    open_surge_pct = 0 if stock_data['today_open'] is None else (current_price / stock_data['today_open'] - 1)
    low_surge_pct = 0 if stock_data['today_low'] is None else (current_price / stock_data['today_low'] - 1)
    entry_surge_pct = 0 if stock_data['entry_price'] == 0 else (current_price / stock_data['entry_price'] - 1)
    
    # 条件检查
    condition1 = open_surge_pct >= context.intraday_high_pct
    condition2 = low_surge_pct >= context.min_high_pct
    condition3 = entry_surge_pct >= context.entry_high_pct
    
    # 成交量检查
    condition4 = False
    if len(stock_data['intraday_volumes']) > 20:
        avg_volume = np.mean(stock_data['intraday_volumes'][-20:-1])
        current_vol = stock_data['intraday_volumes'][-1]
        condition4 = current_vol > avg_volume * context.vol_surge_ratio
    
    # 满足至少3个条件
    return sum([condition1, condition2, condition3, condition4]) >= 3


# 优化版本：检查是否满足"缓慢涨幅"卖出条件
def is_slow_rise_sell_condition(current_price, resistance, rsi, stock_data, context):
    if stock_data['entry_price'] == 0:
        return False
    
    profit_pct = (current_price / stock_data['entry_price'] - 1)
    
    # 快速条件检查
    return (rsi > context.rsi_sell_base and 
            is_price_breaking_resistance(current_price, resistance) and 
            profit_pct >= context.slow_rise_pct)


# 获取持仓成本的安全方法
def get_position_cost(position):
    if hasattr(position, 'avg_cost'):
        return position.avg_cost
    if hasattr(position, 'vwap'):
        return position.vwap
    if hasattr(position, 'cost_basis'):
        return position.cost_basis
    
    return 0


# 优化版本：调整RSI阈值
def adjust_rsi_threshold(current_price, support, resistance, context):
    # 快速计算调整值
    buy_adjust = 0
    sell_adjust = 0
    
    if support is not None:
        support_proximity = (current_price - support) / current_price
        buy_adjust = int(context.rsi_float_range * (1 - support_proximity / context.price_threshold))
    
    if is_price_breaking_resistance(current_price, resistance) and resistance is not None:
        breakout_strength = (current_price - resistance) / resistance
        sell_adjust = min(int(context.rsi_float_range * min(breakout_strength * 10, 1)), context.rsi_float_range)
    
    # 快速计算阈值
    buy_threshold = context.rsi_buy_base + buy_adjust
    sell_threshold = context.rsi_sell_base + sell_adjust
    sell_threshold = min(sell_threshold, 95)
    
    return buy_threshold, sell_threshold


# 优化后的每日收盘后处理函数
def after_trading_end(context, data):
    for stock in context.filtered_stocks:
        # 直接设置看多信号
        if stock in context.stock_data:
            context.stock_data[stock]['market_signal'] = 'bullish'


# 优化的主要处理函数
def handle_data(context, data):
    current_time = context.current_dt
    is_tailend = is_last_10_minutes(current_time)
    
    # 仅在尾盘时记录ETF池状态
    if is_tailend and current_time.minute == 50 and current_time.second < 10:
        log.info("今日考虑交易的ETF数量: {}".format(len(context.filtered_stocks)))
        log.info("账户总资产: {:.2f}万, 可用资金: {:.2f}万".format(
            context.portfolio.total_value/10000, context.portfolio.cash/10000))
    
    # 获取当前持仓数量
    positions = context.portfolio.positions
    held_positions = len([p for p in positions.values() if p.amount > 0])
    
    # 如果已达到最大持仓，并且不在尾盘时间，跳过买入检查
    if held_positions >= context.max_positions and not is_tailend:
        return
    
    # 优化：只在尾盘检查买入
    if is_tailend:
        # 获取待检查的未持仓ETF
        check_stocks = [s for s in context.filtered_stocks 
                        if s not in positions or positions[s].amount == 0]
        
        # 分批处理ETF
        for stock in check_stocks:
            try:
                # 获取当前价格
                price_data = None
                if stock in data:
                    current_price = data[stock].close
                else:
                    # 使用历史数据获取价格
                    price_data = get_history(1, '1d', 'close', stock)
                    if price_data is None or price_data.empty:
                        continue
                    current_price = price_data.iloc[-1]['close']
                
                # 获取交易状态数据
                if stock not in context.stock_data:
                    continue
                stock_data = context.stock_data[stock]
                
                # 检查是否可买入
                if not stock_data['can_buy']:
                    continue
                
                # 获取历史数据计算指标
                hist_data = get_history(60, '1d', field=['close', 'high', 'low'], security_list=stock)
                if hist_data is None or hist_data.empty:
                    continue
                
                close_data = hist_data['close'].values  # 转为numpy数组加速计算
                high_data = hist_data['high'].values
                low_data = hist_data['low'].values
                
                # 计算RSI
                rsi6 = calc_rsi(close_data, context.rsi_short)
                if rsi6 is None:
                    continue
                
                # 寻找支撑位和压力位
                support, resistance = find_support_resistance(close_data, high_data, low_data, context.support_period, context)
                
                # 调整RSI阈值
                rsi_buy_threshold, _ = adjust_rsi_threshold(current_price, support, resistance, context)
                
                # 检查RSI买入条件
                if rsi6 < rsi_buy_threshold:
                    # 检查持仓上限
                    if held_positions >= context.max_positions:
                        continue
                        
                    # 计算可用资金
                    cash = context.portfolio.cash
                    available_cash = min(cash, context.max_capital_per_stock)
                    
                    if available_cash < 2000:
                        continue
                    
                    # 计算买入数量
                    buy_amount = int(available_cash / current_price / 100) * 100
                    
                    if buy_amount >= 100:
                        log.info("买入 {} - RSI={:.2f}<{:.2f}, 价格={:.2f}, 数量={}份".format(
                            stock, rsi6, rsi_buy_threshold, current_price, buy_amount))
                        
                        # 执行买入
                        order(stock, buy_amount)
                        
                        # 更新交易状态
                        stock_data['hold_position'] = True
                        stock_data['entry_price'] = current_price
                        stock_data['entry_date'] = current_time.strftime('%Y-%m-%d')
                        
                        # 更新持仓计数
                        held_positions += 1
            except Exception as e:
                continue
    
    # 处理已持仓的ETF - 检查卖出条件
    held_stocks = [stock for stock, position in positions.items() if position.amount > 0]
    
    # 处理持仓ETF
    for stock in held_stocks:
        try:
            # 获取价格
            if stock in data:
                current_price = data[stock].close
                current_volume = data[stock].volume if hasattr(data[stock], 'volume') else 0
            else:
                # 使用历史数据
                price_data = get_history(1, '1d', ['close', 'volume'], stock)
                if price_data is None or price_data.empty:
                    continue
                current_price = price_data['close'].iloc[-1]
                current_volume = price_data['volume'].iloc[-1] if 'volume' in price_data else 0
            
            # 获取持仓信息
            position = positions[stock]
            current_amount = position.amount
            
            # 获取交易状态数据
            if stock not in context.stock_data:
                continue
            stock_data = context.stock_data[stock]
            
            # 更新价格数据
            stock_data['intraday_prices'].append(current_price)
            if current_volume > 0:
                stock_data['intraday_volumes'].append(current_volume)
            
            # 更新开盘价
            if stock_data['today_open'] is None and stock in data and hasattr(data[stock], 'open'):
                stock_data['today_open'] = data[stock].open
            
            # 检查持仓成本和持仓天数
            entry_price = stock_data['entry_price']
            entry_date = stock_data['entry_date']
            
            # 如果没有保存入场价，使用持仓的平均成本
            if entry_price == 0:
                entry_price = get_position_cost(position)
                stock_data['entry_price'] = entry_price
            
            # 如果没有保存入场日期，使用当前日期
            if not entry_date:
                entry_date = current_time.strftime('%Y-%m-%d')
                stock_data['entry_date'] = entry_date
            
            # 计算持仓天数和盈亏
            holding_days = 0
            if entry_date:
                entry_date_obj = datetime.datetime.strptime(entry_date, '%Y-%m-%d')
                holding_days = (current_time.date() - entry_date_obj.date()).days
            
            profit_pct = (current_price / entry_price - 1) * 100
            
            # 更新最大盈利
            if profit_pct > stock_data['max_profit_pct']:
                stock_data['max_profit_pct'] = profit_pct
            
            # 获取历史数据计算技术指标
            hist_data = get_history(60, '1d', field=['close', 'high', 'low'], security_list=stock)
            if hist_data is None or hist_data.empty:
                continue
            
            close_data = hist_data['close'].values
            high_data = hist_data['high'].values
            low_data = hist_data['low'].values
            
            # 计算RSI
            rsi6 = calc_rsi(close_data, context.rsi_short)
            if rsi6 is None:
                continue
            
            # 寻找支撑位和压力位
            support, resistance = find_support_resistance(close_data, high_data, low_data, context.support_period, context)
            
            # 调整RSI阈值
            _, rsi_sell_threshold = adjust_rsi_threshold(current_price, support, resistance, context)
            
            # 检查价格是否处于盘中冲高状态
            is_price_surging = False
            if len(stock_data['intraday_prices']) >= 30:
                is_price_surging = is_intraday_price_surging(current_price, stock_data, context)
            
            # 检查是否满足缓慢涨幅卖出条件
            is_slow_rise = is_slow_rise_sell_condition(current_price, resistance, rsi6, stock_data, context)
            
            # 检查卖出条件
            sell_signal = False
            sell_type = None
            
            # 卖出条件1：技术指标
            if is_price_surging or rsi6 > rsi_sell_threshold or is_slow_rise:
                sell_signal = True
                sell_type = "technical"
            
            # 卖出条件2：止损 - 1.5%止损
            elif profit_pct < -context.stop_loss_pct * 100:
                sell_signal = True
                sell_type = "stop_loss"
            
            # 卖出条件3：止盈回撤 - 5%盈利后回撤1%
            elif (stock_data['max_profit_pct'] >= context.profit_target_pct * 100 and 
                 (stock_data['max_profit_pct'] - profit_pct) >= context.profit_retreat_pct * 100):
                sell_signal = True
                sell_type = "profit_taking"
            
            # 卖出条件4：持仓超时
            elif holding_days >= context.max_holding_days:
                sell_signal = True
                
                sell_type = "timeout"
            
            # 执行卖出
            if sell_signal:
                # 记录卖出原因
                sell_reasons = {
                    "technical": "技术指标",
                    "stop_loss": "止损",
                    "profit_taking": "止盈回撤",
                    "timeout": "持仓时间超限"
                }
                
                log.info("卖出 {} - 原因: {}, 买价: {:.2f}, 卖价: {:.2f}, 数量: {}, 盈亏: {:.2f}%, 持仓天数: {}".format(
                    stock, sell_reasons.get(sell_type, "未知"), 
                    entry_price, current_price, current_amount,
                    profit_pct, holding_days))
                
                # 执行卖出
                order(stock, -current_amount)
                
                # 设置冷静期
                if sell_type == "profit_taking":
                    stock_data['cooling_days_left'] = context.cooling_days_profit
                else:
                    stock_data['cooling_days_left'] = context.cooling_days_technical
                
                # 重置交易状态
                stock_data['hold_position'] = False
                stock_data['can_buy'] = False
                stock_data['entry_price'] = 0
                stock_data['max_profit_pct'] = 0
        except:
            continue
