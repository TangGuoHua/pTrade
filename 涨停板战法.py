import pandas as pd 
import numpy as np
import talib
import sxsc_tushare as sx
from datetime import datetime, timedelta
import random

sx.set_token("")
pro = sx.get_api(env="prd")

'''
策略名称：
日级别多指标策略（动态股票池版本）
策略流程：
1.每日动态获取符合条件的涨跌停股票作为股票池；
2.每日固定时间执行，多指标交易；
3.不同股票可根据不同资金买入；
4.可设置止盈止损；
5.新增市场环境判断条件：两连板及以上股票总数<=5，或最高板<=4，或涨停总数<20时，停止买入
6.新增规则：必须按连板数由大到小的顺序，依次买入股票，连板越多越优先买入
7.将每个股票最大持仓比例（原硬编码35%）改为可配置参数
Author: 高顿量化
Version: 2.0.6
Date: 2024/06/15
修改说明：
1. 修复卖出逻辑：改为遍历所有持仓股票，而不是只遍历股票池中的股票
2. 新增持仓天数限制：买入后超过10个交易日强制卖出
3. 修复利润保护机制的逻辑错误
4. 修复当日数据获取逻辑，确保能正确获取当日价格数据
5. 优化卖出条件判断，避免长期持仓不卖出
6. 拆分买入和卖出时间：买入在9:30和9:35，卖出在14:30
7. 新增市场环境判断条件：两连板及以上股票总数<=5，或最高板<=4，或涨停总数<20时，停止买入
8. 新增规则：必须按连板数由大到小的顺序，依次买入股票，连板越多越优先买入
9. 将每个股票最大持仓比例（原硬编码35%）改为可配置参数
'''

def initialize(context):
    # 股票池配置：初始为空，将在每天开盘前动态更新
    g.sec_dict = {}
    
    # 用户可修改参数
    g.config = {
        'buy_time1': '09:30',   # 第一次买入时间
        'buy_time2': '09:35',   # 第二次买入时间  
        'sell_time': '14:30',   # 卖出时间
        'SL': -0.05,            # 止损参数：-0.05表示-5%止损卖出
        'TP': 0.5,              # 止盈参数：1.0表示100%止盈卖出
        "MA": [5, 10, 20, 30, 60, 120, 250, 1],  # 多均线参数
        "KDJ": [9, 3, 3, 1],    # KDJ参数
        "RSI": [6, 12, 24, 1],  # RSI参数
        "CCI": [14, 1],         # CCI参数
        "DXJC": [8, 1],         # 短线决策指标
        "MACD": [12, 26, 9, 1], # MACD参数
        "BBIBOLL": [11, 6, 1],  # 布林带参数
        "WR": [10, 6, 1],       # WR参数
        "DKDD": [1],            # 多空底顶指标
        "JKGP": [9, 2, 5, 1],   # 机构控盘指标
        'stock_pool_min_amount': 10000,  # 每只股票最小买入金额
        'stock_pool_max_amount': 200000, # 每只股票最大买入金额
        'stock_pool_count': 10,          # 股票池最大股票数量
        'initial_capital': 100000,       # 初始资金
        'max_holding_days': 10,          # 最大持仓天数
        'market_condition_enabled': True, # 是否启用市场环境判断
        'min_two_limit_stocks': 4,       # 最小两连板股票数量
        'min_highest_limit': 4,          # 最小最高板数
        'min_limit_stocks': 10,          # 最小涨停股票数量
        'enable_limit_order': True,      # 是否启用按连板数顺序买入
        'max_position_per_stock': 0.4   # 每个股票最大持仓比例（35%）
    }
    # 添加最高价记录字典
    g.highest_prices = {}  # 记录持仓股票的最高价
    # 添加买入日期记录字典
    g.buy_dates = {}  # 记录持仓股票的买入日期
    # 添加历史数据缓存
    g.data_cache = {}
    # 添加市场环境状态
    g.market_condition_ok = True  # 市场环境是否满足买入条件
    g.market_stats = {}  # 市场统计数据
    # 添加连板数信息字典
    g.limit_times_dict = {}  # 记录每只股票的连板数
    
    # 设置初始资金
    context.portfolio.starting_cash = g.config['initial_capital']
    context.portfolio.cash = g.config['initial_capital']
    
    # 设置多个触发时间：9:30和9:35进行买入，14:30进行卖出
    # run_daily(context, buy_event, time=g.config['buy_time1'])
    # run_daily(context, buy_event, time=g.config['buy_time2'])
    # run_daily(context, sell_event, time=g.config['sell_time'])
    
def before_trading_start(context, data):
    # 更新股票池
    update_stock_pool(context)
    
    # 从股票池字典获取股票列表
    g.sec_list = list(g.sec_dict.keys())
    
    # 获取昨日持仓股票
    positions = get_positions()
    yst_hold = [v.sid for s, v in positions.items()] if positions else []
    
    # 今日不在股票池的持仓股票（需要按照卖出条件判断）
    g.sell_list = list(set(yst_hold) - set(g.sec_list))
    
    log.info('=' * 60)
    log.info('日级别多指标策略 (动态股票池版本 v2.0.6)')
    log.info('=' * 60)
    log.info('策略配置:')
    log.info(f'买入时间1: {g.config["buy_time1"]}')
    log.info(f'买入时间2: {g.config["buy_time2"]}')
    log.info(f'卖出时间: {g.config["sell_time"]}')
    log.info(f'止损比例: {g.config["SL"]*100:.1f}%')
    log.info(f'止盈比例: {g.config["TP"]*100:.0f}%')
    log.info(f'最大持仓天数: {g.config["max_holding_days"]}天')
    log.info(f'每个股票最大持仓比例: {g.config["max_position_per_stock"]*100:.0f}%')
    log.info(f'启用市场环境判断: {g.config["market_condition_enabled"]}')
    log.info(f'按连板数顺序买入: {g.config["enable_limit_order"]}')
    log.info('')
    
    # 输出市场环境判断条件
    if g.config['market_condition_enabled']:
        log.info('市场环境买入条件(以下条件需全部满足):')
        log.info(f'1. 两连板及以上股票数量 > {g.config["min_two_limit_stocks"]}')
        log.info(f'2. 最高板数 > {g.config["min_highest_limit"]}')
        log.info(f'3. 涨停股票总数 >= {g.config["min_limit_stocks"]}')
        log.info('')
    
    log.info('股票池筛选条件:')
    log.info('1. N大于3 (涨停统计: N/T T天有N次涨停)')
    log.info('2. limit_times大于等于1 (连板数)')
    log.info('')
    
    # 统计启用的指标
    strtInfo = []
    for strt in ["MA", "KDJ", "RSI", "CCI", "DXJC", "MACD", "BBIBOLL", "WR", "DKDD", "JKGP"]:
        if g.config[strt][-1]:
            strtInfo.append(strt)
    log.info("启用的技术指标: {}".format(strtInfo))
    log.info('')
    
    log.info('买卖条件:')
    log.info('买入条件: 至少7个指标满足买入条件，且机构控盘、布林带、RSI、MACD必须都满足')
    log.info('卖出条件: 至少3个指标满足卖出条件 或 满足买入的指标不足6个 或 止损止盈 或 新增卖出条件 或 持仓超过{}天'.format(g.config['max_holding_days']))
    log.info('')
    
    # 输出市场环境判断结果
    if g.config['market_condition_enabled']:
        if g.market_condition_ok:
            log.info('✅ 市场环境判断: 满足买入条件')
        else:
            log.warning('❌ 市场环境判断: 不满足买入条件，今日停止买入操作')
        log.info('')
    
    if len(g.sec_list) > 0:
        # 按连板数排序
        sorted_stocks = sorted(g.sec_list, key=lambda x: g.limit_times_dict.get(x, 0), reverse=True)
        
        log.info('今日股票池 (按连板数排序，共{}只):'.format(len(g.sec_list)))
        for i, sec in enumerate(sorted_stocks, 1):
            try:
                sec_name = get_security_info(sec).display_name if get_security_info(sec) else sec
            except:
                sec_name = sec
            buy_amount = g.sec_dict.get(sec, g.config['stock_pool_min_amount'])
            limit_times = g.limit_times_dict.get(sec, 0)
            log.info(f'{i:2d}. {sec} {sec_name} - 连板数: {limit_times}板 - 计划买入金额: ¥{buy_amount:,.0f}')
    else:
        if g.config['market_condition_enabled'] and not g.market_condition_ok:
            log.warning('今日股票池为空：市场环境不满足买入条件')
        else:
            log.warning('今日股票池为空：没有符合条件的股票！')
    log.info('')
    
def update_stock_pool(context):
    """更新股票池：根据涨跌停数据筛选符合条件的股票"""
    try:
        # 获取前一个交易日的日期
        current_date = context.current_dt
        if hasattr(context, 'previous_date'):
            # PTrade可能提供前一个交易日
            trade_date = context.previous_date.strftime('%Y%m%d')
        else:
            # 如果PTrade没有提供，使用当前日期减一天
            trade_date = (current_date - timedelta(days=1)).strftime('%Y%m%d')
        
        log.info(f"获取{trade_date}的涨跌停数据...")
        
        # 获取涨跌停数据
        df = pro.limit_list_d(
            trade_date=trade_date, 
            limit_type='U',  # 涨停
            fields='ts_code,trade_date,industry,name,close,pct_chg,open_times,up_stat,limit_times'
        )
        
        if df is None or len(df) == 0:
            log.warning(f"未获取到{trade_date}的涨停数据")
            g.sec_dict = {}
            g.limit_times_dict = {}
            g.market_condition_ok = False
            g.market_stats = {
                'two_limit_stocks': 0,
                'highest_limit': 0,
                'total_limit_stocks': 0
            }
            return
        
        log.info(f"获取到{len(df)}只涨停股票")
        
        # 计算市场环境统计数据
        two_limit_stocks = len(df[df['limit_times'] >= 2])  # 两连板及以上股票数量
        highest_limit = df['limit_times'].max() if len(df) > 0 else 0  # 最高板数
        total_limit_stocks = len(df)  # 涨停股票总数
        
        # 存储市场统计数据
        g.market_stats = {
            'two_limit_stocks': two_limit_stocks,
            'highest_limit': highest_limit,
            'total_limit_stocks': total_limit_stocks
        }
        
        log.info(f"市场环境统计:")
        log.info(f"  两连板及以上股票数量: {two_limit_stocks}")
        log.info(f"  最高板数: {highest_limit}")
        log.info(f"  涨停股票总数: {total_limit_stocks}")
        
        # 判断市场环境是否满足买入条件
        if g.config['market_condition_enabled']:
            condition1 = two_limit_stocks > g.config['min_two_limit_stocks']  # 两连板及以上股票数量>5
            condition2 = highest_limit > g.config['min_highest_limit']  # 最高板数>4
            condition3 = total_limit_stocks >= g.config['min_limit_stocks']  # 涨停股票总数>=20
            
            g.market_condition_ok = condition1 and condition2 and condition3
            
            if not g.market_condition_ok:
                log.warning("市场环境不满足买入条件:")
                if not condition1:
                    log.warning(f"  - 两连板及以上股票数量({two_limit_stocks}) <= {g.config['min_two_limit_stocks']}")
                if not condition2:
                    log.warning(f"  - 最高板数({highest_limit}) <= {g.config['min_highest_limit']}")
                if not condition3:
                    log.warning(f"  - 涨停股票总数({total_limit_stocks}) < {g.config['min_limit_stocks']}")
                
                # 市场环境不满足，清空股票池
                g.sec_dict = {}
                g.limit_times_dict = {}
                return
        
        # 筛选符合条件的股票
        qualified_stocks = []
        
        for _, row in df.iterrows():
            ts_code = row['ts_code']
            up_stat = str(row['up_stat'])
            limit_times = int(row['limit_times'])
            
            # 解析up_stat：格式为"N/T"
            try:
                if '/' in up_stat:
                    N_str, _ = up_stat.split('/')
                    N = int(N_str)
                else:
                    N = int(up_stat)
            except:
                N = 0
            
            # 筛选条件：N大于等于3 且 limit_times大于等于1
            if N >= 3 and limit_times >= 1:
                # 计算买入金额（可以根据市值或其他因素调整，这里使用固定范围随机分配）
                buy_amount = random.randint(
                    g.config['stock_pool_min_amount'], 
                    g.config['stock_pool_max_amount']
                )
                
                # 确保金额是100的整数倍（方便计算股数）
                buy_amount = (buy_amount // 100) * 100
                
                qualified_stocks.append((ts_code, buy_amount, limit_times))
                log.info(f"符合条件: {ts_code} {row['name']} - N={N}, 连板数={limit_times}, 计划买入金额={buy_amount:,.0f}")
        
        log.info(f"共筛选出{len(qualified_stocks)}只符合条件的股票")
        
        # 如果符合条件的股票太多，随机选择一部分（最多stock_pool_count只）
        if len(qualified_stocks) > g.config['stock_pool_count']:
            # 先按连板数排序，保留连板数多的股票
            qualified_stocks.sort(key=lambda x: x[2], reverse=True)
            # 取前stock_pool_count只股票
            qualified_stocks = qualified_stocks[:g.config['stock_pool_count']]
            log.info(f"选择连板数最高的{len(qualified_stocks)}只股票进入股票池")
        
        # 更新股票池字典和连板数字典
        g.sec_dict = {code: amount for code, amount, _ in qualified_stocks}
        g.limit_times_dict = {code: limit_times for code, _, limit_times in qualified_stocks}
        
        if len(g.sec_dict) == 0 and g.market_condition_ok:
            log.warning("市场环境满足条件，但没有符合条件的股票，今日股票池为空")
        
    except Exception as e:
        log.error(f"更新股票池时出错: {e}")
        # 出错时清空股票池
        g.sec_dict = {}
        g.limit_times_dict = {}
        g.market_condition_ok = False

def buy_event(context):
    """买入事件处理函数，在9:30和9:35执行"""
    current_time = context.current_dt.time()
    log.info(f'=== 买入事件触发，当前时间: {context.blotter.current_dt.strftime("%Y-%m-%d %H:%M:%S")} ===')
    
    # 获取账户总资产
    total_value = context.portfolio.total_value
    log.info(f'当前总资产: ¥{total_value:,.2f}')
    log.info(f'可用现金: ¥{context.portfolio.cash:,.2f}')
    log.info('')
    
    # 检查市场环境条件
    if g.config['market_condition_enabled'] and not g.market_condition_ok:
        log.info("市场环境不满足买入条件，停止买入操作")
        log.info(f"市场环境统计: 两连板及以上={g.market_stats.get('two_limit_stocks', 0)}个, "
                f"最高板={g.market_stats.get('highest_limit', 0)}, "
                f"涨停总数={g.market_stats.get('total_limit_stocks', 0)}个")
        return
    
    # 如果股票池为空，直接返回
    if len(g.sec_list) == 0:
        log.info("今日股票池为空，不进行买入操作")
        return
    
    # 按连板数排序股票列表
    if g.config['enable_limit_order']:
        # 按连板数由大到小排序
        sorted_sec_list = sorted(g.sec_list, key=lambda x: g.limit_times_dict.get(x, 0), reverse=True)
        log.info(f"按连板数排序买入顺序（连板数最多者优先）:")
        for i, sec in enumerate(sorted_sec_list, 1):
            limit_times = g.limit_times_dict.get(sec, 0)
            try:
                sec_name = get_security_info(sec).display_name if get_security_info(sec) else sec
            except:
                sec_name = sec
            log.info(f"{i:2d}. {sec} {sec_name} - {limit_times}板")
    else:
        # 如果不启用连板数排序，保持原有顺序
        sorted_sec_list = g.sec_list
    
    log.info('')
    
    # 处理买入逻辑
    for sec in sorted_sec_list:
        try:
            posInfo = get_position(sec)
            if posInfo is None:
                # 如果没有持仓信息，初始化一个空持仓
                class EmptyPosition:
                    def __init__(self):
                        self.amount = 0
                        self.enable_amount = 0
                        self.cost_basis = 0
                        self.last_sale_price = 0
                
                posInfo = EmptyPosition()
            
            # 只有没有持仓时才考虑买入
            if posInfo.amount == 0:
                # 获取股票信号
                buy_signals_count, sell_signals_count, buy_signals = calculate_indicators(sec)
                
                # 检查机构控盘、布林带、RSI、MACD是否都满足买入条件
                # 注意：即使指标未启用，我们也认为它不满足买入条件
                jkgp_satisfied = buy_signals.get('JKGP', False) if g.config['JKGP'][-1] else False
                boll_satisfied = buy_signals.get('BBIBOLL', False) if g.config['BBIBOLL'][-1] else False
                rsi_satisfied = buy_signals.get('RSI', False) if g.config['RSI'][-1] else False
                macd_satisfied = buy_signals.get('MACD', False) if g.config['MACD'][-1] else False
                
                key_indicators_satisfied = jkgp_satisfied and boll_satisfied and rsi_satisfied and macd_satisfied
                
                # 买入条件：至少7个指标满足买入条件，并且机构控盘、布林带、RSI、MACD必须都满足
                if buy_signals_count >= 7 and key_indicators_satisfied:
                    # 计算可买入金额：不超过总资金的max_position_per_stock比例，且不超过该股票的计划买入金额
                    max_per_stock = total_value * g.config['max_position_per_stock']  # 每个股票最大持仓不超过总资产的指定比例
                    planned_amount = g.sec_dict.get(sec, g.config['stock_pool_min_amount'])  # 获取该股票的计划买入金额
                    
                    # 实际买入金额取三者最小值：可用现金、最大单股持仓、计划买入金额
                    buy_amount = min(context.portfolio.cash, max_per_stock, planned_amount)
                    
                    if buy_amount > 0 and posInfo.last_sale_price > 0:
                        # 计算买入数量（取整百）
                        vol = buy_amount // posInfo.last_sale_price // 100 * 100
                        
                        if vol > 0:
                            order(sec, vol)
                            # 买入后初始化最高价记录和买入日期
                            g.highest_prices[sec] = posInfo.last_sale_price
                            g.buy_dates[sec] = context.current_dt.date()
                            
                            try:
                                sec_name = get_security_info(sec).display_name if get_security_info(sec) else sec
                            except:
                                sec_name = sec
                            limit_times = g.limit_times_dict.get(sec, 0)
                            log.info(f"买入 {sec} {sec_name} ({limit_times}板): 买入信号({buy_signals_count}/10), 关键指标:机构控盘{'√' if jkgp_satisfied else '×'} 布林带{'√' if boll_satisfied else '×'} RSI{'√' if rsi_satisfied else '×'} MACD{'√' if macd_satisfied else '×'}, 数量{vol}股, 金额¥{buy_amount:,.0f}")
                    elif buy_amount <= 0:
                        log.debug(f"跳过买入 {sec}: 可用资金不足")
                    else:
                        log.debug(f"跳过买入 {sec}: 价格异常")
                else:
                    # 记录不满足买入条件的原因
                    limit_times = g.limit_times_dict.get(sec, 0)
                    if buy_signals_count < 7:
                        log.debug(f"跳过买入 {sec} ({limit_times}板): 买入信号不足({buy_signals_count}/7)")
                    if not key_indicators_satisfied:
                        log.debug(f"跳过买入 {sec} ({limit_times}板): 关键指标不满足(机构控盘:{'√' if jkgp_satisfied else '×'}, 布林带:{'√' if boll_satisfied else '×'}, RSI:{'√' if rsi_satisfied else '×'}, MACD:{'√' if macd_satisfied else '×'})")
        except Exception as e:
            log.error(f"处理买入逻辑时出错 {sec}: {e}")

def sell_event(context):
    """卖出事件处理函数，在14:30执行"""
    current_time = context.current_dt.time()
    log.info(f'=== 卖出事件触发，当前时间: {context.blotter.current_dt.strftime("%Y-%m-%d %H:%M:%S")} ===')
    
    # 获取账户总资产
    total_value = context.portfolio.total_value
    log.info(f'当前总资产: ¥{total_value:,.2f}')
    log.info(f'可用现金: ¥{context.portfolio.cash:,.2f}')
    log.info('')
    
    # 获取所有持仓股票
    positions = get_positions()
    all_hold_positions = []
    if positions:
        for sid, pos in positions.items():
            if pos is not None and pos.enable_amount > 0:
                all_hold_positions.append(sid)
    
    log.info(f"当前持仓股票数量: {len(all_hold_positions)}")
    
    # 如果没有任何持仓，直接返回
    if len(all_hold_positions) == 0:
        log.info("当前没有持仓，无需卖出")
        return
    
    # 处理所有持仓股票的卖出逻辑
    for sec in all_hold_positions:
        try:
            posInfo = get_position(sec)
            if posInfo is None or posInfo.enable_amount <= 0:
                continue
                
            # 计算持仓收益率
            if posInfo.cost_basis != 0 and posInfo.cost_basis is not None:
                holdRet = posInfo.last_sale_price / posInfo.cost_basis - 1
            else:
                holdRet = 0
            
            # 更新最高价记录
            if sec not in g.highest_prices:
                g.highest_prices[sec] = posInfo.last_sale_price
            else:
                g.highest_prices[sec] = max(g.highest_prices[sec], posInfo.last_sale_price)
            
            highest_price = g.highest_prices[sec]
            
            # 获取当日价格数据用于新卖出条件的判断
            try:
                # 获取当日实时数据 - 使用正确的API
                today_data = get_price(sec, end_date=context.current_dt, frequency='1d', fields=['open', 'high', 'low', 'close', 'volume'], count=1)
                if today_data is not None and len(today_data) > 0:
                    today_open = today_data['open'].iloc[0] if hasattr(today_data['open'], 'iloc') else today_data['open'][0]
                    today_high = today_data['high'].iloc[0] if hasattr(today_data['high'], 'iloc') else today_data['high'][0]
                    today_low = today_data['low'].iloc[0] if hasattr(today_data['low'], 'iloc') else today_data['low'][0]
                    today_close = today_data['close'].iloc[0] if hasattr(today_data['close'], 'iloc') else today_data['close'][0]
                    
                    # 获取昨日收盘价
                    yest_data = get_price(sec, end_date=context.current_dt - timedelta(days=1), frequency='1d', fields=['close'], count=1)
                    if yest_data is not None and len(yest_data) > 0:
                        yest_close = yest_data['close'].iloc[0] if hasattr(yest_data['close'], 'iloc') else yest_data['close'][0]
                    else:
                        yest_close = today_close
                    
                    # 计算当日涨跌幅
                    if yest_close != 0:
                        today_change = (today_close - yest_close) / yest_close
                    else:
                        today_change = 0
                    
                    # 计算当日是否为阴线
                    is_negative_line = today_close < today_open
                    
                    # 判断是否为涨停
                    limit_up_price = yest_close * 1.1  # 假设涨停为10%
                    is_limit_up = today_high >= limit_up_price * 0.999  # 考虑浮点误差
                    
                    # 计算回落幅度（如果涨停）
                    fall_back = 0
                    if is_limit_up:
                        fall_back = (limit_up_price - today_close) / limit_up_price
                else:
                    # 如果无法获取当日数据，使用持仓价格作为近似
                    today_close = posInfo.last_sale_price
                    yest_close = posInfo.cost_basis if posInfo.cost_basis > 0 else today_close * 0.95
                    today_change = (today_close - yest_close) / yest_close if yest_close != 0 else 0
                    is_negative_line = False
                    is_limit_up = False
                    fall_back = 0
            except Exception as e:
                log.warning(f"获取{sec}当日数据失败: {e}")
                today_change = 0
                is_negative_line = False
                is_limit_up = False
                fall_back = 0
                
            # 获取股票信号
            buy_signals_count, sell_signals_count, buy_signals = calculate_indicators(sec)
            
            # 新增卖出条件判断（14:30后判断）
            new_sell_conditions = []
            
            # 条件1: 当天跌幅超过5%
            if today_change <= -0.05:
                new_sell_conditions.append(f"当日跌幅超过5%({today_change:.2%})")
            
            # 注意：当前触发时间是14:30，所以以下条件在14:30触发
            # 条件2.1: 涨幅小于3个点
            if today_change < 0.03:
                new_sell_conditions.append(f"下午2点半后涨幅小于3%({today_change:.2%})")
            
            # 条件2.2: 涨幅小于5个点且阴线
            if today_change < 0.05 and is_negative_line:
                new_sell_conditions.append(f"下午2点半后涨幅小于5%且阴线({today_change:.2%})")
            
            # 条件2.3: 涨停开板回落3%以上
            if is_limit_up and fall_back >= 0.03:
                new_sell_conditions.append(f"下午2点半后涨停开板回落{fall_back:.2%}")
            
            # 修复的利润保护机制
            profit_protection_conditions = []
            
            # 计算从最高价回撤的幅度
            if highest_price > 0 and posInfo.cost_basis > 0:
                max_profit = (highest_price - posInfo.cost_basis) / posInfo.cost_basis
                current_drawdown = (highest_price - posInfo.last_sale_price) / highest_price
                
                # 修复：使用当前从最高点回撤的幅度，而不是当前总盈利
                # 条件1: 获利5%以上，从最高点回撤超过80%时卖出
                if max_profit >= 0.05 and current_drawdown >= 0.8:
                    profit_protection_conditions.append(f"获利5%以上回撤80%(最高获利:{max_profit:.2%}, 回撤:{current_drawdown:.2%})")
                
                # 条件2: 获利15%以上，从最高点回撤超过67%时卖出
                if max_profit >= 0.15 and current_drawdown >= 0.67:
                    profit_protection_conditions.append(f"获利15%以上回撤67%(最高获利:{max_profit:.2%}, 回撤:{current_drawdown:.2%})")
                
                # 条件3: 获利20%以上，从最高点回撤超过50%时卖出
                if max_profit >= 0.20 and current_drawdown >= 0.50:
                    profit_protection_conditions.append(f"获利20%以上回撤50%(最高获利:{max_profit:.2%}, 回撤:{current_drawdown:.2%})")
            
            # 原止损止盈条件
            stop_loss_condition = holdRet <= g.config['SL']  # 止损条件
            take_profit_condition = holdRet >= g.config['TP']  # 止盈条件
            
            # 新增：持仓超过最大天数强制卖出条件
            force_sell_condition = False
            force_sell_reason = ""
            
            if sec in g.buy_dates:
                buy_date = g.buy_dates[sec]
                # 计算持仓天数（交易日天数）
                current_date = context.current_dt.date()
                holding_days = (current_date - buy_date).days
                if holding_days >= g.config['max_holding_days']:
                    force_sell_condition = True
                    force_sell_reason = f"持仓超过{g.config['max_holding_days']}天(已持仓{holding_days}天)"
            
            # 卖出条件：新增条件 或 利润保护 或 原止损止盈 或 指标信号 或 强制卖出
            should_sell = (len(new_sell_conditions) > 0 or 
                          len(profit_protection_conditions) > 0 or 
                          sell_signals_count >= 3 or 
                          buy_signals_count < 6 or 
                          stop_loss_condition or 
                          take_profit_condition or
                          force_sell_condition)
            
            if should_sell:
                vol = posInfo.enable_amount
                if vol > 0:
                    order(sec, -vol)
                    reason = []
                    if len(new_sell_conditions) > 0:
                        reason.append(f"新增卖出条件: {', '.join(new_sell_conditions)}")
                    if len(profit_protection_conditions) > 0:
                        reason.append(f"利润保护: {', '.join(profit_protection_conditions)}")
                    if sell_signals_count >= 3:
                        reason.append(f"卖出信号({sell_signals_count}/10)")
                    if buy_signals_count < 6:
                        reason.append(f"买入信号不足({buy_signals_count}/6)")
                    if stop_loss_condition:
                        reason.append(f"止损({holdRet:.2%})")
                    if take_profit_condition:
                        reason.append(f"止盈({holdRet:.2%})")
                    if force_sell_condition:
                        reason.append(force_sell_reason)
                    
                    # 卖出后删除记录
                    if sec in g.highest_prices:
                        del g.highest_prices[sec]
                    if sec in g.buy_dates:
                        del g.buy_dates[sec]
                        
                    try:
                        sec_name = get_security_info(sec).display_name if get_security_info(sec) else sec
                    except:
                        sec_name = sec
                    # 获取连板数信息
                    limit_times = g.limit_times_dict.get(sec, "未知")
                    log.info(f"卖出 {sec} {sec_name} ({limit_times}板): {', '.join(reason)}")
                else:
                    log.warning(f"尝试卖出{sec}但可卖数量为0")
            else:
                # 记录未卖出的原因（调试用）
                limit_times = g.limit_times_dict.get(sec, "未知")
                log.debug(f"持有 {sec} ({limit_times}板): 买入信号={buy_signals_count}, 卖出信号={sell_signals_count}, 持仓收益={holdRet:.2%}, 止损={stop_loss_condition}, 止盈={take_profit_condition}")
        except Exception as e:
            log.error(f"处理卖出逻辑时出错 {sec}: {e}")

def calculate_indicators(sec):
    """计算指定股票的技术指标信号
    
    Returns:
        tuple: (买入信号数量, 卖出信号数量, 买入信号字典)
    """
    try:
        # 检查缓存
        cache_key = f"{sec}_{g.data_cache.get('date', '')}"
        if cache_key in g.data_cache:
            return g.data_cache[cache_key]
        
        # 获取历史数据，考虑到机构控盘和多空底顶指标需要足够数据，我们获取61天数据足够
        secData = get_history(61, '1d', ['close', 'open', 'high', 'low', 'volume'], sec, fq='pre', include=True)
        
        if secData is None or len(secData) < 60:  # 确保有足够数据
            log.warning(f"获取{sec}的历史数据失败或数据不足")
            return 0, 0, {}
        
        # 将数据转换为numpy数组供talib使用
        close_prices = secData['close'].values
        high_prices = secData['high'].values
        low_prices = secData['low'].values
        volumes = secData['volume'].values
        
        # 初始化指标信号
        condBuy_list = []
        condSell_list = []
        buy_signals = {}  # 用于存储每个指标的买入信号
        
        # 指标1: MA多均线策略
        if g.config['MA'][-1]:
            ma_periods = g.config['MA'][:7]
            ma_values = {}
            
            if len(secData) >= max(ma_periods):
                for i, period in enumerate(ma_periods):
                    ma_values[f'MA{i+1}'] = talib.MA(close_prices, timeperiod=period)
                
                # 获取当前值和前一日值
                ma1_current = ma_values['MA1'][-1]
                ma1_last = ma_values['MA1'][-2]
                ma2_current = ma_values['MA2'][-1]
                ma2_last = ma_values['MA2'][-2]
                ma3_current = ma_values['MA3'][-1]
                ma3_last = ma_values['MA3'][-2]
                ma4_current = ma_values['MA4'][-1]
                ma4_last = ma_values['MA4'][-2]
                ma5_current = ma_values['MA5'][-1]
                ma5_last = ma_values['MA5'][-2]
                ma6_current = ma_values['MA6'][-1]
                ma6_last = ma_values['MA6'][-2]
                ma7_current = ma_values['MA7'][-1]
                ma7_last = ma_values['MA7'][-2]
                close_current = close_prices[-1]
                
                # 计算XG1条件
                condition1 = ma6_current > ma6_last
                condition2 = (ma5_current > ma5_last) and (ma4_current > ma4_last) and (ma3_current > ma3_last) and (ma3_current > ma4_current) and (ma4_current > ma5_current)
                condition3 = (ma7_current > ma7_last) and (ma6_current > ma7_current) and (close_current > ma7_current)
                XG1 = condition1 or condition2 or condition3
                
                # 计算XG2条件
                XG2 = (ma5_current > ma6_current) and (ma6_current > ma7_current)
                
                # 计算买入条件
                condBuy1 = (XG1 or XG2) and (ma1_current > ma1_last) and (ma2_current > ma2_last) and (close_current > ma1_current)
                
                # 计算卖出条件
                band_red1 = (ma1_current < ma1_last) and (ma2_current < ma2_last)
                band_red2 = (ma1_current < ma1_last) and (ma1_current < ma2_current)
                band_red3 = (ma1_current < ma1_last) and (close_current < ma1_current)
                condSell1 = band_red1 or band_red2 or band_red3
            else:
                condBuy1 = False
                condSell1 = False
            
            condBuy_list.append(condBuy1)
            condSell_list.append(condSell1)
            buy_signals['MA'] = condBuy1
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['MA'] = False
        
        # 指标2: KDJ指标
        if g.config['KDJ'][-1]:
            k_, d_ = talib.STOCH(high_prices, low_prices, close_prices, 
                                 fastk_period=g.config['KDJ'][0],
                                 slowk_period=g.config['KDJ'][1],
                                 slowk_matype=0,
                                 slowd_period=g.config['KDJ'][2],
                                 slowd_matype=0)
            j_ = 3 * k_ - 2 * d_
            
            k_current = k_[-1]
            k_last = k_[-2]
            d_current = d_[-1]
            d_last = d_[-2]
            j_current = j_[-1]
            j_last = j_[-2]
            
            zhunduo = (j_current > k_current) or \
                     (d_current > 72 and k_current > 70 and j_current > 65) or \
                     (j_current > j_last and k_current > k_last and j_current > 50 and k_current > 50)
            
            condSell2 = (not zhunduo) or (k_current < d_current and k_current < k_last and d_current < d_last and j_current < j_last)
            condBuy2 = zhunduo and not (k_current < d_current and k_current < k_last and d_current < d_last and j_current < j_last)
            
            condBuy_list.append(condBuy2)
            condSell_list.append(condSell2)
            buy_signals['KDJ'] = condBuy2
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['KDJ'] = False
        
        # 指标3: RSI指标
        if g.config['RSI'][-1]:
            rsi1_ = talib.RSI(close_prices, timeperiod=g.config['RSI'][0])
            rsi2_ = talib.RSI(close_prices, timeperiod=g.config['RSI'][1])
            rsi3_ = talib.RSI(close_prices, timeperiod=g.config['RSI'][2])
            
            rsi1_current = rsi1_[-1]
            rsi1_last = rsi1_[-2]
            rsi2_current = rsi2_[-1]
            rsi2_last = rsi2_[-2]
            rsi3_current = rsi3_[-1]
            rsi3_last = rsi3_[-2]
            
            xg1 = (rsi3_current >= 48) and (
                (rsi1_current > rsi2_current and rsi2_current > rsi3_current) or
                (rsi1_current > rsi3_current and rsi3_current > rsi2_current) or
                (rsi2_current > rsi1_current and rsi1_current > rsi3_current)
            )
            
            xg2 = (rsi3_current >= 48) and (
                (rsi1_current > rsi2_current and rsi2_current > rsi3_current and rsi3_current > 48) or
                (rsi1_current > rsi1_last and rsi2_current > rsi2_last and rsi3_current > rsi3_last)
            )
            
            xg3 = (rsi1_current > 80 and rsi2_current > 80 and rsi3_current > 80)
            
            xg4 = (rsi1_current > 40 and rsi1_current > rsi2_current and rsi2_current > rsi3_current)
            
            xg5 = (rsi1_current > rsi2_current and rsi3_current > 40) and (rsi1_current >= rsi1_last and rsi2_current >= rsi2_last)
            
            xg6 = (rsi1_current > rsi2_current and rsi2_current > rsi3_current and rsi1_current > 20) and (rsi1_current >= rsi1_last and rsi2_current >= rsi2_last and rsi3_current >= rsi3_last)
            
            cross1 = rsi1_current > rsi2_current and rsi1_last <= rsi2_last
            cross2 = rsi2_current > rsi3_current and rsi2_last <= rsi3_last
            xg7 = (rsi1_current > rsi1_last and rsi2_current > rsi2_last and rsi3_current > rsi3_last) and (cross1 or cross2)
            
            rsi_bottom_divergence = cross1 and (close_prices[-2] > close_prices[-1]) and (rsi1_last < rsi1_current)
            
            condBuy3 = (xg1 or xg2 or xg3 or xg4 or xg5 or xg6 or xg7 or rsi_bottom_divergence) and (rsi1_current >= 40)
            
            sell_cond1 = (rsi3_current < 48 and rsi1_current < 40) or (rsi1_current < rsi2_current and rsi2_current < rsi3_current and 
                       rsi1_current < rsi1_last and rsi2_current < rsi2_last and rsi3_current < rsi3_last)
            
            sell_cond2 = rsi1_current < rsi2_current and (rsi1_current < rsi1_last and rsi2_current < rsi2_last and rsi3_current < rsi3_last)
            
            condSell3 = sell_cond1 or sell_cond2
            
            condBuy_list.append(condBuy3)
            condSell_list.append(condSell3)
            buy_signals['RSI'] = condBuy3
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['RSI'] = False
        
        # 指标4: CCI指标
        if g.config['CCI'][-1]:
            cci_ = talib.CCI(high_prices, low_prices, close_prices, timeperiod=g.config['CCI'][0])
            
            cci_current = cci_[-1]
            cci_last = cci_[-2]
            
            condBuy4 = (cci_current > 100) or (cci_current > 0 and cci_current > cci_last)
            condSell4 = (cci_current < -100) or (cci_current < 0 and cci_current < cci_last)
            
            condBuy_list.append(condBuy4)
            condSell_list.append(condSell4)
            buy_signals['CCI'] = condBuy4
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['CCI'] = False
        
        # 指标5: 短线决策(DXJC)
        if g.config['DXJC'][-1]:
            N = g.config['DXJC'][0]
            if len(secData) >= N:
                AMOV = volumes * (close_prices + low_prices + high_prices) / 3
                AMOV_series = pd.Series(AMOV)
                volume_series = pd.Series(volumes)
                MN = AMOV_series.rolling(N).sum() / volume_series.rolling(N).sum()
                condBuy5 = close_prices[-1] >= MN.iloc[-1] if not pd.isna(MN.iloc[-1]) else False
                condSell5 = close_prices[-1] < MN.iloc[-1] if not pd.isna(MN.iloc[-1]) else False
            else:
                condBuy5 = False
                condSell5 = False
            
            condBuy_list.append(condBuy5)
            condSell_list.append(condSell5)
            buy_signals['DXJC'] = condBuy5
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['DXJC'] = False
        
        # 指标6: MACD指标
        if g.config['MACD'][-1]:
            try:
                macd, macdsignal, macdhist = talib.MACD(close_prices, 
                                                       fastperiod=g.config['MACD'][0],
                                                       slowperiod=g.config['MACD'][1],
                                                       signalperiod=g.config['MACD'][2])
                
                if len(macd) >= 3 and len(macdsignal) >= 3 and len(macdhist) >= 3:
                    dif_current = macd[-1]
                    dif_last1 = macd[-2]
                    dif_last2 = macd[-3]
                    
                    dea_current = macdsignal[-1]
                    dea_last1 = macdsignal[-2]
                    dea_last2 = macdsignal[-3]
                    
                    macd_current = macdhist[-1]
                    macd_last1 = macdhist[-2]
                    macd_last2 = macdhist[-3]
                    
                    ma1 = talib.MA(close_prices, timeperiod=5)
                    ma2 = talib.MA(close_prices, timeperiod=10)
                    
                    ma1_current = ma1[-1]
                    ma2_current = ma2[-1]
                    
                    xg1_macd = ((dif_current > dea_current or dif_current > dif_last1) and 
                               ((dif_current > -0.15 and dea_current > -0.25) or (dif_current > dif_last1 and dea_current > dea_last1)) and
                               (macd_current >= 0 or macd_current > macd_last1) and (macd_current > -0.35) and
                               (close_prices[-1] > ma1_current or close_prices[-1] > ma2_current))
                    
                    xg2_macd = dea_current > -0.3 and dea_current > 0 and macd_current > macd_last1
                    xg3_macd = dif_current > dif_last1 and dea_current > dea_last1 and dif_current > dea_current and close_prices[-1] > ma2_current
                    xg4_macd = dif_current > dif_last1 and macd_current > macd_last1
                    xg5_macd = macd_last1 > macd_last2 and macd_current > macd_last1
                    xg6_macd = dea_current > dea_last1 and dif_current > dea_current and dea_current > 0
                    
                    macd_buy_filter = (macd_current > -2) or (dea_current > -1.2 and dif_current > -0.5 and macd_current > macd_last1)
                    condBuy6 = (xg1_macd or xg2_macd or xg3_macd or xg4_macd or xg5_macd or xg6_macd) and macd_buy_filter
                    
                    condSell6 = (not condBuy6) or (dea_current < dea_last1 and dif_current < dea_current)
                else:
                    condBuy6 = False
                    condSell6 = False
                    
            except Exception as e:
                log.error(f"计算MACD指标时出错: {e}")
                condBuy6 = False
                condSell6 = False
            
            condBuy_list.append(condBuy6)
            condSell_list.append(condSell6)
            buy_signals['MACD'] = condBuy6
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['MACD'] = False
        
        # 指标7: 布林带(BBIBOLL)
        if g.config['BBIBOLL'][-1]:
            N = g.config['BBIBOLL'][0]
            M = g.config['BBIBOLL'][1]
            
            if len(secData) >= 24:
                CV = close_prices
                ma3 = talib.MA(CV, timeperiod=3)
                ma6 = talib.MA(CV, timeperiod=6)
                ma12 = talib.MA(CV, timeperiod=12)
                ma24 = talib.MA(CV, timeperiod=24)
                BBIBOLL = (ma3 + ma6 + ma12 + ma24) / 4
                
                std_val = talib.STDDEV(BBIBOLL, timeperiod=N, nbdev=1)
                UPR = BBIBOLL + M * std_val
                DWN = BBIBOLL - M * std_val
                
                bbiboll_current = BBIBOLL[-1]
                bbiboll_last = BBIBOLL[-2]
                upr_current = UPR[-1]
                upr_last = UPR[-2]
                dwn_current = DWN[-1]
                dwn_last = DWN[-2]
                close_current = close_prices[-1]
                close_last = close_prices[-2]
                
                XG1 = (bbiboll_current > 0.999 * bbiboll_last) and \
                      (close_current > 0.99 * bbiboll_current) and \
                      ((upr_current > upr_last) or (dwn_current > dwn_last))
                
                XG2 = (bbiboll_current > 0.98 * bbiboll_last) and \
                      (close_current > 0.98 * bbiboll_current) and \
                      ((upr_current > 0.98 * upr_last) or (dwn_current > 0.98 * dwn_last)) and \
                      (close_current > 1.04 * close_last)
                
                condBuy7 = XG1 or XG2
                condSell7 = (bbiboll_current < bbiboll_last) or (close_current < bbiboll_current)
            else:
                condBuy7 = False
                condSell7 = False
            
            condBuy_list.append(condBuy7)
            condSell_list.append(condSell7)
            buy_signals['BBIBOLL'] = condBuy7
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['BBIBOLL'] = False
        
        # 指标8: WR指标
        if g.config['WR'][-1]:
            N = g.config['WR'][0]
            N1 = g.config['WR'][1]
            
            if len(secData) >= max(N, N1):
                hhv_N = talib.MAX(high_prices, timeperiod=N)
                llv_N = talib.MIN(low_prices, timeperiod=N)
                WR1 = 100 * (hhv_N - close_prices) / (hhv_N - llv_N + 1e-10)
                
                hhv_N1 = talib.MAX(high_prices, timeperiod=N1)
                llv_N1 = talib.MIN(low_prices, timeperiod=N1)
                WR2 = 100 * (hhv_N1 - close_prices) / (hhv_N1 - llv_N1 + 1e-10)
                
                wr1_current = WR1[-1]
                wr2_current = WR2[-1]
                
                condSell8 = (wr1_current > 65) or (wr2_current > 65)
                condBuy8 = (wr1_current < 65) or (wr2_current < 65)
            else:
                condBuy8 = False
                condSell8 = False
            
            condBuy_list.append(condBuy8)
            condSell_list.append(condSell8)
            buy_signals['WR'] = condBuy8
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['WR'] = False
        
        # 指标9: 多空底顶指标(DKDD)
        if g.config['DKDD'][-1]:
            if len(secData) >= 27:
                llv_27 = talib.MIN(low_prices, timeperiod=27)
                hhv_27 = talib.MAX(high_prices, timeperiod=27)
                RSV = 100 * (close_prices - llv_27) / (hhv_27 - llv_27 + 1e-10)
                
                sma1 = talib.SMA(RSV, timeperiod=5)
                sma2 = talib.SMA(sma1, timeperiod=3)
                sma3 = talib.SMA(sma2, timeperiod=2)
                
                NIU = 5 * sma1 - 3 * sma2 - sma3
                niu_ma5 = talib.MA(NIU, timeperiod=5)
                XIONG = talib.EMA(niu_ma5, timeperiod=3)
                
                niu_current = NIU[-1]
                niu_last = NIU[-2]
                xiong_current = XIONG[-1]
                xiong_last = XIONG[-2]
                close_current = close_prices[-1]
                
                DI = -5
                
                N_period = min(8, len(secData))
                AMOV = volumes * (close_prices + low_prices + high_prices) / 3
                AMOV_series = pd.Series(AMOV)
                volume_series = pd.Series(volumes)
                MN = AMOV_series.rolling(N_period).sum() / volume_series.rolling(N_period).sum()
                mn_current = MN.iloc[-1] if not pd.isna(MN.iloc[-1]) else 0
                mn_last = MN.iloc[-2] if not pd.isna(MN.iloc[-2]) else 0
                
                cross_condition = (niu_last <= DI and niu_current > DI)
                buy_cond1 = cross_condition or (niu_current >= xiong_current and niu_current > 0)
                buy_cond2 = niu_current < 0 and niu_current > niu_last and niu_current >= xiong_current
                buy_cond3 = niu_current > 100
                buy_cond4 = niu_current > 70 and (close_current > mn_current and mn_current > mn_last)
                buy_cond5 = (niu_current < 40 and xiong_current < 40 and 
                            niu_current < xiong_current and 
                            niu_current > niu_last and 
                            xiong_current < xiong_last)
                
                condBuy9 = buy_cond1 or buy_cond2 or buy_cond3 or buy_cond4 or buy_cond5
                
                sell_cond1 = niu_current > 100 and niu_current < xiong_current and not (niu_current > 70 and close_current > mn_current and mn_current > mn_last)
                sell_cond2 = niu_current <= xiong_current and niu_current < 100 and not (niu_current > 70 and close_current > mn_current and mn_current > mn_last)
                sell_cond3 = (niu_current > xiong_current and 
                             niu_current < niu_last and 
                             xiong_current > xiong_last and 
                             niu_current < 100 and 
                             not (niu_current > 70 and close_current > mn_current and mn_current > mn_last))
                sell_cond4 = niu_current < 100 and (niu_current + 8) < xiong_current and niu_current < niu_last
                
                condSell9 = sell_cond1 or sell_cond2 or sell_cond3 or sell_cond4
            else:
                condBuy9 = False
                condSell9 = False
            
            condBuy_list.append(condBuy9)
            condSell_list.append(condSell9)
            buy_signals['DKDD'] = condBuy9
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['DKDD'] = False
        
        # 指标10: 机构控盘指标(JKGP)
        if g.config['JKGP'][-1]:
            # 计算机构控盘指标
            ema_period = g.config['JKGP'][0]
            kp2_period = g.config['JKGP'][1]
            kp5_period = g.config['JKGP'][2]
            
            if len(secData) >= max(ema_period, kp2_period, kp5_period, 8):
                # 计算VAR1 = EMA(EMA(CLOSE,9),9)
                ema1 = talib.EMA(close_prices, timeperiod=ema_period)
                VAR1 = talib.EMA(ema1, timeperiod=ema_period)
                
                # 计算控盘值
                kp_current = (VAR1[-1] - VAR1[-2]) / VAR1[-2] * 10000 if VAR1[-2] != 0 else 0
                kp_last = (VAR1[-2] - VAR1[-3]) / VAR1[-3] * 10000 if VAR1[-3] != 0 else 0
                
                # 计算KP2和KP5
                kp_values = []
                for i in range(len(VAR1)-1):
                    if VAR1[i] != 0:
                        kp_val = (VAR1[i+1] - VAR1[i]) / VAR1[i] * 10000
                        kp_values.append(kp_val)
                    else:
                        kp_values.append(0)
                
                kp_series = pd.Series(kp_values)
                KP2 = kp_series.rolling(kp2_period).mean()
                KP5 = kp_series.rolling(kp5_period).mean()
                
                kp2_current = KP2.iloc[-1] if not pd.isna(KP2.iloc[-1]) else 0
                kp2_last = KP2.iloc[-2] if len(KP2) >= 2 and not pd.isna(KP2.iloc[-2]) else 0
                kp5_current = KP5.iloc[-1] if not pd.isna(KP5.iloc[-1]) else 0
                kp5_last = KP5.iloc[-2] if len(KP5) >= 2 and not pd.isna(KP5.iloc[-2]) else 0
                
                # 计算MA1-MA7均线
                ma1 = talib.MA(close_prices, timeperiod=5)
                ma2 = talib.MA(close_prices, timeperiod=10)
                ma3 = talib.MA(close_prices, timeperiod=20)
                ma4 = talib.MA(close_prices, timeperiod=30)
                ma5 = talib.MA(close_prices, timeperiod=60)
                ma6 = talib.MA(close_prices, timeperiod=120)
                ma7 = talib.MA(close_prices, timeperiod=250)
                
                ma5_current = ma5[-1] if len(ma5) > 0 else 0
                ma5_last = ma5[-2] if len(ma5) >= 2 else 0
                
                # 计算MN（成本均线）
                N_period = min(8, len(secData))
                AMOV = volumes * (close_prices + low_prices + high_prices) / 3
                AMOV_series = pd.Series(AMOV)
                volume_series = pd.Series(volumes)
                MN = AMOV_series.rolling(N_period).sum() / volume_series.rolling(N_period).sum()
                mn_current = MN.iloc[-1] if not pd.isna(MN.iloc[-1]) else 0
                
                close_current = close_prices[-1]
                
                # 计算通达信中的各种买入条件
                xg1_condition = ((kp_current > kp_last and kp_current > 0) or (kp_current > -80)) and (kp5_current > kp5_last and kp_current > kp5_current)
                
                xg2_condition = (kp_current > kp_last and 
                               kp5_current > kp5_last and kp_current > kp5_current and
                               kp2_current > kp2_last and kp_current > kp2_current and
                               kp2_current > kp5_current and
                               (kp_current > -120 or (close_current > ma5_current and ma5_current > ma5_last)))
                
                youzhuang_kongpan = kp_current > kp_last and kp_current > 0
                xg4_condition = kp2_current > kp2_last and youzhuang_kongpan
                
                xg5_condition = kp2_current > kp2_last and kp_current > kp_last and kp_current < 0
                
                xg6_condition = kp_current > kp_last and kp_current > -80 and kp2_current > kp2_last
                
                close_last = close_prices[-2] if len(close_prices) >= 2 else 0
                xg7_condition = kp_current > kp_last and close_current > 1.07 * close_last
                
                xg8_condition = kp_current > 100
                
                # 买入条件：((XG1 OR XG2 OR XG4 OR XG5 OR XG6 OR XG7 OR XG8) AND (控盘>-50 OR C>=MN))
                xg_conditions = xg1_condition or xg2_condition or xg4_condition or xg5_condition or xg6_condition or xg7_condition or xg8_condition
                condBuy10 = xg_conditions and (kp_current > -50 or close_current >= mn_current)
                
                # 卖出条件：根据通达信卖出条件简化处理
                condSell10 = (kp_current < kp_last and kp_current < 0) or (kp_current < -40)
            else:
                condBuy10 = False
                condSell10 = False
            
            condBuy_list.append(condBuy10)
            condSell_list.append(condSell10)
            buy_signals['JKGP'] = condBuy10
        else:
            condBuy_list.append(False)
            condSell_list.append(False)
            buy_signals['JKGP'] = False
        
        # 统计信号数量
        buy_signals_count = sum(condBuy_list)
        sell_signals_count = sum(condSell_list)
        
        # 缓存结果
        result = (buy_signals_count, sell_signals_count, buy_signals)
        # g.data_cache[cache_key] = result
        
        return result
    except Exception as e:
        log.error(f"计算股票{sec}的指标时发生错误: {e}")
        return 0, 0, {}

def handle_data(context, data):
    # 获取当前时间（时分）
    current_time = context.blotter.current_dt.time()
    current_hour = current_time.hour
    current_minute = current_time.minute
    
    # 1. 10点前（包含10:00）每分钟执行buy_event
    if current_hour < 10 or (current_hour == 10 and current_minute == 0):
        buy_event(context)
    
    # 2. 全天每分钟执行sell_event
    sell_event(context)
   