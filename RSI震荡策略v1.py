import pandas as pd
import numpy as np

def initialize(context):
    # ========== 1. 基础配置优化 ==========
    g.etf_list = list(dict.fromkeys([
        # '518880.SS',   # 黄金ETF  
        # '162719.SZ',   # 广发石油   
        # '560860.SS',   # 工业有色   
        # '516650.SS',   # 有色50   
        # '513080.SS',   # 法国ETF  
        # '513030.SS',   # 德国ETF  
        # '516020.SS',   # 化工ETF
        # '510300.SS',  # 沪深300
        # '513100.SS',  # 纳指ETF
        # '513500.SS',  # 标普500ETF
        # '512100.SS',  # 中证1000ETF
        # '513520.SS',  # 日经225ETF
        # '513180.SS',  # 恒生科技指数ETF   
        # '510880.SS',  # 上证红利ETF
        # '512180.SS',  # MSCIA股ETF
        # '159915.SZ',  #创业板ETF
        # '561580.SS',  #央企红利ETF
        # '512480.SS',  # 半导体ETF
        # '159857.SZ',  # 光伏ETF
        # '516290.SS',  #光伏龙头ETF
        # '515050.SS',  # 5G通信ETF
        '515880.SS',  # 通信ETF
        # '561360.SS',  #石油ETF
        # '159570.SZ',  # 港股通创新药
        # '588060.SS',  #科创50ETF
        # '588200.SS',  # 科创芯片ETF
        # '515980.SS',  # 人工智能ETF
        # '561910.SS',  #电池ETF
        # '159851.SZ',  # 金融科技ETF
        # '516750.SS',  # 养殖ETF
        # '515220.SS',  #煤炭ETF
        # '512820.SS',  #银行ETF 
        # '516150.SS',  # 稀土ETF
        # '588710.SS',  # 科创半导体设备ETF
        # '515030.SS',  # 新能源车ETF
        # '159767.SZ',  # 电池龙头ETF
        # '516770.SS',  # 游戏动漫ETF
        # '513310.SS',  # 中韩芯片ETF
        # '159805.SZ',  # 传媒ETF
    ]))
    
    # ========== 2. 策略参数优化（调整为日线级别） ==========
    g.buy_threshold = 30  # RSI买入阈值（上穿30）
    g.sell_threshold = 65  # RSI卖出阈值（下穿65）
    g.momentum_period = 20  # 动量计算周期（日线数）
    g.frequency = '1d'  # 调整为日线周期
    g.max_position_ratio = 0.8  # 单标的最大仓位比例（风控）
    g.stop_loss_ratio = 0.05  # 5%止损比例
    g.max_hold_etfs = 3  # 最大持仓ETF数量（分散风险）
    g.min_momentum = 0.005  # 最小动量阈值（过滤弱动量）
    g.ma_cross_days = 40  # 计算均线交叉的周期（40天）
    g.short_ma = 5  # 短期均线（5日线）
    g.long_ma = 10  # 长期均线（10日线）
    g.rsi_rate = 0.5
    g.stop_loss_low_days = 5  # 新增：跌破N天最低价止损的周期（5天）
    set_universe(g.etf_list)  # 设置ETF池
    g.buy_price = {}  # 记录买入价格（用于止损）
    # 新增：标记当日是否已执行2:30的操作，避免重复执行
    g.executed_afternoon_task = False  

# ========== 修复：动态计算RSI周期的核心方法（解决索引越界+类型错误） ==========
def calculate_rsi_period(context, etf_code):
    """
    动态计算RSI的period参数
    核心修复：
    1. 修正高低点索引计算逻辑，避免iloc越界
    2. 增加索引有效性校验
    3. 统一索引为从0开始的位置索引
    """
    try:
        # 1. 获取足够的日线数据（40天+10天，确保均线计算完整）
        history_data = get_history(
            count=g.ma_cross_days + g.long_ma,
            frequency='1d',
            field='close',
            security_list=etf_code,
            include=True
        )
        # 增加数据空值/长度双重校验
        if history_data.empty or len(history_data) < g.ma_cross_days + g.long_ma:
            log.info(f"{etf_code} 均线计算数据不足/为空，默认返回RSI周期12")
            return 12
        
        close_series = history_data['close'].copy()
        # 重置索引为从0开始的整数（关键：统一索引体系）
        close_series = close_series.reset_index(drop=True)
        total_length = len(close_series)  # 记录总长度，用于后续索引校验
        
        # 2. 计算5日和10日均线（处理空值）
        ma5 = close_series.rolling(window=g.short_ma, min_periods=1).mean()
        ma10 = close_series.rolling(window=g.long_ma, min_periods=1).mean()
        
        # 3. 识别均线交叉点（上穿/下穿）
        ma_diff = ma5 - ma10
        cross_points = []  # 存储交叉点：(索引, 类型) 类型：up=上穿，down=下穿
        
        # 过滤掉ma_diff为空的行
        ma_diff_valid = ma_diff.dropna()
        if len(ma_diff_valid) < 2:
            log.info(f"{etf_code} 均线差值数据不足，默认返回RSI周期12")
            return 12
        
        # 遍历有效差值，判断交叉（使用位置索引）
        for i in range(1, len(ma_diff_valid)):
            prev_diff = ma_diff_valid.iloc[i-1]
            curr_diff = ma_diff_valid.iloc[i]
            
            # 上穿：前一天ma5<ma10，当天ma5≥ma10
            if prev_diff < 0 and curr_diff >= 0:
                cross_points.append((ma_diff_valid.index[i], 'up'))
            # 下穿：前一天ma5>ma10，当天ma5≤ma10
            elif prev_diff > 0 and curr_diff <= 0:
                cross_points.append((ma_diff_valid.index[i], 'down'))
        
        # 4. 过滤过去40天内的交叉点（只保留最后40天的数据）
        recent_cross_points = cross_points[-g.ma_cross_days:] if cross_points else []
        cross_count = len(recent_cross_points)
        
        # 5. 根据交叉次数分支处理
        if cross_count <= 3:
            # log.info(f"{etf_code} 过去{g.ma_cross_days}天均线交叉次数为{cross_count}，震荡周期长，RSI周期设为12")
            return 12
        else:
            # 取最近3次交叉点
            latest_3_cross = recent_cross_points[-3:]
            price_points = []  # 存储高低点：(类型, 索引, 价格) 类型：low=低点，high=高点
            
            # 初始化数值类型变量
            total_kline_count = 0  # 存储高低点之间的K线根数总和
            valid_pair_count = 0   # 有效高低点对数量
            
            for i in range(len(latest_3_cross)-1):
                cross1_idx, cross1_type = latest_3_cross[i]
                cross2_idx, cross2_type = latest_3_cross[i+1]
                
                # 确保索引为整数，且在合法范围内
                start_idx = min(int(cross1_idx), int(cross2_idx))
                end_idx = max(int(cross1_idx), int(cross2_idx))
                
                # 核心修复1：严格校验索引边界，防止越界
                start_idx = max(0, start_idx)
                end_idx = min(total_length - 1, end_idx)
                
                # 如果起始索引 >= 结束索引，跳过该区间
                if start_idx >= end_idx:
                    log.warning(f"{etf_code} 交叉点区间无效（start={start_idx}, end={end_idx}），跳过")
                    continue
                
                # 确定价格区间：使用iloc切片（基于位置的索引）
                price_slice = close_series.iloc[start_idx:end_idx+1]
                if price_slice.empty or len(price_slice) < 2:
                    continue
                
                # 提取高低点（核心修复2：使用位置索引，避免越界）
                try:
                    if cross1_type == 'down' and cross2_type == 'up':
                        # 下穿→上穿：取区间最低点
                        # 先获取最小值的位置（相对于切片的位置）
                        low_pos = price_slice.argmin()  # 返回位置索引（0开始）
                        # 转换为全局位置索引
                        global_low_idx = start_idx + low_pos
                        # 校验全局索引是否合法
                        if 0 <= global_low_idx < total_length:
                            low_price = price_slice.iloc[low_pos]
                            price_points.append(('low', global_low_idx, low_price))
                        else:
                            log.warning(f"{etf_code} 低点全局索引{global_low_idx}越界（总长度{total_length}），跳过")
                            continue
                    
                    elif cross1_type == 'up' and cross2_type == 'down':
                        # 上穿→下穿：取区间最高点
                        high_pos = price_slice.argmax()  # 返回位置索引（0开始）
                        global_high_idx = start_idx + high_pos
                        # 校验全局索引是否合法
                        if 0 <= global_high_idx < total_length:
                            high_price = price_slice.iloc[high_pos]
                            price_points.append(('high', global_high_idx, high_price))
                        else:
                            log.warning(f"{etf_code} 高点全局索引{global_high_idx}越界（总长度{total_length}），跳过")
                            continue
                except Exception as inner_e:
                    log.warning(f"{etf_code} 提取高低点异常：{str(inner_e)}，跳过该区间")
                    continue
                
                # 计算高低点对之间的K线根数（距离）
                if len(price_points) >= 2:
                    p1_type, p1_idx, _ = price_points[-2]
                    p2_type, p2_idx, _ = price_points[-1]
                    
                    # 确保索引为整数且合法
                    p1_idx_int = int(p1_idx) if isinstance(p1_idx, (int, np.integer)) else 0
                    p2_idx_int = int(p2_idx) if isinstance(p2_idx, (int, np.integer)) else 0
                    
                    # 校验索引是否在合法范围
                    if not (0 <= p1_idx_int < total_length and 0 <= p2_idx_int < total_length):
                        log.warning(f"{etf_code} 高低点索引{p1_idx_int}/{p2_idx_int}越界，跳过")
                        continue
                    
                    if (p1_type == 'low' and p2_type == 'high') or (p1_type == 'high' and p2_type == 'low'):
                        # 计算两个点之间的K线根数（索引差值的绝对值）
                        kline_distance = abs(p1_idx_int - p2_idx_int)
                        if kline_distance >= 0:
                            total_kline_count += kline_distance
                            valid_pair_count += 1
            
            # 计算最终period（取K线根数的整数部分，至少为6，避免过小）
            if valid_pair_count > 0:
                avg_kline_distance = total_kline_count / valid_pair_count
                period = max(int(round(avg_kline_distance)), 6)
            else:
                # 无有效高低点对，默认返回12
                period = 12
            
            # log.info(f"{etf_code} 过去{g.ma_cross_days}天均线交叉次数为{cross_count}，计算得RSI周期为{period}（基于高低点K线根数）")
            return period
    except Exception as e:
        log.error(f"{etf_code} 计算RSI周期异常：{str(e)}，默认返回12")
        return 12

def before_trading_start(context, data):
    # ========== 第一步：量化选股 ==========   
    # print(f"默认股票池：{g.etf_list}")
    # 每日开盘重置2:30任务执行标记
    g.executed_afternoon_task = False  

# ========== 优化：增强版止损检查函数（新增跌破5天最低价止损） ==========
def check_stop_loss(context, data):
    """
    止损检查逻辑，每分钟执行
    新增：
    1. 原有5%亏损止损逻辑保留
    2. 新增跌破过去5天最低价止损逻辑
    """
    for etf in g.etf_list:
        try:
            position = get_position(etf)
            if position and position.amount > 0:
                # 获取当前价格
                current_price = data[etf].close if etf in data else 0
                if current_price <= 0:
                    continue
                
                # 标记是否触发止损
                stop_loss_triggered = False
                stop_loss_reason = ""
                
                # ========== 原有逻辑：5%亏损止损 ==========
                if etf in g.buy_price:
                    loss_ratio = (g.buy_price[etf] - current_price) / g.buy_price[etf]
                    if loss_ratio >= g.stop_loss_ratio:
                        stop_loss_triggered = True
                        stop_loss_reason = f"亏损{loss_ratio:.2%}（超过{g.stop_loss_ratio*100}%）"
                
                # ========== 新增逻辑：跌破过去5天最低价止损 ==========
                if not stop_loss_triggered:  # 未触发亏损止损时，检查最低价止损
                    # 获取过去N天的价格数据（包含low字段）
                    history_data = get_history(
                        count=g.stop_loss_low_days,
                        frequency='1d',
                        field=['close', 'low'],
                        security_list=etf,
                        include=True
                    )
                    if not history_data.empty and len(history_data) >= g.stop_loss_low_days:
                        # 计算过去N天最低价
                        past_n_days_low = history_data['low'].min()
                        # 当前价格跌破过去N天最低价则止损
                        # print(f"{etf} 当前价 {current_price}， 过去5日最低价 {past_n_days_low}")
                        if current_price < past_n_days_low:
                            stop_loss_triggered = True
                            stop_loss_reason = f"跌破{g.stop_loss_low_days}天最低价（当前价{current_price:.4f} < {g.stop_loss_low_days}天最低价{past_n_days_low:.4f}）"
                # ========== 执行RSI卖出检查 ==========
                if not stop_loss_triggered:
                        rsi_period = calculate_rsi_period(context, etf)              
                        if check_rsi_sell(context, etf, rsi_period):
                            order_target(etf, 0)
                            if etf in g.buy_price:
                                del g.buy_price[etf]
                            # log.info(f"{context.current_dt.strftime('%Y-%m-%d %H:%M')} {etf} RSI下穿65 前值{previous_rsi:.2f} → 当前值{current_rsi:.2f}），执行卖出")
                # ========== 执行止损操作 ==========
                if stop_loss_triggered:
                    order_target(etf, 0)
                    stop_loss_triggered = False
                    if etf in g.buy_price:
                        del g.buy_price[etf]  # 删除买入价格记录
                    log.info(f"{context.current_dt.strftime('%Y-%m-%d %H:%M')} {etf} 触发止损：{stop_loss_reason}，执行卖出")
        except Exception as e:
            log.error(f"{etf} 止损检查异常：{str(e)}")
            continue



# 检查单个持仓标的RSI是否跌破卖出阈值
def check_rsi_sell(context, etf, rsi_period):
    history_data = get_history(
        count=60,  # 适配动态周期的数据源
        frequency='1d',  
        field=['close'],
        security_list=etf,
        fq = 'pre',
        include=False
    )
    latest_data = get_history(count=1,frequency='1m',field='close',security_list=etf,fq='pre',include=True)
        
    if latest_data is not None and not latest_data.empty:
        
        history_data = history_data.append(latest_data, ignore_index=False) # 修复未来函数问题
    
    # 传入动态计算的period计算RSI（使用ptrade原生API）
    # rsi_data = get_RSI(history_data['close'], max(6, rsi_period*g.rsi_rate))
    rsi_data = get_RSI(history_data['close'].values, max(6, rsi_period*g.rsi_rate))    
    # if rsi_data.empty or len(rsi_data) < 2:
    if rsi_data.size < 2:
        return False
    current_rsi = rsi_data[-1]
    previous_rsi = rsi_data[-2]  # 获取前一期RSI值
    
    # 卖出条件：前一期RSI≥65，当前期RSI<65（下穿65）
    log.info(f"{context.current_dt.strftime('%Y-%m-%d %H:%M')} {etf} RSI{max(6, rsi_period*g.rsi_rate)}下穿{g.sell_threshold} 前值{previous_rsi:.2f} → 当前值{current_rsi:.2f}")
    if previous_rsi >= g.sell_threshold and current_rsi < g.sell_threshold:
        return True
    return False

# ========== 新增：卖出信号检查函数 ==========
def sell_target(context):
    for etf in g.etf_list:
        try:
            position = get_position(etf)
            if position and position.amount > 0:
                # 动态计算当前ETF的RSI周期
                rsi_period = calculate_rsi_period(context, etf)              
                if check_rsi_sell(context, None, etf, rsi_period):
                    order_target(etf, 0)
                    if etf in g.buy_price:
                        del g.buy_price[etf]
                    log.info(f"{context.current_dt.strftime('%Y-%m-%d %H:%M')} {etf} RSI下穿65 前值{previous_rsi:.2f} → 当前值{current_rsi:.2f}），执行卖出")
        except Exception as e:
            log.error(f"{etf} 卖出检查异常：{str(e)}")
            continue


# ========== 新增：下午2:30执行的核心交易逻辑函数 ==========
def execute_afternoon_trading(context, data):
    """
    下午2:30执行的交易逻辑（第三步到第五步）
    """
    # 标记任务已执行，避免重复运行
    if g.executed_afternoon_task:
        return
    g.executed_afternoon_task = True
    
    # ========== 第三步：持仓标的卖出检查（使用动态RSI周期） ==========
    sell_target(context)
    
    # ========== 第四步：买入候选筛选（使用动态RSI周期） ==========
    candidate_etfs = []
    for etf in g.etf_list:
        try:
            # 跳过已有持仓的ETF
            position = get_position(etf)
            if position and position.amount > 0:
                continue
            
            # 动态计算当前ETF的RSI周期
            rsi_period = calculate_rsi_period(context, etf)
            
            # 获取足够的日线数据（动态RSI周期+动量周期）
            history_data = get_history(
                count=60,  # 适配动态周期的数据源
                frequency='1d',  
                field=['close'],
                security_list=etf,
                fq = 'pre',
                include=False
            )
            latest_data = get_history(count=1,frequency='1m',field='close',security_list=etf,fq='pre',include=True)
            if latest_data is not None and not latest_data.empty:  
                history_data = history_data.append(latest_data, ignore_index=False) # 修复未来函数问题
            
            # 传入动态period计算RSI（使用ptrade原生API）
            rsi_data = get_RSI(history_data['close'].values, max(6,rsi_period*g.rsi_rate))
            if rsi_data.empty or len(rsi_data) < 2:
                continue
            current_rsi = rsi_data.iloc[-1]
            previous_rsi = rsi_data.iloc[-2]
            
            # RSI上穿判断 + 最小动量过滤
            if (previous_rsi < g.buy_threshold and current_rsi >= g.buy_threshold):
                momentum = close_series.iloc[-1] / close_series.iloc[-g.momentum_period] - 1
                # 过滤弱动量标的
                if momentum >= g.min_momentum:
                    candidate_etfs.append((etf, momentum, rsi_period))
                    log.info(f"{context.current_dt.strftime('%Y-%m-%d %H:%M')} {etf} RSI上穿30，当前RSI {current_rsi}，上一个RSI {previous_rsi}，{g.momentum_period}期动量: {momentum:.4f}，动态RSI周期: {rsi_period}")
        except Exception as e:
            log.error(f"{etf} 买入筛选异常：{str(e)}")
            continue
    
    # ========== 第五步：买入执行（分散持仓+仓位控制） ==========
    if candidate_etfs:
        # 按动量降序排序
        candidate_etfs.sort(key=lambda x: x[1], reverse=True)
        
        # 计算当前持仓数量
        current_hold_count = 0
        try:
            current_hold_count = sum([1 for etf in g.etf_list if get_position(etf).amount > 0])
        except Exception as e:
            log.error(f"计算持仓数量异常：{str(e)}")
            current_hold_count = 0
        
        # 可买入数量 = 最大持仓数 - 当前持仓数
        buy_count = max(0, g.max_hold_etfs - current_hold_count)
        
        if buy_count > 0:
            # 选择前N个动量最大的标的
            target_etfs = candidate_etfs[:buy_count]
            # 计算每只标的可买入金额（均分可用现金，且单标的不超过最大仓位比例）
            try:
                total_cash = context.portfolio.cash
                max_single_cash = total_cash * g.max_position_ratio
                per_etf_cash = min(total_cash / buy_count, max_single_cash)
            except Exception as e:
                log.error(f"计算买入金额异常：{str(e)}")
                return  # 替换非法的continue，避免后续报错
            
            for target_etf, target_momentum, rsi_period in target_etfs:
                try:
                    # 确保无持仓才买入
                    if get_position(target_etf).amount == 0 and per_etf_cash > 100:  # 最小买入金额过滤
                        order_value(target_etf, per_etf_cash)
                        # 记录买入价格（用于止损）
                        g.buy_price[target_etf] = data[target_etf].close if target_etf in data else 0
                        log.info(f"{context.current_dt.strftime('%Y-%m-%d %H:%M')} 买入 {target_etf}，金额: {per_etf_cash:.2f}，动量: {target_momentum:.4f}，动态RSI周期: {rsi_period}")
                except Exception as e:
                    log.error(f"买入{target_etf}异常：{str(e)}")
                    continue

def handle_data(context, data):
    """
    核心处理函数：
    1. 每分钟执行止损检查
    2. 每日14:30执行卖出/买入逻辑（仅执行一次）
    """
    # 获取当前时间
    current_dt = context.current_dt
    current_hour = current_dt.hour
    current_minute = current_dt.minute
    
    # ========== 第二步：风控优先 - 止损检查（每分钟执行） ==========
    check_stop_loss(context, data)
    
    # ========== 第三步到第五步：下午2:30执行核心交易逻辑 ==========
    if current_hour == 14 and current_minute == 30:
        execute_afternoon_trading(context, data)