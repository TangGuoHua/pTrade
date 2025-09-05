import numpy as np          # 数值计算基础库
import pandas as pd         # 数据处理核心库
from datetime import datetime, timedelta  # Python标准日期时间库
from scipy import stats     # 科学计算统计模块
from sklearn.metrics import r2_score  # 机器学习评估指标

# 初始化策略
def initialize(context):
    # run_daily(context, ETF轮动策略, time='10:30')
    g.period_type = '1d'  # 交易周期
    g.lookback_window = 251 # 回看窗口
    g.target_num = 1  # 持仓数量
    g.stop_loss_pct = 0.07  # 止损比例
    g.score_threshold = 2.0 , #在下行趋势的时候加这个条件
    g.total_cash = 450000 # 实盘时固定资金，回测时全仓
    # 交易标的
    g.symbols = [
        '518880.SS',  # 黄金ETF
        '513100.SS',  # 纳指ETF
        '512100.SS',  # 中证1000ETF
        '513520.SS',  # 日经225ETF
        '513180.SS',  # 恒生科技指数ETF
        '512480.SS',  # 半导体ETF
        '510880.SS',  # 上证红利ETF
        '513030.SS',  # 华安德国ETF
        '159857.SZ',  # 光伏ETF
        # '515050.SS',  # 5G通信ETF
        '515880.SS',    ## 通信ETF 
        # '515980.SS',  ## AI ETF
        '162719.SZ',  # 石油LO
        '510300.SS',  # 沪深300
        '159851.SZ']  # 金融科技
        
    # 状态变量
    g.positions = {}  # 当前持仓
    g.last_buy_prices = {}  # 买入价格记录

    # 获取历史数据参数设置
    g.hist_fq = "dypre"

    # # 设置基准和股票池
    # set_benchmark(g.symbols[0])
    # set_universe(g.symbols)
    
    if not is_trade():        
    # # 设置佣金费率
        set_commission(0.0005)  # 设置佣金为万分之一
    
    # # 设置滑点
        set_slippage(0.0002)  # 设置滑点为万分之二 0.0002
    # # 设置成交数量限制模式
        set_limit_mode(limit_mode='UNLIMITED')
    # # 设置策略的可交易时间
    # g.trade_flag = True
    print("pd.__version__: {}".format(pd.__version__)) 
    
def handle_data(context, data):
    # log.info("###crrent returns = %s "%(context.portfolio.portfolio_value))
    # 获取当前时间
    current_time = context.blotter.current_dt.strftime('%H:%M')
    # 获取当前持仓
    holdings = get_positions()
    holding_list = [holdings[p].sid for p in holdings if holdings[p].amount > 0]
    # 检查止损
    stop_loss_list = check_stop_loss(context, holdings, data)
    current_hold_size = len(holding_list)
    for symbol in stop_loss_list:
        order_target_value(symbol, 0) # 清空持仓
        print('止损卖出{}'.format(symbol))
        current_hold_size = current_hold_size - 1
        holding_list.remove(symbol) # 从holding_list中移除止损的股票
        if symbol in g.last_buy_prices:
            del g.last_buy_prices[symbol]
            
    # 在指定时间点执行交易
    # log.info("###current_time=%s"%current_time)
    # if current_time in ['09:50']: 
    if current_time in ['14:50']: 
        
        sh_result = is_maX_above_maY('000001.SS', 10, 20)
        # log.info(f"相对关系 {sh_result}")
        if sh_result is not None and sh_result==False:
            print("上证指数10日均线在20日均线下方，暂停交易")
            return
        
        market_data = get_history(
            count=g.lookback_window,       # 回溯周期（全局变量）
            frequency=g.period_type,       # 数据频率（日/分钟）
            field='close',                 # 获取收盘价
            security_list=g.symbols,       # 监控标的池
            fq=g.hist_fq,                      # 前复权处理
            include=False                  # 修复未来函数问题
        )
        #print("market_data:%s" % market_data)
        if market_data is None or market_data.empty:
            return
        
        # log.info("###获取最近1分钟数据并替换最后一条")        
        # 获取最近1分钟数据并替换最后一条
        latest_data = get_history(count=1,frequency='1m',field='close',security_list=g.symbols,fq=g.hist_fq,include=True)
        
        if latest_data is not None and not latest_data.empty:
            # market_data.iloc[-1] = latest_data.iloc[-1]
            market_data = market_data.append(latest_data, ignore_index=False) # 修复未来函数问题
            # print("market_data 替换后:%s" % market_data)
            target_list = calculate_etf_scores(market_data)[:g.target_num]
            current_hold_size = len(holding_list)    
        
        # log.info("###执行卖出操作")
        # 执行卖出操作
        for symbol in holding_list:
            if symbol not in g.symbols: continue # 标的隔离
            if symbol not in target_list:
                order_target_value(symbol, 0) # 清空持仓
                print('卖出{}'.format(symbol))
                current_hold_size = current_hold_size - 1
                if symbol in g.last_buy_prices:
                    del g.last_buy_prices[symbol]
                
        # log.info("###执行买入操作")       
        # 执行买入操作
        if current_hold_size < g.target_num:
            real_cash = context.portfolio.cash
            if is_trade():
                account = g.total_cash if 0 < g.total_cash <= real_cash else real_cash
            else:
                account = real_cash
            
            # log.info("account = %s" %account)            
            per_cash = account / (g.target_num - current_hold_size) * 0.999
            for symbol in target_list:
                if symbol not in holding_list:
                    current_price = data[symbol]['close']
                    if current_price > 0:     
                        # 应用风险管理
                        risk_factor = risk_management(market_data[market_data['code'].isin([symbol])]['close'], symbol)
                        target_value = per_cash * risk_factor
                        if target_value > 0:
                            order_target_value(symbol, target_value) # 按金额买入
                            print('买入{}，金额：{:.2f}'.format(symbol, target_value))
                            g.last_buy_prices[symbol] = current_price



def is_maX_above_maY(security, x_days, y_days):
    """
    判断指定标的的X日均线是否在Y日均线上方（结合get_trading_day接口规范）
    """
    try:
        # 1. 参数校验
        if not (isinstance(x_days, int) and isinstance(y_days, int) and x_days > 0 and y_days > 0):
            log.error(f"参数错误：均线天数必须为正整数（X={x_days}, Y={y_days}）")
            return None
        
        max_days = max(x_days, y_days)
        # log.info(f"计算 {security} 的 {x_days}日/{y_days}日均线关系")
        
        # 2. 交易日期处理（严格遵循get_trading_day接口文档）
        # 根据文档：day=0表示当前交易日（非交易日则返回下一交易日）
        current_trading_day = get_trading_day(0)
        # 转换为get_price接口要求的'YYYYMMDD'格式字符串
        end_date_str = current_trading_day.strftime('%Y%m%d')
        # log.info(f"当前交易日：{current_trading_day}，转换为接口日期格式：{end_date_str}")
        
        # 3. 调用get_price接口（参数组合符合文档要求）
        hist_data = get_price(
            security=security,
            end_date=end_date_str,
            frequency='1d',
            fields=['close'],
            count=max_days,  # 与end_date组合使用，不传入start_date
            fq=g.hist_fq
        )
        
        # 4. 数据有效性检查
        if hist_data is None:
            log.warning(f"接口返回空数据，可能标的代码错误或非交易日")
            return None
            
        if not isinstance(hist_data, pd.DataFrame) or hist_data.empty:
            log.warning(f"返回数据格式错误或为空")
            return None
            
        if 'close' not in hist_data.columns:
            log.warning(f"返回数据不包含'close'字段")
            return None
            
        # 文档说明get_price返回数据不包括当天，因此实际获取的是end_date前的count个交易日数据
        if len(hist_data) < max_days:
            log.warning(f"数据量不足：需要{max_days}个交易日，实际获取{len(hist_data)}个")
            return None
        
        # 5. 计算均线并判断
        ma_x = hist_data['close'].iloc[-x_days:].mean()
        ma_y = hist_data['close'].iloc[-y_days:].mean()
        
        log.info(f"{x_days}日均线：{ma_x:.2f}，{y_days}日均线：{ma_y:.2f}")
        return ma_x > ma_y
    
    except TypeError as e:
        log.error(f"类型错误：{str(e)}")
        return None
    except KeyError as e:
        log.error(f"数据字段错误：{str(e)}")
        return None
    except Exception as e:
        log.error(f"均线判断失败：{str(e)}")
        return None

        
def after_trading_end(context, data):
    # try catch the exception
    # to avoid the error stop the whole strategy
    try:
        log.info("盘后打印上次买入价")
        # print_holding_details(context, data)
        holdings = get_positions()
        holding_list =  [p for p in holdings if holdings[p].amount > 0]
        
        if not holding_list:
            log.info("当前无持仓\n")
            return
        
        # print  each position details if amount > 0
        for symbol in holding_list:
            position = holdings[symbol]
            if position.amount > 0:
                log.info("持仓: %s, 数量: %d, 买入价： %.2f, 成本价: %.2f, 当前价: %.2f, 盈亏: %.2f%% " %
                        (position.sid, position.amount,g.last_buy_prices[position.sid], position.cost_basis, position.last_sale_price, (position.last_sale_price - g.last_buy_prices[position.sid]) * 100/ (g.last_buy_prices[position.sid] if g.last_buy_prices[position.sid] != 0 else 1) ) )
    # add empty line
        log.info(" ")
    except Exception as e:
        log.error("after_trading_end error: %s" % str(e))
        return
    
def calculate_etf_scores(market_data, lookback_window=63):
    """计算ETF评分"""
    # log.info(f"lookback_window={lookback_window}")
    score_list = []
    valid_etfs = []
    
    for symbol in g.symbols:
        try:
            #df = market_data.query('code in ["'+symbol+'"]')
            df = market_data[market_data['code'].isin([symbol])]
            # Check for NaN values in close prices
            if df['close'].isna().any():
                print("警告：{} 包含NaN值，跳过处理".format(symbol))
                continue
            if len(df) < lookback_window * 0.8:
                continue
                

            # 使用实际时间序列（处理非交易日问题）
            x = pd.to_datetime(df.index).strftime('%Y%m%d').astype(int).values.reshape(-1, 1)
            y = np.log(np.maximum(df['close'].values, 1e-10))  # 防止对数为0
            # log.info("x={}, y={}".format(x, y) )
            
            # 稳健线性回归（处理异常值）
            slope, intercept, r_value, _, _ = stats.linregress(x.flatten(), y)
            # log.info("slope={}, intercept={}, r_value={}".format(slope, intercept, r_value))
            
            # 年化收益率计算优化
            daily_growth = np.exp(slope) - 1
            # log.info("daily_growth={}".format(daily_growth))
            annualized_returns = (1 + daily_growth) ** 252 - 1
            # log.info("annualized_returns={}".format(annualized_returns))
            
            # 改进的R2计算
            y_pred = slope * x.flatten() + intercept
            r_squared = r2_score(y, y_pred)
            # log.info("y_pred={}, r_squared".format(y_pred, r_squared))
            
            # 动态权重调整（波动率调整）
            volatility = np.std(np.diff(y)) * np.sqrt(252)
            risk_adjusted_return = annualized_returns / (volatility + 1e-6)  # 防止除零
            # log.info("volatility={}, risk_adjusted_return={}".format(volatility,risk_adjusted_return))
            
            # 综合评分（加入动量因子）
            momentum = df['close'].pct_change(21).iloc[-1]  # 1个月动量
            score = (risk_adjusted_return * r_squared) + (0.3 * momentum)
            # log.info("symbol={}, momentum={},score={}".format(symbol,momentum,score))
            #if symbol=='159509.SZ':
            #    print("算分:",x,y,slope, intercept, r_value)
            
            valid_etfs.append(symbol)
            score_list.append(float(score))
            

        except Exception as e:
            print("计算{}评分时出错: {}".format(symbol, str(e)))
            continue
            
    # 创建评分DataFrame  
    # log.info("score_list len = {}".format(len(score_list)))        
    df_score = pd.DataFrame(index=valid_etfs, data={'score': score_list})

    
    # 分数标准化
    df_score['score'] = (df_score['score'] - df_score['score'].mean()) / df_score['score'].std()
    df_score = df_score[df_score['score'] > g.score_threshold]
    df_score = df_score.sort_values(by='score', ascending=False)
    return list(df_score.index) 

def check_stop_loss(context, holdings, data):
    """检查止损条件"""
    # log.info("检查止损条件")
    stop_loss_list = []
    for symbol, position in holdings.items():
        # log.info(f"当前symbol = {symbol};  当前持仓{position}")
        if position.amount > 0 and position.sid in g.last_buy_prices:
            current_price = data[symbol]['close']
            # log.info(f"当前价格 = {current_price}")
            if current_price > 0:
                buy_price = g.last_buy_prices[position.sid] # 修复止损问题
                # log.info(f"上次买入价格 {buy_price}, 止损比例 {g.stop_loss_pct}, 止损价格：{buy_price * (1 - g.stop_loss_pct)} ")
                if current_price < buy_price * (1 - g.stop_loss_pct):
                    stop_loss_list.append(position.sid)
                    print('{}触发止损，买入价：{:.2f}，当前价：{:.2f}，止损比例：{:.2%}'.format(
                        position.sid, buy_price, current_price, g.stop_loss_pct))
            else:
                print('警告：无法获取{}的市场数据'.format(position.sid))
    return stop_loss_list

def risk_management(market_data, symbol):
    """风险管理"""
    price_series = df = market_data[-21:]
    if len(price_series) < 20:
        return 1.0 # 数据不足时不调整
    # 计算波动率
    returns = price_series.pct_change().dropna()
    current_vol = returns[-20:].std()
    hist_vol = returns.std()
    # 波动率突破检查
    vol_ratio = current_vol / hist_vol
    if vol_ratio > 3.0:
        print("波动率突破 {}: {:.2f}".format(symbol, vol_ratio))
        return 0.5
    if vol_ratio > 2.0:
        print("波动率突破 {}: {:.2f}".format(symbol, vol_ratio))
        return 0.8

    # 最大回撤检查
    rolling_max = price_series.expanding().max()
    drawdown = (price_series - rolling_max) / rolling_max
    if drawdown.min() < -0.1:
        print("触发最大回撤限制 {}".format(symbol))
        return 0
    return 1.0