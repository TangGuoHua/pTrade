import numpy as np          # 数值计算基础库
import pandas as pd         # 数据处理核心库
from datetime import datetime, timedelta  # Python标准日期时间库
from scipy import stats     # 科学计算统计模块
from sklearn.metrics import r2_score  # 机器学习评估指标
import time
# 初始化策略
def initialize(context):
    # run_daily(context, ETF轮动策略, time='10:30')
    g.period_type = '1d'  # 交易周期
    g.lookback_window = 251 # 回看窗口
    g.target_num = 1  # 持仓数量
    g.stop_loss_pct = 0.05  # 止损比例
    g.score_threshold = 1 , #在下行趋势的时候加这个条件
    g.total_cash = 300000 # 实盘时固定资金，回测时全仓
    # 宽基标的池
    g.broadIndexFund =[
        '518880.SS',  # 黄金ETF  , 商品          
        '510300.SS',  # 沪深300 
        '513520.SS',  # 日经225ETF
        '513100.SS',  # 纳指ETF
        '513030.SS'  # 华安德国ETF
        
        ]
    
    # 行业轮动标的
    g.a_industry = [
        '513180.SS',  # 恒生科技指数ETF
        '510880.SS',  # 上证红利ETF
        '512480.SS',  # 半导体ETF        
        '159857.SZ',  # 光伏ETF
        '515880.SS',  # 通信ETF 
        '162719.SZ',  # 石油LO     
        '159851.SZ'  # 金融科技 
    ]
    
    g.allFunds = g.broadIndexFund+g.a_industry
    g.symbols = g.broadIndexFund
    # 状态变量
    g.positions = {}  # 当前持仓
    g.last_buy_prices = {}  # 买入价格记录
    g.stop_loss_list = []
    # # 设置基准和股票池
    # set_benchmark(g.symbols[0])
    # set_universe(g.symbols)
    
    g.sell_list = [] # 记录当日卖出单的 order_id
    g.buy_list = []  # 记录当日买入单的 order_id 
    
    if not is_trade():        
    # # 设置佣金费率
        set_commission(0.0001)  # 设置佣金为万分之一
    
    # # 设置滑点
        set_slippage(0.0002)  # 设置滑点为万分之二 0.0002
    # # 设置成交数量限制模式
        set_limit_mode(limit_mode='UNLIMITED')

    print("pd.__version__: {}".format(pd.__version__)) 

   
def handle_data(context, data):
    # log.info("###crrent returns = %s "%(context.portfolio.portfolio_value))
    # 获取当前时间
    current_time = context.blotter.current_dt.strftime('%H:%M')
    
    # 检查止损
    stop_loss_list = check_stop_loss(context,  data)
    if stop_loss_list:
        print(f"止损标的列表: {stop_loss_list}")
        clear_holdings(stop_loss_list, data)
  
    # 在指定时间点执行交易
    # log.info("###current_time=%s"%current_time)      
    if current_time in ['14:20']: 
        
        prepare_symbols()
        
        market_data = getPrices()
        if market_data is not None:
            target_list = calculate_etf_scores(market_data)[:g.target_num]
            # 执行卖出操作            
            to_be_clear_symbols = get_symbols_tobe_clear(target_list)             
            if to_be_clear_symbols:
                clear_holdings(to_be_clear_symbols, data)
            
            # if is_trade():
                # time.sleep(6)  # 等待6秒，确保数据更新      
                          
    if current_time in ['14:23']:
        log.info(f"检查当日卖出单是否成交")
        if len(g.sell_list) > 0:
            
            for order_id in g.sell_list:
                ord = get_order(order_id)
                if len(ord)> 0 and ord[0].status == '8':
                    log.info(f"卖出单 {order_id}, 已经成交")                
                else:
                    log.info(f"卖出订单 {ord} 没有成交")                
        else:
            log.info("今天没有卖出单")  
            
    if current_time in ["14:29"]:
        log.info("执行买入操作") 
        market_data = getPrices()
        if market_data is not None:
            target_list = calculate_etf_scores(market_data)[:g.target_num]        
            buy_stocks(context, data, market_data, target_list)

    if current_time in ['14:33']:    
        if len(g.buy_list) > 0:            
            for order_id in g.buy_list:
                ord = get_order(order_id)
                if len(ord) > 0 and ord[0].status == '8':
                    log.info(f"买入单 {order_id}, 已经成交")                
                else:
                    log.info(f"买入订单 {ord} 没有成交")
        else:
            log.info("今天没有买入单") 
            
def buy_stocks(context, data, market_data, target_list):
    holding_list = get_current_positions_list()
    current_hold_size =  len(holding_list)
    if current_hold_size < g.target_num:
        if is_trade():
            account = g.total_cash
        else:
            account = context.portfolio.cash
            # account = g.total_cash
        log.info("account = %s" %account)  
        try:
            per_cash = account / (g.target_num - current_hold_size) * 0.999
            for symbol in target_list:
                if symbol in g.stop_loss_list:
                    print("跳过止损标的 {}".format(symbol))
                    continue
                if symbol not in holding_list:
                    current_price = data[symbol]['price']
                    if current_price > 0:     
                        # 应用风险管理
                        risk_factor = risk_management(market_data[market_data['code'].isin([symbol])]['close'], symbol)
                        target_value = per_cash * risk_factor
                        if target_value > 0:
                            limit_price = current_price
                            if is_trade():
                                limit_price = get_snapshot(symbol)[symbol]['last_px']
                                log.info(f"买入标的 {symbol} 时候的行情快照价 {limit_price}。")
                            limit_price = limit_price * 1.005  # 上浮0.5%买入
                            log.info(f"以上浮0.5%的限价 {limit_price} 买入标的 {symbol}")
                            order_id = order_target_value(symbol, target_value, limit_price) # 按金额买入
                            if order_id is None:
                                log.error("买入失败！")
                                raise ValueError(f"调用 order_target_value 买入时，未能获取 order_id")
                            else:
                                g.buy_list.append(order_id)
                                buyInfo = '开仓买入{}，金额：{:.2f}'.format(symbol, target_value)
                                log.info(buyInfo)
                                g.last_buy_prices[symbol] = limit_price
                                if is_trade():
                                    send_email('15228207@qq.com', ['15228207@qq.com'], 'wrmmhpwuutdfcbcd', info=buyInfo,  subject="pTrade通知，开仓买入！！！")
        except Exception as e:
            log.error("买入失败: %s" % str(e))
            if is_trade():
                send_email('15228207@qq.com', ['15228207@qq.com'], 'wrmmhpwuutdfcbcd', info=f"买入标的{str(e)} 失败！！！",  subject="pTrade通知，策略执行失败！！！")
            raise e               
def get_current_positions_list():
    try:
        holdings = get_positions()
        holding_list = [holdings[p].sid for p in holdings if holdings[p].amount > 0 and holdings[p].sid in g.allFunds]
        return holding_list
    except Exception as e:
        log.error("获取当前持仓列表失败: %s" % str(e))
        raise e
    
def get_current_hold_size():
    holding_list =get_current_positions_list()
    return len(holding_list)

# 获取需要清仓的标的列表
def get_symbols_tobe_clear(target_list):
    """获取需要清仓的标的列表"""
    try:
        
        tobe_clear = []
        holding_list = get_current_positions_list()
        for symbol in holding_list:
            if symbol not in g.symbols: continue # 标的隔离
            if symbol not in target_list: # 标的隔离
                tobe_clear.append(symbol)
        return tobe_clear
    except Exception as e:
        log.error("获取需要清仓的标的列表失败: %s" % str(e))
        if is_trade():
            send_email('15228207@qq.com', ['15228207@qq.com'], 'wrmmhpwuutdfcbcd', info=f"获取清仓标的 {str(e)} 失败！！！",  subject="pTrade通知，获取清仓标的失败！！！")
        
        return []
        
 # 清空持仓函数
 
 
# 清仓指定标的
def clear_holdings(stock_list_to_cleared, data):
    
    try:
        for symbol in stock_list_to_cleared:
            #  标的隔离
            if symbol not in g.allFunds:
                continue
            current_price = data[symbol]['price']
            log.info(f"current_price = {current_price} in clear_holdings()")
            limit_price = current_price
            if is_trade():
                limit_price = get_snapshot(symbol)[symbol]['last_px']
                log.info(f"交易中的行情快照价格={limit_price}")
            limit_price = limit_price * (1-0.005)  # 下调0.5%卖出            
            log.info(f"以调整限价：{limit_price}, 清仓标的 {symbol}")
            order_id = order_target_value(symbol, 0, limit_price) # 清空持仓
            if order_id is None:
                log.error("清仓失败！")
                raise ValueError(f"调用 order_target_value 清仓时，未能获取 order_id")
            else:
                g.sell_list.append(order_id)
                g.stop_loss_list.append(symbol) # 记录止损标的
                sellInfo = f"卖出{symbol} 订单号 {order_id}\n"
                log.info(sellInfo)
                if symbol in g.last_buy_prices: # 删除买入价记录
                    del g.last_buy_prices[symbol]
                if is_trade():
                    send_email('15228207@qq.com', ['15228207@qq.com'], 'wrmmhpwuutdfcbcd', info=sellInfo,  subject=f"pTrade通知，清仓{symbol}！")                
    except Exception as e:
        log.error("清空持仓失败: %s" % str(e))
        if is_trade():
            send_email('15228207@qq.com', ['15228207@qq.com'], 'wrmmhpwuutdfcbcd', info=f"清仓标的 {str(e)} 失败！！！",  subject="pTrade通知，策略执行失败！！！")
        raise e
        
def getPrices():
    try:
        market_data = get_history(
                count=g.lookback_window,       # 回溯周期（全局变量）
                frequency=g.period_type,       # 数据频率（日/分钟）
                field='close',                 # 获取收盘价
                security_list=g.symbols,       # 监控标的池
                fq='pre',                      # 前复权处理
                include=False                  # 修复未来函数问题
            )
            #print("market_data:%s" % market_data)

        # log.info("###获取最近1分钟数据并替换最后一条")        
        # 获取最近1分钟数据并替换最后一条
        latest_data = get_history(count=1,frequency='1m',field='close',security_list=g.symbols,fq='pre',include=True)
        
        if latest_data is not None and not latest_data.empty:
            # market_data.iloc[-1] = latest_data.iloc[-1]
            market_data = market_data.append(latest_data, ignore_index=False) # 修复未来函数问题
            # print("market_data 替换后:%s" % market_data)
            return market_data
        else:
            print("###未获取到最新1分钟数据")
            #TODO: 记录日志,报警等
        if market_data is None or market_data.empty:
            return None
    except Exception as e:
        log.error("获取价格数据失败: %s" % str(e))
        raise e
    return None
    
def prepare_symbols():
    sh_result = is_maX_above_maY('000001.SS', 10, 20)
    # log.info(f"相对关系 {sh_result}")
    if sh_result is not None and sh_result==False:
        print("弱势行情10<20, 控制风险，只操作宽基")
        # 只保留宽基标的池
        g.symbols = g.broadIndexFund
        g.score_threshold = 1  # 在下行趋势的时候加这个条件
        g.stop_loss_pct=0.05
    if is_maX_above_maY('000001.SS', 10, 20) and is_maX_above_maY('000001.SS', 20, 30)and is_maX_above_maY('000001.SS', 5, 10):
        print("强势行情 5>10>20>30，继续交易")
        # 合并行业标的池
        g.symbols = g.symbols + g.a_industry
        g.score_threshold = 2  # 在上行趋势的时候放宽条件
        # 去除重复标的
        # g.stop_loss_pct=0.02
        g.symbols = list(set(g.symbols))
 
    print(f"当前标的池: {g.symbols}, g.stop_loss_pct={g.stop_loss_pct}, g.score_threshold={g.score_threshold}") 
    
    
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
            fq='pre'
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
        print("盘后清空止损列表")
        g.stop_loss_list = []
        g.sell_list=[]
        g.buy_list = []
        
        log.info("盘后打印持仓信息")
        # print_holding_details(context, data)
        holdings = get_positions()
        print(f"所有持仓 ： {holdings}")
        holding_list =  [holdings[p].sid for p in holdings if holdings[p].amount > 0 and holdings[p].sid in g.allFunds]
        print(holding_list)
        print(len(holding_list))
        if not holding_list:
            log.info("当前无持仓\n")
            return
        
        # print  each position details if amount > 0
        for symbol in holding_list:
            if symbol not in g.symbols: continue # 标的隔离
            position = holdings[symbol]
            if position.amount > 0:
                log.info("持仓: %s, 数量: %d, 买入价： %.4f, 成本价: %.4f, 当前价: %.4f, 盈亏: %.4f%% " %
                        (position.sid, position.amount,g.last_buy_prices[position.sid], position.cost_basis, position.last_sale_price, (position.last_sale_price - g.last_buy_prices[position.sid]) * 100/ (g.last_buy_prices[position.sid] if g.last_buy_prices[position.sid] != 0 else 1) ) )
    # add empty line
        log.info(" ")
    except Exception as e:
        log.error("after_trading_end error: %s" % str(e))
        return
    
def calculate_etf_scores(market_data, lookback_window=63):
    """计算ETF评分"""
    log.info(f"lookback_window={lookback_window}")
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
    df_score = df_score.sort_values(by='score', ascending=False)
    
    print("优化后评分结果:",df_score.head(40))
    if is_trade():
        send_email('15228207@qq.com', ['15228207@qq.com'], 'wrmmhpwuutdfcbcd', info=f"今日评分 \n {df_score}！\n",  subject="pTrade通知，今日评分！！！")
        
    df_score = df_score[df_score['score'] > g.score_threshold]
    log.info(f"分数超过 {g.score_threshold} 的标的数量是 {len(df_score)}")
    return list(df_score.index) 

def check_stop_loss(context, data):
    """检查止损条件"""
    # 获取当前持仓
    holdings = get_positions()
    
    holding_list = [holdings[p].sid for p in holdings if holdings[p].amount > 0 and holdings[p].sid in g.allFunds]
    # print(f"当前持仓列表: {holding_list}")
    # log.info("检查止损条件")
    stop_loss_list = []
    for symbol, position in holdings.items():
        # log.info(f"当前symbol = {symbol};  当前持仓{position}")
        if position.amount > 0 and position.sid in g.last_buy_prices:
            current_price = data[symbol]['close']
            # log.info(f"当前价格 = {current_price}")
            if current_price > 0:
                buy_price = g.last_buy_prices[position.sid] # 修复止损问题
                # log.info(f"上次买入价格 {buy_price}, 止损比例 {g.stop_loss_pct}, 止損價格：{buy_price * (1 - g.stop_loss_pct)} ")
                if (current_price < buy_price * (1 - g.stop_loss_pct)) :
                    stop_loss_list.append(position.sid)
                    print('{}触发止损，买入价：{:.2f}，当前价：{:.2f}，止损比例：{:.2%}'.format(
                        position.sid, buy_price, current_price, g.stop_loss_pct))
                if position.sid not in g.symbols:
                    stop_loss_list.append(position.sid)
                    print("{position.sid} 被清理出持仓池 {g.symbols} 止损")
            else:
                print('警告：无法获取{}的市场数据'.format(position.sid))
    return stop_loss_list
def risk_management(market_data, symbol):
    """
    风险管理：计算近20天波动率/过去200天波动率的比值，结合波动率突破、最大回撤做仓位调整
    :param market_data: get_history返回的DataFrame，含datetime索引、code、close列
    :param symbol: 标的代码（如162719.SZ）
    :return: 仓位调整系数（0-1.0）
    """
    print("开始执行风险管理计算")
    # 核心适配：提取收盘价列，重置索引避免时间索引切片问题，仅保留数值做计算
    close_series = market_data.copy()
    # 近20天收盘价（取最后20条，若不足20天则返回默认系数）
    price_20d = close_series.tail(20)
    # 过去200天收盘价（取倒数201到倒数21条，和近20天无重叠，纯历史区间）
    price_200d = close_series.iloc[-200:-20] if len(close_series)>=201 else close_series.head(len(close_series)-20)

    # 数据有效性校验：20天和200天数据均需至少10条，否则不调整仓位
    if len(price_20d) < 10 or len(price_200d) < 10:
        log.warning(f"标的{symbol}数据不足，20天数据量：{len(price_20d)}，200天数据量：{len(price_200d)}")
        return 1.0

    # 计算日收益率（涨跌幅），dropna删除空值（第一条无涨跌幅）
    ret_20d = price_20d.pct_change(fill_method=None).dropna()
    ret_200d = price_200d.pct_change(fill_method=None).dropna()

    # 计算波动率：日收益率的标准差（金融中波动率的标准计算方式）
    vol_20d = ret_20d.std()  # 近20天波动率
    vol_200d = ret_200d.std()# 过去200天波动率

    # 避免除零错误：若历史波动率为0，返回默认系数
    if vol_200d == 0:
        log.warning(f"标的{symbol}过去200天波动率为0，无波动")
        return 1.0

    # 计算波动率比值：近20天 / 过去200天
    vol_ratio = vol_20d / vol_200d
    print(f"symbol={symbol}, 近20天波动率={vol_20d:.6f}, 过去200天波动率={vol_200d:.6f}, 波动率比值={vol_ratio:.4f}")

    # 波动率突破检查：根据比值调整仓位
    if vol_ratio > 3.0:
        print(f"波动率突破3倍阈值 {symbol}: {vol_ratio:.2f}")
        return 0.5
    if vol_ratio > 1.5:
        print(f"波动率突破1.5倍阈值 {symbol}: {vol_ratio:.2f}")
        return 0.8

    # 最大回撤检查：基于近20天收盘价，回撤超10%则清仓
    rolling_max = close_series.tail(10).expanding().max()  # 20天内的滚动最高价
    drawdown = (close_series.tail(10) - rolling_max) / rolling_max  # 累计回撤
    max_drawdown = drawdown.min()
    if max_drawdown < -0.1:
        print(f"触发最大回撤10%限制 {symbol}，最大回撤：{max_drawdown:.2%}")
        return 0.0

    # 无异常则返回满仓系数
    return 1.0
    
 