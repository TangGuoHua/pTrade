"""
策略名称：
小市值日线交易策略
运行周期:
日线
策略流程：
盘前将中小板综成分股中st、停牌、退市的股票过滤得到股票池
盘中换仓，始终持有当日流通市值最小的股票（涨停标的不换仓）。
注意事项：
策略中调用的order_target_value接口的使用有场景限制，回测可以正常使用，交易谨慎使用。
回测场景下撮合是引擎计算的，因此成交之后持仓信息的更新是瞬时的，但交易场景下信息的更新依赖于柜台数据
的返回，无法做到瞬时同步，可能造成重复下单。详细原因请看帮助文档。
"""


# 初始化
def initialize(context):
    # 设置基准指数
    set_benchmark("000300.XSHG")
    # 股票池对应指数代码
    g.index = "399101.XBHS"  # 中小板综
    # 持有股票数量
    g.buy_stock_count = 5
    # 筛选股票数量
    g.screen_stock_count = 10
    if not is_trade():
        # # 设置佣金费率
        set_commission(0.0005)  # 设置佣金为万分之一
    
        # 设置滑点
        set_slippage(0.0002)  # 设置滑点为万分之二 0.0002
        
        # 设置成交数量限制模式
        set_limit_mode(limit_mode='UNLIMITED')


# 盘前处理
def before_trading_start(context, data):
    g.pre_position_list = list(get_positions().keys())
    g.stock_list = get_index_stocks(g.index)
    # 指数成分股按昨日收盘时的流通市值进行从小到大排序，截取市值最小的100个标的进行股票状态筛选（考虑回测速度）
    df = get_fundamentals(g.stock_list, "valuation", fields=["total_value", "a_floats", "float_value"],
                          date=context.previous_date).sort_values(by="float_value").head(100)
    stock_list_tmp = df.index.tolist()
    # 将ST、停牌、退市三种状态的股票剔除当日的股票池
    stock_list_tmp = filter_stock_by_status(stock_list_tmp, filter_type=["ST", "HALT", "DELISTING"], query_date=None)
    # 保留状态筛选后的股票，并取其中流通市值最小的10个股票
    df = df[df.index.isin(stock_list_tmp)]
    g.df = df.head(g.screen_stock_count)


# 盘中处理
def handle_data(context, data):
    buy_stocks = get_trade_stocks(context, data)
    log.info("buy_stocks:%s" % buy_stocks)
    trade(context, buy_stocks)


# 交易函数
def trade(context, buy_stocks):
    # 卖出
    for stock in context.portfolio.positions:
        if stock not in buy_stocks:
            order_target_value(stock, 0)
            log.info("sell:%s" % stock)
    # 买入
    position_list = [position.sid for position in context.portfolio.positions.values()
                     if position.amount != 0]
    position_count = len(position_list)
    if g.buy_stock_count > position_count:
        value = context.portfolio.cash / (g.buy_stock_count - position_count)
        for stock in buy_stocks:
            if stock not in context.portfolio.positions:
                order_target_value(stock, value)


# 获取买入股票池（涨停股不参与换仓）
def get_trade_stocks(context, data):
    # 获取持仓中涨停的标的
    hold_up_limit_stock = [stock.replace("XSHG", "SS").replace("XSHE", "SZ") for stock in g.pre_position_list if check_limit(stock)[stock] == 1]
    df = g.df
    if df.empty:
        return hold_up_limit_stock
    df["code"] = df.index
    # 计算当时最新的流通市值（昨日的流通股本*最新价）
    df["curr_float_value"] = df.apply(lambda x: x["a_floats"] * data[x["code"]].price, axis=1)
    df = df[df["curr_float_value"] != 0]
    # 获取股票标的（按流通市值从小到大排序）        
    stocks = df.sort_values(by="curr_float_value").index.tolist()
    # 计算本次拟买入的数量（最大持仓量-持仓中涨停的数量（因为涨停股不卖））
    count = g.buy_stock_count - len(hold_up_limit_stock)
    check_out_lists = stocks[:count]
    check_out_lists = check_out_lists + hold_up_limit_stock
    return check_out_lists