"""
策略名称：
小市值日线交易策略（带资金限制）
运行周期:
日线
策略流程：
盘前将中小板综成分股中st、停牌、退市的股票过滤得到股票池
盘中换仓，始终持有当日流通市值最小的股票（涨停标的不换仓）。
优化点：
1. 只卖出本策略买入的股票
2. 增加 g.allowCash 控制最大可用资金
注意事项：
策略中调用的order_target_value接口的使用有场景限制，回测可以正常使用，交易谨慎使用。
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
    # 策略可使用的最大资金（单位：元）
    g.allowCash = 100000  # 策略可用资金上限
    
    if not is_trade():
        # 设置佣金费率
        set_commission(0.0005)  # 设置佣金为万分之一
        # 设置滑点
        set_slippage(0.0002)  # 设置滑点为万分之二
        # 设置成交数量限制模式
        set_limit_mode(limit_mode='UNLIMITED')
    
    # 跟踪本策略买入的股票集合
    g.strategy_stocks = set()


# 盘前处理
def before_trading_start(context, data):
    g.pre_position_list = list(get_positions().keys())
    g.stock_list = get_index_stocks(g.index)
    # 指数成分股按昨日收盘时的流通市值进行从小到大排序，截取市值最小的100个标的进行股票状态筛选
    df = get_fundamentals(
        g.stock_list, "valuation",
        fields=["total_value", "a_floats", "float_value"],
        date=context.previous_date
    ).sort_values(by="float_value").head(100)
    stock_list_tmp = df.index.tolist()
    # 将ST、停牌、退市三种状态的股票剔除当日的股票池
    stock_list_tmp = filter_stock_by_status(
        stock_list_tmp, filter_type=["ST", "HALT", "DELISTING"], query_date=None
    )
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
    # 卖出：只卖出本策略买入且不在买入列表中的股票
    for stock in list(g.strategy_stocks):
        if stock in context.portfolio.positions and stock not in buy_stocks:
            order_target_value(stock, 0)
            log.info("sell:%s" % stock)
            g.strategy_stocks.remove(stock)
    
    # 计算当前持仓总成本
    total_position_value = sum(
        pos.amount * pos.cost_basis
        for pos in context.portfolio.positions.values()
    )
    log.info(f"当前持仓总成本: {total_position_value:.2f} 元")

    # 计算可用于买入的剩余资金额度
    remaining_cash = g.allowCash - total_position_value
    if remaining_cash <= 0:
        log.info("持仓已达最大资金限制，不再买入")
        return

    # 买入
    position_list = [pos.sid for pos in context.portfolio.positions.values() if pos.amount != 0]
    position_count = len(position_list)
    if g.buy_stock_count > position_count:
        value = remaining_cash / (g.buy_stock_count - position_count)
        for stock in buy_stocks:
            if stock not in context.portfolio.positions:
                order_target_value(stock, value)
                g.strategy_stocks.add(stock)
                log.info("buy:%s" % stock)


# 获取买入股票池（涨停股不参与换仓）
def get_trade_stocks(context, data):
    # 获取持仓中涨停的标的
    hold_up_limit_stock = [
        stock.replace("XSHG", "SS").replace("XSHE", "SZ")
        for stock in g.pre_position_list
        if check_limit(stock)[stock] == 1
    ]
    df = g.df
    if df.empty:
        return hold_up_limit_stock
    df["code"] = df.index
    # 计算当时最新的流通市值（昨日的流通股本*最新价）
    df["curr_float_value"] = df.apply(
        lambda x: x["a_floats"] * data[x["code"]].price, axis=1
    )
    df = df[df["curr_float_value"] != 0]
    # 按流通市值从小到大排序
    stocks = df.sort_values(by="curr_float_value").index.tolist()
    # 本次拟买入的数量
    count = g.buy_stock_count - len(hold_up_limit_stock)
    check_out_lists = stocks[:count]
    check_out_lists = check_out_lists + hold_up_limit_stock
    return check_out_lists