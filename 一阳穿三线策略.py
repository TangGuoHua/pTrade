# 中小板指100成分股均线突破策略（修正版）
# from cmath import log


def initialize(context):
    # 初始化策略参数
    g.index_code = "399005.XSHE"  # 中小板指代码

    # 定义移动平均线周期参数
    g.short_period = 5  # 短期均线周期
    g.mid_period = 13  # 中期均线周期
    g.long_period = 21  # 长期均线周期
    g.volume_period = 15  # 成交量均线周期

    # 初始化持仓记录字典（关键修正：确保该变量被正确初始化）
    g.bought_stocks = {}  # 存储{股票代码: 买入日开盘价}

    # 标记是否已获取股票池
    g.stocks_initialized = False
    # 股票池变量
    g.stocks = []

    # 获取历史数据参数设置
    g.hist_fq = "dypre"

    run_daily(context, exec_strategy, time="09:31")
    run_daily(context, exec_strategy, time="14:55")


def before_trading_start(context, data):
    # 首先初始化股票池（修正：在第一个交易日获取成分股）
    if not g.stocks_initialized:
        g.stocks = get_index_stocks(g.index_code)  # 获取成分股列表
        set_universe(g.stocks)  # 设置股票池
        g.stocks_initialized = True
        log.info("股票池初始化完成，共{}只股票".format(len(g.stocks)))


def exec_strategy(context):
    # 首先初始化股票池（修正：在第一个交易日获取成分股）
    if not g.stocks_initialized:
        g.stocks = get_index_stocks(g.index_code)  # 获取成分股列表
        set_universe(g.stocks)  # 设置股票池
        g.stocks_initialized = True
        log.info("股票池初始化完成，共{}只股票".format(len(g.stocks)))

    # 执行卖出逻辑
    sell_strategy(context, None)

    # 执行买入逻辑
    buy_strategy(context, None)


def handle_data(context, data):
    pass


def buy_strategy(context, data):
    # 股票池未初始化则不执行买入
    if not g.stocks_initialized or len(g.stocks) == 0:
        return

    history_data = get_history(
        count=max(g.long_period, g.volume_period) + 20,  # 取最长周期确保数据完整
        frequency='1d',
        field=['open', 'close', 'volume'],
        security_list=g.stocks,
        fq=g.hist_fq,
        include=False,
        is_dict=False
    )


    # log.info("数据长度 {}".format(len(hist_data)))
    # print(hist_data.head(5))

    # 打印hist_data的类型
    # print(type(hist_data))

    # 遍历股票池中的每只股票
    for stock in g.stocks:
        # 跳过已买入的股票
        if stock in g.bought_stocks:
            continue
        try:
            # 获取所需历史数据

            stock_hd = history_data[history_data['code'].isin([stock])]
            # 确保有足够的历史数据
            # log.debug("Debug 1")
            if len(stock_hd) < g.long_period:
                continue

            #log.info("开始计算均线")
            # 价格均线计算（使用pandas的rolling方法更可靠）
            close_prices = stock_hd['close']
            ma5 = close_prices.rolling(window=g.short_period).mean().iloc[-1]  # 5天收盘价均线
            ma13 = close_prices.rolling(window=g.mid_period).mean().iloc[-1]  # 13天收盘价均线
            ma21 = close_prices.rolling(window=g.long_period).mean().iloc[-1]  # 21天收盘价均线
            #log.info("计算均线完毕")

            # 成交量均线计算
            volume_data = stock_hd['volume']
            vol_ma15 = volume_data.rolling(window=g.volume_period).mean().iloc[-1]  # 15天成交量均线

            # log.debug("Debug 2")

            # 获取今日开盘价和收盘价
            today_open = stock_hd['open'][-1]  # 今日开盘价（最后一条数据）
            today_close = stock_hd['close'][-1]  # 今日收盘价
            today_volume = stock_hd['volume'][-1]  # 今日成交量

            # log.debug("Debug 3")
            # 交易规则判断
            if (
                    # 开盘价低于所有均线
                    today_open < ma5 and
                    today_open < ma13 and
                    today_open < ma21 and
                    # 收盘价高于所有均线
                    today_close > ma5 and
                    today_close > ma13 and
                    today_close > ma21 and
                    # 成交量放大（大于15天均量的2倍）
                    today_volume > vol_ma15 * 2
            ):
                # 满足条件，买入股票（使用可用资金的90%）
                cash = context.portfolio.cash * 0.9
                if cash > 0:
                    # 计算可买数量（取整百股）
                    price = today_close  # 以收盘价作为委托价
                    amount = int(cash / price / 100) * 100
                    if amount >= 100:  # 最低买入单位100股
                        order(stock, amount, limit_price=price)
                        g.bought_stocks[stock] = today_open  # 记录买入日开盘价
                        log.info(f"买入 {stock}，价格：{price}，数量：{amount}，买入日开盘价：{today_open}")

        except Exception as e:
            # 异常处理，避免单只股票报错影响整体策略
            log.error(f"处理股票 {stock} 时出错：{str(e)}")
            # 记录错误详细信息
            log.error(f"错误详细信息：{e}")
            continue


def sell_strategy(context, data):
    # 如果没有持仓，直接返回
    if not hasattr(g, 'bought_stocks') or len(g.bought_stocks) == 0:
        return

    bought_stocks_list = list(g.bought_stocks.keys())

    if len(bought_stocks_list) <= 0:
        return

    history_data = get_history(
        count=2,  # 需要昨天和今天的价格数据
        frequency='1d',
        field=['open', 'high', 'low', 'close'],
        security_list=bought_stocks_list,
        fq=g.hist_fq
    )

    # 遍历已买入股票，执行卖出逻辑
    for stock in bought_stocks_list:  # 使用list避免字典修改时迭代错误
        try:
            # 获取所需历史数据
            stock_hd = history_data[history_data['code'].isin([stock])]

            # 确保有足够的历史数据
            if len(stock_hd) < 2:
                continue

            # 获取关键价格数据
            buy_open = g.bought_stocks[stock]  # 买入日开盘价
            today_close = stock_hd['close'][-1]  # 今日收盘价
            today_high = stock_hd['high'][-1]  # 今日最高价
            today_low = stock_hd['low'][-1]  # 今日最低价
            yesterday_high = stock_hd['high'][-2]  # 昨日最高价
            yesterday_low = stock_hd['low'][-2]  # 昨日最低价

            # 卖出条件判断
            sell_condition1 = today_close < buy_open  # 价格跌破买入日开盘价
            sell_condition2 = (today_high < yesterday_high) and (today_low < yesterday_low)  # 价量形态走弱

            if sell_condition1 or sell_condition2:
                # 执行卖出（全部平仓）
                position = get_position(stock)
                if position.amount > 0:
                    order(stock, -position.amount, limit_price=today_close)
                    del g.bought_stocks[stock]  # 从持仓列表移除
                    log.info(
                        f"卖出 {stock}，价格：{today_close}，"
                        f"触发条件：{'跌破买入价' if sell_condition1 else '价量形态走弱'}"
                    )

        except Exception as e:
            log.error(f"处理股票 {stock} 卖出逻辑时出错：{str(e)}")
            continue


# 盘后日志记录
def after_trading_end(context, data):
    # 检查变量是否存在，避免属性错误
    if hasattr(g, 'bought_stocks'):
        log.info(f"今日持仓股票数量：{len(g.bought_stocks)}")
        log.info(f"当前持仓股票：{list(g.bought_stocks.keys())}")
    else:
        log.info("当前无持仓股票")
    log.info(f"账户总资产：{context.portfolio.portfolio_value}")
