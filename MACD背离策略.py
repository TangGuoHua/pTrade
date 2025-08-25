# 初始化函数
def initialize(context):
    # 设置全局变量
    g.security = 'SR509.XZCE'  # 示例标的，确保股票代码符合规范，尾缀为SS
    set_universe(g.security)  # 设置股票池
    g.short_period = 12  # 短期EMA天数
    g.long_period = 26  # 长期EMA天数
    g.signal_period = 9  # 信号线EMA天数
    g.last_low = None  # 上一次低点价格
    g.last_macd_low = None  # 上一次MACD低点

# 盘前事件
def before_trading_start(context, data):
    pass

# 60分钟级别盘中事件
def handle_data(context, data):
    # 获取60分钟K线数据
    df = get_history(120, '60m', ['open', 'close', 'high', 'low'], security_list=g.security, fq=None, include=False)

    # 计算MACD指标
    df['EMA12'] = df['close'].ewm(span=g.short_period, adjust=False).mean()
    df['EMA26'] = df['close'].ewm(span=g.long_period, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=g.signal_period, adjust=False).mean()
    df['MACD'] = 2 * (df['DIF'] - df['DEA'])

    # 获取最新价格和MACD值
    current_price = df['close'].iloc[-1]
    current_macd = df['MACD'].iloc[-1]
    current_dif = df['DIF'].iloc[-1]
    previous_dif = df['DIF'].iloc[-2]
    current_dea = df['DEA'].iloc[-1]

    # 检查底背离
    if g.last_low is not None and g.last_macd_low is not None:
        if current_price < g.last_low and current_macd > g.last_macd_low:
            # 价格创新低，但MACD不创新低，底背离
            cash = context.portfolio.cash
            if cash > 0:
                order_value(g.security, cash)
                log.info("底背离买入 %s" % g.security)

    # 检查死叉
    if previous_dif > current_dea and current_dif < current_dea:
        # DIF线下穿DEA线，形成死叉
        position = get_position(g.security)
        if position.amount > 0:
            order_target(g.security, 0)
            log.info("死叉卖出 %s" % g.security)

    # 更新价格和MACD的低点
    g.last_low = df['low'].min()
    g.last_macd_low = df['MACD'].min()

# 盘后事件
def after_trading_end(context, data):
    pass