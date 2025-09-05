import pandas as pd
import numpy as np
from scipy.stats import linregress
# define a dictionay g
g = {}
g['etf_pool'] =['159857.SZ',    # 光伏ETF
                '162719.SZ',    # 石油LO
                '510880.SS',    # 上证红利ETF
                '512100.SS',    # 中证1000ETF
                '512480.SS',    # 半导体ETF
                '513030.SS',    # 华安德国ETF
                '513100.SS',    # 纳指ETF
                '513180.SS',    # 恒生科技指数ETF
                '513500.SS',
                '513520.SS',    # 日经225ETF
                '515050.SS',    # 5G通信ETF
                '518880.SS']    # 黄金ETF
def calculate_etf_score(df):
    """
    根据多因子动态ETF策略的评分逻辑计算ETF得分
    :param df: 包含'close'（收盘价）和'volume'（成交量）的DataFrame，索引为时间
    :return: 包含各因子得分及总得分的DataFrame
    """
    # 确保数据按时间排序
    df = df.sort_index()
    score_df = pd.DataFrame(index=df.index)
    
    # ----------------------
    # 1. 趋势评分（权重25%）
    # 计算逻辑：25天滑动窗口线性回归，(年化收益率 × R平方)
    # ----------------------
    def trend_score(window):
        if len(window) < 25:
            return np.nan  # 窗口不足时返回NaN
        # 时间序列（转换为天数，用于线性回归）
        x = np.arange(len(window))
        # 收盘价序列（只取收盘价列）
        y = window.values  # 现在window是Series，直接取values
        # 线性回归
        reg = linregress(x, y)
        # 计算窗口内日均收益率（最后一天/第一天 - 1）
        daily_return = (y[-1] / y[0]) ** (1 / len(window)) - 1
        # 年化收益率（假设252个交易日）
        annual_return = (1 + daily_return) ** 252 - 1
        # R平方（拟合优度）
        r_squared = reg.rvalue **2
        # 趋势评分 = 年化收益率 × R平方
        return annual_return * r_squared
    
    # 修正：只对close列进行滚动计算，确保返回单值
    score_df['trend_score'] = df['close'].rolling(window=25).apply(trend_score)
    
    # ----------------------
    # 2. 反转因子（权重17%）
    # 计算逻辑：5日涨跌幅 + 10日涨跌幅
    # ----------------------
    # 5日涨跌幅 =（当前收盘价 / 5日前收盘价 - 1）
    score_df['5d_return'] = df['close'] / df['close'].shift(5) - 1
    # 10日涨跌幅 =（当前收盘价 / 10日前收盘价 - 1）
    score_df['10d_return'] = df['close'] / df['close'].shift(10) - 1
    # 反转因子 = 5日涨跌幅 + 10日涨跌幅
    score_df['reverse_factor'] = score_df['5d_return'] + score_df['10d_return']
    
    # ----------------------
    # 3. 量能比率（权重58%）
    # 计算逻辑：5日平均成交量 / 18日平均成交量
    # ----------------------
    # 5日平均成交量
    score_df['5d_volume_mean'] = df['volume'].rolling(window=5).mean()
    # 18日平均成交量
    score_df['18d_volume_mean'] = df['volume'].rolling(window=18).mean()
    # 量能比率（避免除零错误）
    score_df['volume_ratio'] = score_df['5d_volume_mean'] / score_df['18d_volume_mean'].replace(0, np.nan)
    
    # ----------------------
    # 4. 标准化因子（避免量纲影响）
    # ----------------------
    def normalize(series):
        return (series - series.rolling(window=60).mean()) / series.rolling(window=60).std()
    
    score_df['trend_norm'] = normalize(score_df['trend_score'])
    score_df['reverse_norm'] = normalize(score_df['reverse_factor'])
    score_df['volume_norm'] = normalize(score_df['volume_ratio'])
    
    # ----------------------
    # 5. 总得分（按权重计算）
    # ----------------------
    score_df['total_score'] = (
        0.25 * score_df['trend_norm'] +
        0.17 * score_df['reverse_norm'] +
        0.58 * score_df['volume_norm']
    )
    
    # ----------------------
    # 6. 动态止盈信号
    # ----------------------
    score_df['stop_profit_signal'] = df['close'] / df['close'].shift(20) - 1
    
    return score_df.tail(1)


def calculate_all_etf_score(market_data):
    result_df = pd.DataFrame()
    if market_data is None or market_data.empty:
        print("market data is empty")
        return
    
    for symbol in g['etf_pool']:
        try:
            
            df = market_data[market_data['code'].isin([symbol])]
            stock_df = calculate_etf_score(df)
            print(f"计算标的{symbol} 的得分是{stock_df['total_score'].values[0]}")
            result_df = pd.concat([result_df, stock_df])
            
        except:
            print(f"计算标的{symbol} 得分出错")

    # sort by total_score in result_df
    result_df = result_df.sort_values(by='total_score', ascending=False)
    return result_df

# 数据加载与处理部分保持不变
df = pd.read_csv('D:\\Documents\\投资\\量化\\market_data2.csv')
df.rename(columns={'Unnamed: 0': 'index', 'ts_code': 'code', 'close': 'close'}, inplace=True)
market_data = df.set_index('index')

market_data.index = pd.to_datetime(market_data.index)

score_list = calculate_all_etf_score(market_data)
print(score_list[['total_score', 'stop_profit_signal']].tail(30))