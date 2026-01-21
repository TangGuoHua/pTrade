import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import akshare as ak

# 获取上证指数数据（修改列名适配）
df_sh = ak.stock_zh_index_daily("000001")  # 上证指数
print(df_sh.head())
# 检查并修正日期列名（可能是"trade_date"而非"date"）
# if "trade_date" in df_sh.columns:
#     df_sh = df_sh.rename(columns={"trade_date": "date"})
# df_sh['date'] = pd.to_datetime(df_sh['date'])
# df_sh = df_sh[(df_sh['date'] >= '2015-12-29') & (df_sh['date'] <= '2025-12-28')]

# # 获取沪深300ETF数据（前复权）
# df_300etf = ak.fund_etf_hist_sina("510300")  # 沪深300ETF
# if "trade_date" in df_300etf.columns:
#     df_300etf = df_300etf.rename(columns={"trade_date": "date"})
# df_300etf['date'] = pd.to_datetime(df_300etf['date'])
# df_300etf = df_300etf[(df_300etf['date'] >= '2015-12-29') & (df_300etf['date'] <= '2025-12-28')]

# # 获取沪深300指数数据
# df_300 = ak.stock_zh_index_daily("000300")  # 沪深300指数
# if "trade_date" in df_300.columns:
#     df_300 = df_300.rename(columns={"trade_date": "date"})
# df_300['date'] = pd.to_datetime(df_300['date'])
# df_300 = df_300[(df_300['date'] >= '2015-12-29') & (df_300['date'] <= '2025-12-28')]

# # 2. 计算均线与金叉信号
# df_sh['ma20'] = df_sh['close'].rolling(20).mean()
# df_sh['ma30'] = df_sh['close'].rolling(30).mean()
# df_sh['golden_cross'] = (df_sh['ma20'] > df_sh['ma30']) & (df_sh['ma20'].shift(1) <= df_sh['ma30'].shift(1))

# # 3. 去重连续金叉
# df_sh['golden_cross_filtered'] = df_sh['golden_cross']
# df_sh.loc[df_sh['golden_cross'] & df_sh['golden_cross'].shift(1), 'golden_cross_filtered'] = False

# # 4. 市场环境划分
# df_300['ma60'] = df_300['close'].rolling(60).mean()
# df_300['30d_return'] = df_300['close'].pct_change(30) * 100

# def market_environment(row):
#     if row['close'] > row['ma60'] and row['30d_return'] >= 5:
#         return 'Bull'
#     elif row['close'] < row['ma60'] and row['30d_return'] <= -5:
#         return 'Bear'
#     else:
#         return 'Sideways'

# df_300['environment'] = df_300.apply(market_environment, axis=1)

# # 5. 匹配金叉信号与ETF收益
# golden_cross_dates = df_sh[df_sh['golden_cross_filtered']]['date'].tolist()
# results = []

# for date in golden_cross_dates:
#     # 找到ETF对应日期的索引
#     idx = df_300etf[df_300etf['date'] == date].index
#     if len(idx) == 0:
#         continue
#     idx = idx[0]
#     # 计算未来10日收益（跳过当前日）
#     if idx + 10 >= len(df_300etf):
#         continue
#     entry_price = df_300etf.loc[idx, 'close']
#     exit_price = df_300etf.loc[idx + 10, 'close']
#     return_10d = (exit_price - entry_price) / entry_price * 100
#     # 获取市场环境
#     env = df_300[df_300['date'] == date]['environment'].values[0]
#     results.append({
#         'date': date,
#         'return_10d': return_10d,
#         'environment': env
#     })

# # 6. 统计结果
# df_results = pd.DataFrame(results)
# df_results['win'] = df_results['return_10d'] > 0

# # 全市场统计
# total_count = len(df_results)
# win_count = df_results['win'].sum()
# win_rate = win_count / total_count * 100
# avg_gain = df_results[df_results['win']]['return_10d'].mean()
# avg_loss = df_results[~df_results['win']]['return_10d'].mean()
# profit_loss_ratio = avg_gain / abs(avg_loss)
# max_drawdown = df_results['return_10d'].min()
# sharpe_ratio = df_results['return_10d'].mean() / df_results['return_10d'].std() * np.sqrt(252/10)  # 年化

# # 分市场统计
# env_stats = df_results.groupby('environment').agg({
#     'return_10d': ['count', lambda x: (x > 0).sum() / len(x) * 100, 
#                    lambda x: x[x > 0].mean(), lambda x: x[x <= 0].mean(), 
#                    'min', lambda x: x.mean() / x.std() * np.sqrt(252/10)]
# }).round(2)

# env_stats.columns = ['Count', 'Win Rate (%)', 'Avg Gain (%)', 'Avg Loss (%)', 'Max Drawdown (%)', 'Sharpe Ratio']

# # 7. 输出结果
# print("=== 全市场统计 ===")
# print(f"金叉总次数: {total_count}")
# print(f"上涨次数: {win_count}")
# print(f"上涨概率: {win_rate:.1f}%")
# print(f"上涨平均涨幅: {avg_gain:.2f}%")
# print(f"下跌平均跌幅: {avg_loss:.2f}%")
# print(f"盈亏比: {profit_loss_ratio:.2f}")
# print(f"最大回撤: {max_drawdown:.2f}%")
# print(f"夏普比率: {sharpe_ratio:.2f}")

# print("\n=== 分市场环境统计 ===")
# print(env_stats)