import pandas as pd
import os
from datetime import datetime, timedelta

def analyze_etf_scores(csv_file, target_etf):
    if not os.path.exists(csv_file):
        return None, None
    
    # 读取CSV，第一列是索引（ETF代码）
    df = pd.read_csv(csv_file, index_col=0)
    # 转换“当前日期”列为日期格式
    df['当前日期'] = pd.to_datetime(df['date'])
    
    # 获取所有不重复的日期并按降序排序（最新日期在前）
    all_dates = sorted(df['当前日期'].unique(), reverse=True)
    if len(all_dates) < 2:
        raise ValueError("数据不足两天，无法分析")
    
    # 取最近两天的日期
    recent_dates = all_dates[:2]
    date1, date2 = recent_dates  # date1是最新日期，date2是前一天
    
    # 分别获取两天的数据
    df_day1 = df[df['当前日期'] == date1].copy()
    df_day2 = df[df['当前日期'] == date2].copy()
    
    # 检查目标ETF是否在两天的数据中都存在
    if target_etf not in df_day1.index or target_etf not in df_day2.index:
        raise ValueError(f"指定ETF {target_etf} 在最近两天的数据中不完整")
    
    # 1. 判断过去两天是否都是排名第一（分数最高）
    day1_is_first = df_day1.loc[target_etf, 'score'] == df_day1['score'].max()
    day2_is_first = df_day2.loc[target_etf, 'score'] == df_day2['score'].max()
    both_first = day1_is_first and day2_is_first and df_day2.loc[target_etf, 'score'] > 1.5
    
    # 2. 判断与第二名的差距是否拉大（仅当两天都是第一时才计算）
    gap_enlarged = None
    if both_first:
        # 计算第一天目标ETF与第二名的分差
        day1_scores = df_day1['score'].sort_values(ascending=False)
        day1_gap = day1_scores.iloc[0] - day1_scores.iloc[1]  # 第一名 - 第二名
        
        # 计算第二天目标ETF与第二名的分差
        day2_scores = df_day2['score'].sort_values(ascending=False)
        day2_gap = day2_scores.iloc[0] - day2_scores.iloc[1]
        
        # 差距拉大：当天分差 > 前一天分差
        gap_enlarged = day1_gap > day2_gap
    
    return both_first, gap_enlarged

# 示例用法
if __name__ == "__main__":
    csv_file = ".\\test\\.\\data\\Score.csv"
    target_etf = "513520.SS"  
    
    try:
        both_first, gap_enlarged = analyze_etf_scores(csv_file, target_etf)
        print(f"1. 过去两天都是排名第一: {both_first}")
        if both_first:
            print(f"2. 与第二名的差距是否拉大: {gap_enlarged}")
        else:
            print("2. 因并非两天都排名第一，无需计算差距变化")
    except Exception as e:
        print(f"分析失败: {e}")