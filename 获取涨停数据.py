

import sxsc_tushare as ts
import pandas as pd
import time
from datetime import datetime
import logging as log

# ===================== 请先配置你的 Tushare token =====================
# 替换成你自己的 Tushare token
# get token from config.Config
from config.Config import config
secret = config.load_secrets()
api_key = secret["api_key"]
ts.set_token(api_key)
pro = ts.get_api()

# ===================== 路径相关配置 =====================
def get_research_path():
    """
    请根据你的实际情况实现这个函数
    返回你的研究目录的根路径，例如：return "/Users/xxx/research/"
    """
    # 这里是示例，你需要替换成自己的实际路径
    return "/your/actual/research/path/"

# 定义 CSV 文件路径
# limit_up_csv_path = f"{get_research_path()}csv/limit_up.csv"
limit_up_csv_path = f"./test/data/limit_up.csv"

def get_trading_dates(year=2025):
    """获取指定年份的所有交易日列表"""
    log.info(f"获取 {year} 年的交易日历")
    # 获取当年的交易日历
    start_date = f"{year}0101"
    end_date = f"{year}1231"
    
    try:
        # 获取交易日历
        cal_df = pro.trade_cal(
            start_date=start_date,
            end_date=end_date,
            is_open=1  # 只获取交易日
        )
        # 返回排序后的交易日列表
        trading_dates = cal_df['cal_date'].sort_values().tolist()
        print(f"获取到 {year} 年共 {len(trading_dates)} 个交易日")
        return trading_dates
    except Exception as e:
        print(f"获取交易日历失败: {e}")
        return []

def get_all_limit_up_data(year=2025):
    """获取2025年所有交易日的涨停数据"""
    log.info(f"开始获取 {year} 年的涨停数据")   
    # 获取2025年所有交易日
    trading_dates = get_trading_dates(year)
    if not trading_dates:
        return pd.DataFrame()
    
    # 用于存储所有数据的列表
    all_data = []
    
    # 遍历每个交易日
    total_days = len(trading_dates)
    for idx, trade_date in enumerate(trading_dates, 1):
        try:
            print(f"正在获取 {trade_date} 的涨停数据 ({idx}/{total_days})...")
            
            # 调用涨停数据接口
            df = pro.limit_list_d(
                trade_date=trade_date, 
                limit_type='U',  # 涨停
                fields='ts_code,trade_date,industry,name,close,pct_chg,open_times,up_stat,limit_times'
            )
            
            if not df.empty:
                all_data.append(df)
                print(f"成功获取 {trade_date} 的涨停数据，共 {len(df)} 条记录")
            else:
                print(f"{trade_date} 无涨停数据")
            
            # 添加延时，避免接口调用过于频繁（Tushare有接口限流）
            time.sleep(0.5)
            
        except Exception as e:
            print(f"获取 {trade_date} 数据失败: {e}")
            # 失败时延时稍长一点
            time.sleep(2)
            continue
    
    # 合并所有数据
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        print(f"\n数据获取完成，总计 {len(result_df)} 条涨停记录")
        return result_df
    else:
        print("\n未获取到任何涨停数据")
        return pd.DataFrame()

def save_limit_up_data(year=2025):
    """获取并保存涨停数据到CSV文件"""
    log.info(f"开始保存 {year} 年的涨停数据到 CSV 文件")
    # 获取所有涨停数据
    limit_up_df = get_all_limit_up_data(year)
    
    if not limit_up_df.empty:
        try:
            # 确保目录存在
            import os
            os.makedirs(os.path.dirname(limit_up_csv_path), exist_ok=True)
            
            # 保存到CSV文件
            limit_up_df.to_csv(limit_up_csv_path, index=False, encoding='utf-8-sig')
            print(f"\n数据已成功保存到: {limit_up_csv_path}")
            
            # 打印数据概览
            print(f"\n数据概览:")
            print(f"- 总记录数: {len(limit_up_df)}")
            print(f"- 涉及交易日数: {limit_up_df['trade_date'].nunique()}")
            print(f"- 涉及股票数: {limit_up_df['ts_code'].nunique()}")
            
        except Exception as e:
            print(f"保存文件失败: {e}")
    else:
        print("无数据可保存")

# 主执行函数
if __name__ == "__main__":
    # 执行数据获取和保存
    # print api_key
    print(f"Using Tushare API Key: {api_key}")
    print("开始获取并保存2025年涨停数据...")
    # save_limit_up_data(2025)
    # # read csv from limit_up_csv_path and set the first column as index
    # df = pd.read_csv(limit_up_csv_path, index_col=0)
    # print(df.head())
    # # print index of df
    # print(df.index)
