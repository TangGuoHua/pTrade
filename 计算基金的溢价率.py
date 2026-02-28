
from logging import config

import pandas as pd
import sxsc_tushare as sx
import mplfinance as mpf
import numpy as np          # 数值计算基础库
from datetime import datetime, timedelta  # Python标准日期时间库
from scipy import stats     # 科学计算统计模块
from sklearn.metrics import r2_score
# from config.Config import config

import sys
import os
# 将项目根目录加入Python路径
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../.')))
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../..")))
import config.Config as cfg
    
secrets =cfg.config.load_secrets()  # 确保加载配置文件，获取API Key等敏感信息
db_password = secrets["db_password"]
api_key = secrets["api_key"]
print(f"API Key：{api_key}")

sx.set_token(api_key)

pro=sx.get_api(env="prd")
def get_etf_premium(ts_code, start_date, end_date):
    """
    精准获取场内ETF溢价率（基于fund_nav完整参数优化）
    :param ts_code: ETF代码，如510330.SH（必填）
    :param start_date: 开始日期，格式YYYYMMDD（必填）
    :param end_date: 结束日期，格式YYYYMMDD（必填）
    :return: 包含溢价率的DataFrame，无数据返回空DataFrame
    """
    # 参数校验
    if not all([ts_code, start_date, end_date]):
        raise ValueError("ts_code、start_date、end_date均为必填参数！")
    
    try:
        # 2. 获取ETF日线行情（收盘价，需5000积分）
        df_price = pro.fund_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields="trade_date,ts_code,close"  # 仅取需要的字段
        )
        print(df_price.head())  # 调试：查看价格数据结构，确认字段正确
        if df_price.empty:
            print(f"提示：{ts_code} 在{start_date}-{end_date}无日线数据")
            return pd.DataFrame()
        
        # 3. 获取场内基金净值（关键：指定market='E'，需2000积分）
        df_nav = pro.fund_nav(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            market='E',  # 仅筛选场内基金净值，避免场外数据干扰
            fields="ann_date, nav_date,ts_code,unit_nav"  # 仅取净值日期、代码、单位净值
        )
        if df_nav.empty:
            print(f"提示：{ts_code} 在{start_date}-{end_date}无场内净值数据")
            return pd.DataFrame()
        print(df_nav.head())  # 调试：查看净值数据结构，确认字段正确

        # 4. 数据对齐：交易日=净值日期（ETF净值通常按交易日更新）
        # 重命名字段方便合并，确保日期字段名称一致
        df_price.rename(columns={"trade_date": "date"}, inplace=True)
        df_nav.rename(columns={"nav_date": "date"}, inplace=True)
        
        # 合并数据（inner join确保日期完全匹配）
        df_merged = pd.merge(
            df_price, df_nav, 
            on=["date", "ts_code"], 
            how="inner"
        )
        
        # 5. 计算溢价率（保留4位小数，更精准）
        df_merged["premium_rate"] = round(
            (df_merged["close"] - df_merged["unit_nav"]) / df_merged["unit_nav"] * 100,
            4
        )
        
        # 整理字段顺序，方便查看
        df_result = df_merged[["date", "ts_code", "close", "unit_nav", "premium_rate"]]
        df_result = df_result.sort_values("date").reset_index(drop=True)
        
        return df_result
    
    except Exception as e:
        # 捕获常见错误（如积分不足、token错误、网络问题）
        error_msg = f"获取数据失败：{str(e)}"
        if "积分" in error_msg:
            error_msg += "\n提示：fund_daily需5000积分，fund_nav需2000积分，请检查积分是否充足"
        print(error_msg)
        return pd.DataFrame()

# 示例调用：获取沪深300ETF（510330.SH）2025年12月溢价率
if __name__ == "__main__":
    # 注意：日期范围建议选已过的时间段，确保有完整数据
    etf_code = "513520.SH"
    start = "20260227"
    end = "20260228"
    
    # # 调用函数并输出结果
    premium_data = get_etf_premium(etf_code, start, end)
    if not premium_data.empty:
        print(f"\n{etf_code} 溢价率数据：{premium_data.iloc[-1]['premium_rate']}")
        print(premium_data.iloc[-1]['premium_rate'])
        
        # 可选：保存为Excel
        # premium_data.to_excel(f"{etf_code}_溢价率_{start}_{end}.xlsx", index=False)
        # print(f"\n数据已保存为：{etf_code}_溢价率_{start}_{end}.xlsx")