import pandas as pd
import numpy as np


# ---------------------- 2. 计算核心因子 ----------------------
def calculate_momentum_efficiency_factor(df, N=20):
    df = df.dropna(subset=["close", "open"])
    df["price_center"] = (df["open"] + df["high"] + df["low"] + df["close"]) / 4
    # 计算价格动量
    df["price_center_n_days_ago"] = df.groupby("code")["price_center"].shift(N)
    df["momentum"] = (df["price_center"] - df["price_center_n_days_ago"]) / (
        df["price_center_n_days_ago"] + 1e-8
    )
    
    # print(df.head(10))
    # print(df.tail(10))
    # 计算效率系数
    # 第一步：计算每日价格中枢的变动（当日 - 前一日）
    df["daily_price_change"] = df.groupby("code")["price_center"].diff(1)
    # 第二步：计算“价格路程”
    df["price_path"] = df.groupby("code")["daily_price_change"].transform(
        lambda x: x.abs().rolling(window=N).sum()
    )
    # 第三步：计算“价格位移”（N周期内的最终变动绝对值）
    df["price_displacement_abs"] = (df["price_center"] - df["price_center_n_days_ago"]).abs()
    # 第四步：计算效率系数
    df["efficiency_coefficient"] = df["price_displacement_abs"] / (df["price_path"] + 1e-8)
    df["efficiency_coefficient"] = df["efficiency_coefficient"].clip(0, 1)
    
    # 计算最终因子
    df["momentum_efficiency_factor"] = df["momentum"] * df["efficiency_coefficient"]
    
    # 过滤无效数据
    return df.dropna(subset=["momentum_efficiency_factor"])

# ---------------------- 3. 调用执行 ----------------------
if __name__ == "__main__":
    # 读取数据
    df = pd.read_csv("D:\\Documents\\投资\\量化\\market_data20241013-20251013.csv", index_col=0)
    # df.index.name = "date"
    # df.reset_index(inplace=True)
    # sort by date and code
    # 按照 code 分组， 组内按照date排序 descending=False
    # drop the row with close or open price is null
    # df = df.dropna(subset=["close", "open"])
    # df = df.sort_values(by=["code", "date"], ascending=[True,True])
    # print(df.head(10))
    df_result = calculate_momentum_efficiency_factor(df, N=20)
    # 按照 momentum_efficiency_factor 降序排列, 打印每一个code的最近的5条记录
    # df_result = df_result.sort_values(by=["date", "momentum_efficiency_factor"], ascending=[True, False])
    print(df_result.groupby("code").tail(5))

    print("518880.SS latest record:")
    print(df_result[df_result["code"]=="518880.SS"].tail(1)["momentum_efficiency_factor"].values[0])
    # print latest row for each code, sort by momentum_efficiency_factor descending
    # print(df_result.groupby("code")[["code", "momentum_efficiency_factor"]].tail(1).sort_values(by="momentum_efficiency_factor", ascending=False))

    # # 按照 date 和 momentum_efficiency_factor 降序排列

    # df_result = df_result.sort_values(by=["date", "momentum_efficiency_factor"], ascending=[True, False])
    # # print columns code, momentum_efficiency_factor.
    # print(df_result[["code", "momentum_efficiency_factor"]].tail(10))
    # daily_top_etf = df_result.groupby("date", group_keys=False).apply(
    #     lambda x: x.nlargest(n=3, columns="momentum_efficiency_factor")[
    #         ["code", "momentum_efficiency_factor", "momentum", "efficiency_coefficient"]
    #     ],
    #     include_groups=False  # 添加参数消除警告
    # ).reset_index(drop=False)