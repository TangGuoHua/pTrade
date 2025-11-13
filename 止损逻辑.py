# f"{get_research_path()}csv/stop_loss_records.csv"

import pandas as pd


def get_stop_loss_records(file_path, security, start_time=None, end_time=None):
    """读取止损记录"""
    print(f"读取止损记录: file_path={file_path}, security={security}, start_time={start_time}, end_time={end_time}")
    try:
        # 获取合规的文件路径
        # file_path = 
        
        # np.log.info(file_path)
        # 读取文件内容
        df = pd.read_csv(file_path, names=['date', 'code',  'price'], parse_dates=['date'])
        
        print(f"读取到的止损记录数量: {len(df)}")
        print(df.head())
        # 过滤指定证券和日期范围
        if security:
            df = df[df['code'] == security]
            print(f"过滤后止损记录数量: {len(df)}")
        if start_time:
            df = df[df['date'] >= pd.to_datetime(start_time)]
            print(f"过滤后止损记录数量: {len(df)}")
        if end_time:
            df = df[df['date'] <= pd.to_datetime(end_time)]
            print(f"过滤后止损记录数量: {len(df)}")
        
        return df
    except Exception as e:
        print("读取止损记录失败: %s" % (str(e)))
        return pd.DataFrame(columns=['code', 'date', 'price'])
    

if __name__ == "__main__":
    # file_path = get_research_path() + "csv/stop_loss_records.csv"
    # time format 2025-11-05 09:33:00
    current_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
    # # 3 days ago
    three_days_ago = (pd.Timestamp.now() - pd.Timedelta(days=3)).strftime("%Y-%m-%d %H:%M:%S")
    file_path = "D:\Documents\投资\量化\stop_loss_records.csv"
    records = get_stop_loss_records(file_path, security='600570.SS', start_time=three_days_ago, end_time=current_time)
    # 最新的止损记录
    print(f"数量: {len(records)}")
    latest_record = records.sort_values(by='date', ascending=False).head(1)
    print(f"最新止损记录: {latest_record}")

    # df = pd.read_csv(file_path, names=['date', 'code',  'price'], parse_dates=['date'])
    # df = df[df['code'] == '600570.SS'].sort_values(by='date', ascending=False)
    # print(df.head(1))

    # 获取当天9:30到当前时间的止损记录
    # today_date = pd.Timestamp.now().strftime("%Y-%m-%d")
    # start_of_day = f"{today_date} 09:30:00"
    # records_today = get_stop_loss_records(file_path, security='600570.SS', start_time=start_of_day, end_time=current_time)
    # print(f"今天的止损记录数量: {len(records_today)}")