import os
import time 
import numpy as np
import pandas as pd
import akshare as ak
import quantstats as qs
from datetime import datetime
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings('ignore')



def get_data(code_list, start_date, end_date):
    try:
        df_list = []
        for code in code_list:
            print(f'正在获取[{code}]行情数据...')
            # adjust：""-不复权、qfq-（前复权）、hfq-后复权
            df = ak.fund_etf_hist_em(symbol=code, period='daily', 
                start_date=start_date, end_date=end_date, adjust='hfq')
            df.insert(0, 'code', code)
            df_list.append(df)
            time.sleep(3)
        print('数据获取完毕！')

        all_df = pd.concat(df_list, ignore_index=True)
        data = all_df.pivot(index='日期', columns='code', values='收盘')[code_list]
        data.index = pd.to_datetime(data.index)
        data = data.sort_index()
        data.head(10)
    except Exception as e:
        print(f'获取实时数据出错: {e}')
        # 读取本地数据
        print('读取本地数据文件etf_data2.csv')
        data = pd.read_csv('etf_data2.csv', index_col=0, parse_dates=True)
    return data

# 计算动量
def calculate_momentum(code_list, data, lookback=10):
    # 计算每日涨跌幅和N日涨跌幅
    for code in code_list:
        data['日收益率_'+code] = data[code] / data[code].shift(1) - 1.0
        data['涨幅_'+code] = data[code] / data[code].shift(lookback) - 1.0
    # 去掉缺失值
    data = data.dropna()
    data[['涨幅_'+v for v in code_list]].head(10)

    # 取出每日涨幅最大的证券
    data['信号'] = data[['涨幅_'+v for v in code_list]].idxmax(axis=1).str.replace('涨幅_', '')
    # 今日的涨幅由昨日的持仓产生
    data['信号'] = data['信号'].shift(1)
    data = data.dropna()
    data['轮动策略日收益率'] = data.apply(lambda x: x['日收益率_'+x['信号']], axis=1) 
    # 第一天尾盘交易，当日涨幅不纳入
    data.loc[data.index[0],'轮动策略日收益率'] = 0.0
    data['轮动策略净值'] = (1.0 + data['轮动策略日收益率']).cumprod()
    data[['涨幅_'+v for v in code_list]+['信号','轮动策略日收益率','轮动策略净值']].head(10)

    
    return data

def drawPlot(data, code_list):
    # 绘图
    # 显示中文设置
    plt.rcParams['font.sans-serif']=['SimHei']
    plt.rcParams['axes.unicode_minus']=False

    # 获取ETF名称
    # etf_df = ak.fund_etf_spot_em()
    # save to csv
    etf_df = pd.read_csv('etf_list.csv')
    code_to_name_dict = etf_df.set_index('代码')['名称'].to_dict()
    # ans = code_to_name_dict.keys().__contains__('510300')
    # print(ans)
    # 绘制净值曲线图
    fig, ax = plt.subplots(figsize=(15, 6))
    ax.set_xlabel('日期')
    ax.set_ylabel('净值')
    name_list = []
    for code in code_list:
        if int(code) not in code_to_name_dict.keys():
            continue
        name = code_to_name_dict[int(code)]
        name_list.append(name)
        data[name+'净值'] = data[code] / data[code].iloc[0]
        ax.plot(data.index, data[name+'净值'].values, linestyle='--')
    ax.plot(data.index, data['轮动策略净值'].values, linestyle='-', color='#FF8124')
    name_list.append('轮动策略')

    # 显示图例和标题
    ax.legend(name_list)
    ax.set_title('轮动策略净值曲线对比 by 公众号【量化君也】')

    plt.show()

def generateReport(data):
    # 生成策略报告
    #将完整回测报告存为HTML文件
    title = '轮动策略回测报告_原始版 by 公众号【量化君也】'
    output_file = os.path.join(os.getcwd(), f'{title}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html')
    qs.reports.html(data['轮动策略净值'], benchmark=data['纳指ETF净值'],
                    title=title, output=output_file) 
    print(f'已将回测报告保存到文件: {output_file}')
    #输出基本回测报告信息
    qs.reports.basic(data['轮动策略净值'], benchmark=data['纳指ETF净值']) 

if __name__ == "__main__":
    # 用akshare 获取数据
    # 510300：沪深300ETF，代表大盘
    # 510500：中证500ETF，代表小盘
    # 510880：红利ETF，代表价值
    # 159915：创业板ETF，代表成长
    # code_list = ['510300', '510500', '510880', '159915']

    # 改进版一：剔除中证500，加入纳指和黄金
    # 510880：红利ETF，代表价值
    # 159915：创业板ETF，代表成长
    # 513100：纳指ETF，代表外盘
    # 518880：黄金ETF，代表商品
    code_list = ['510880', '159915', '513100', '518880']
    start_date = '20150101'
    end_date = '20250828'
    # data = get_data(code_list, start_date, end_date)
    # save to csv
    # data.to_csv('etf_data2.csv')
    # data = pd.read_csv('etf_data.csv', index_col=0, parse_dates=True)
    data = pd.read_csv('etf_data2.csv', index_col=0, parse_dates=True)

    data = calculate_momentum(code_list, data, lookback=20)
    # print(data.head(10))
    # print(data.tail(10))

    # 画图
    drawPlot(data, code_list)

    # 生成报告
    generateReport(data)

    # data = pd.read_csv('D:\\Documents\\投资\\量化\\market_data2.csv', index_col=0, parse_dates=True)
    # # 为每一天计算当日均价， 当日均价=(开盘价 + 最高价 + 最低价 + 收盘价) / 4
    # data['today_price'] = (data['open'] + data['high'] + data['low'] + data['close']) / 4
    # # print last 20 rows, colums today_price, close
    # print(data[['today_price', 'close']].tail(20))