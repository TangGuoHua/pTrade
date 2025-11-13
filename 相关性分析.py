import pandas as pd
import numpy as np
import os

def get_matrix(etf_price_history_df, etf_name_list_df, sort_by_correlation=True):
    """
    计算并打印ETF之间的相关性矩阵
    
    参数:
    etf_price_history_df: DataFrame，包含ETF价格历史数据
                         必须包含'code'列(ETF代码)和'close'列(收盘价)
    etf_name_list_df: DataFrame，包含ETF代码和名称的对应关系
                      必须包含'ts_code'列(ETF代码)和'name'列(ETF名称)
    sort_by_correlation: bool，是否按相关性强度排序ETF
    
    返回:
    correlation_matrix: DataFrame，ETF相关性矩阵
    """
    
    # 1. 数据预处理
    # 确保价格数据按日期排序
    if 'date' in etf_price_history_df.columns:
        etf_price_history_df['date'] = pd.to_datetime(etf_price_history_df['date'])
        etf_price_history_df = etf_price_history_df.sort_values(['code', 'date'])
    
    # 2. 将价格数据透视为宽格式，每个ETF代码作为列
    price_wide = etf_price_history_df.pivot_table(
        index='date', 
        columns='code', 
        values='close'
    )
    
    # 3. 计算相关性矩阵
    correlation_matrix = price_wide.corr()
    
    # 4. 创建代码到名称的映射字典
    code_to_name = dict(zip(etf_name_list_df['ts_code'], etf_name_list_df['name']))
    
    # 5. 将列名和索引从代码转换为名称
    # 过滤掉不在名称列表中的ETF
    valid_codes = [code for code in correlation_matrix.columns if code in code_to_name]
    correlation_matrix = correlation_matrix.loc[valid_codes, valid_codes]
    
    # 重命名索引和列
    correlation_matrix.index = [code_to_name[code] for code in correlation_matrix.index]
    correlation_matrix.columns = [code_to_name[code] for code in correlation_matrix.columns]
    
    # 6. 按相关性强度排序
    if sort_by_correlation:
        # 计算平均相关性并排序
        avg_correlation = correlation_matrix.mean().sort_values(ascending=False)
        correlation_matrix = correlation_matrix.loc[avg_correlation.index, avg_correlation.index]
    
    # 7. 格式化输出，模仿参考图的样式
    print("相关性矩阵:")
    print("=" * 80)
    
    # 打印表头
    header = "        "
    for col in correlation_matrix.columns:
        # 限制名称长度以适应表格
        short_name = col[:6] if len(col) > 6 else col
        header += f"{short_name:<8}"
    print(header)
    print("-" * 80)
    
    # 打印数据行
    for idx, row in correlation_matrix.iterrows():
        # 限制行名称长度
        short_idx = idx[:6] if len(idx) > 6 else idx
        line = f"{short_idx:<8}"
        for value in row:
            # 格式化相关系数，保留两位小数
            line += f"{value:.2f}      "
        print(line)
    
    print("=" * 80)
    
    return correlation_matrix

def print_top_correlations(correlation_matrix, top_n=10):
    """
    打印相关性最高的ETF对
    
    参数:
    correlation_matrix: DataFrame，相关性矩阵
    top_n: int，要显示的前n个最高相关性对
    """
    # 创建相关性对的DataFrame
    corr_pairs = correlation_matrix.unstack()
    
    # 过滤掉自相关
    corr_pairs = corr_pairs[corr_pairs < 1.0]
    
    # 按相关性降序排序
    corr_pairs = corr_pairs.sort_values(ascending=False)
    
    # 打印前n个最高相关性对
    print(f"\n相关性最高的{top_n}个ETF对:")
    print("-" * 50)
    for i, ((etf1, etf2), corr) in enumerate(corr_pairs.head(top_n).items(), 1):
        print(f"{i:2d}. {etf1:<10} vs {etf2:<10} : {corr:.4f}")

# 高级版本：包含热力图功能（需要matplotlib）
def get_matrix_with_heatmap(etf_price_history_df, etf_name_list_df, sort_by_correlation=True, save_dir=None):
    """
    计算并打印ETF之间的相关性矩阵，并绘制热力图（需要安装matplotlib）
    
    参数:
    etf_price_history_df: DataFrame，包含ETF价格历史数据
    etf_name_list_df: DataFrame，包含ETF代码和名称的对应关系
    sort_by_correlation: bool，是否按相关性强度排序ETF
    save_dir: str，热力图保存目录（默认为当前工作目录）
    
    返回:
    correlation_matrix: DataFrame，ETF相关性矩阵
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.colors as mcolors
        from matplotlib import rcParams
        
        # 设置中文字体支持
        plt.rcParams['font.sans-serif'] = ['WenQuanYi Zen Hei', 'SimHei', 'DejaVu Sans']
        plt.rcParams['axes.unicode_minus'] = False
        
    except ImportError:
        print("警告: matplotlib未安装，无法生成热力图。请安装matplotlib后重试。")
        print("安装命令: pip install matplotlib")
        return get_matrix(etf_price_history_df, etf_name_list_df, sort_by_correlation)
    except Exception as e:
        print(f"设置中文字体时出现警告: {e}")
        print("将使用默认字体，可能导致中文显示不正常")
    
    # 首先计算相关性矩阵
    correlation_matrix = get_matrix(etf_price_history_df, etf_name_list_df, sort_by_correlation)
    
    # 设置保存目录
    if save_dir is None:
        save_dir = os.getcwd()
    
    # 确保保存目录存在
    os.makedirs(save_dir, exist_ok=True)
    
    # 设置保存路径
    save_path = os.path.join(save_dir, 'correlation_heatmap.png')
    
    # 绘制热力图
    plt.figure(figsize=(14, 12))
    
    # 创建自定义颜色映射
    colors = ['#FF6B6B', '#FFE66D', '#4ECDC4', '#45B7D1']
    n_bins = 100
    cmap = mcolors.LinearSegmentedColormap.from_list('custom_cmap', colors, N=n_bins)
    
    # 绘制热力图
    im = plt.imshow(correlation_matrix.values, cmap=cmap, aspect='auto', vmin=-1, vmax=1)
    
    # 设置坐标轴标签
    plt.xticks(range(len(correlation_matrix.columns)), correlation_matrix.columns, 
               rotation=45, ha='right', fontsize=10)
    plt.yticks(range(len(correlation_matrix.index)), correlation_matrix.index, 
               fontsize=10)
    
    # 在每个格子中添加数值
    for i in range(len(correlation_matrix.index)):
        for j in range(len(correlation_matrix.columns)):
            text = plt.text(j, i, f'{correlation_matrix.iloc[i, j]:.2f}',
                           ha="center", va="center", color="black", fontsize=9)
    
    # 添加颜色条
    cbar = plt.colorbar(im)
    cbar.set_label('相关系数', rotation=270, labelpad=20, fontsize=12)
    
    # 设置标题
    plt.title('ETF相关性矩阵热力图', fontsize=16, fontweight='bold', pad=20)
    
    # 调整布局
    plt.tight_layout()
    
    # 保存图片
    plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f"热力图已保存为: {save_path}")
    
    return correlation_matrix

# 示例用法（假设文件存在）
if __name__ == "__main__":
     # 模拟数据读取
    # 实际使用时请替换为：
    etf_price_history_df = pd.read_csv('.\\test\\data\\market_data20241013-20251013.csv')
    print(etf_price_history_df.head())
    # 把第一列名称改为'date'
    etf_price_history_df.rename(columns={etf_price_history_df.columns[0]: 'date'}, inplace=True)
    # etf_price_history_df['date'] = pd.to_datetime(etf_price_history_df['date'])

    etf_name_list_df = pd.read_csv('.\\test\\data\\industry_df2.csv')
    # 如果etf_name_list_df 中的ts_code是以 .SH 结尾的把 .SH 替换为 .SS
    etf_name_list_df['ts_code'] = etf_name_list_df['ts_code'].str.replace('.SH', '.SS')

    # # 创建模拟数据用于测试
    # dates = pd.date_range('2023-01-01', periods=100, freq='D')
    # codes = ['510050', '510300', '510500', '159915', '159922', '000922', '001594', '001552', '001558', '001559']
    # names = ['上证50', '沪深300', '中证500', '创业板指', '中小板指', '红利指数', '中证红利', '中证环保', '医药100', '全指消费']
    
    # # 创建价格数据
    # np.random.seed(42)
    # price_data = []
    # for code in codes:
    #     base_price = np.random.uniform(1, 5)
    #     prices = base_price * np.exp(np.cumsum(np.random.normal(0.0005, 0.02, len(dates))))
    #     for date, price in zip(dates, prices):
    #         price_data.append({'date': date, 'code': code, 'close': price})
    
    # etf_price_history_df = pd.DataFrame(price_data)
    
    # # 创建名称数据
    # etf_name_list_df = pd.DataFrame({
    #     'ts_code': codes,
    #     'name': names
    # })
    
    # 调用基本函数
    print("=== 基本版本 ===")
    correlation_matrix = get_matrix(etf_price_history_df, etf_name_list_df, sort_by_correlation=True)
    
    # 打印相关性最高的ETF对
    print_top_correlations(correlation_matrix, top_n=10)
    
    # 尝试调用高级版本（带热力图）
    print("\n=== 高级版本（带热力图） ===")
    # 使用当前工作目录作为保存目录
    correlation_matrix_advanced = get_matrix_with_heatmap(
        etf_price_history_df, 
        etf_name_list_df, 
        sort_by_correlation=True,
        save_dir=os.getcwd()
    )
