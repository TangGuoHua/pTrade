# etf_correlation_v5_fixed.py（修复返回值解包错误）
import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# --------------------------
# Windows系统专用字体配置
# --------------------------
def setup_windows_chinese_font():
    plt.rcParams["font.family"] = "SimHei"
    plt.rcParams['axes.unicode_minus'] = False
    print(f"✅ 字体配置完成（Windows专用），当前使用字体：{plt.rcParams['font.family']}")

setup_windows_chinese_font()

# --------------------------
# 核心功能函数（增加完整错误捕获，确保返回值）
# --------------------------
def get_matrix(etf_price_history_df, etf_name_list_df, sort_by_correlation=True):
    """
    修复关键点：增加完整try-except，确保无论何时都返回tuple类型，避免None
    """
    try:
        # 1. 数据预处理（增加严格校验）
        # 校验date列格式并排序
        if 'date' not in etf_price_history_df.columns:
            raise KeyError("价格数据必须包含'date'列")
        
        # 尝试转换日期格式（捕获日期格式错误）
        try:
            etf_price_history_df['date'] = pd.to_datetime(etf_price_history_df['date'])
        except Exception as e:
            raise ValueError(f"日期格式错误，无法转换：{str(e)}（请确保格式为YYYY-MM-DD或YYYY/MM/DD）")
        
        etf_price_history_df = etf_price_history_df.sort_values(['code', 'date'])
        
        # 校验必需列是否存在
        required_price_cols = ['code', 'close']
        missing_cols = [col for col in required_price_cols if col not in etf_price_history_df.columns]
        if missing_cols:
            raise KeyError(f"价格数据缺少必需列：{missing_cols}（需包含'code'和'close'）")
        
        # 校验数据是否为空
        if etf_price_history_df.empty:
            raise ValueError("价格数据为空，请检查数据源")
        
        # 2. 转换为宽格式（捕获透视表错误，如重复日期）
        try:
            price_wide = etf_price_history_df.pivot_table(
                index='date', 
                columns='code', 
                values='close',
                aggfunc='first'  # 处理同一日期多个收盘价：取第一个
            )
        except Exception as e:
            raise RuntimeError(f"生成价格宽表失败：{str(e)}（可能存在同一ETF同一日期多个价格）")
        
        # 校验宽表是否为空
        if price_wide.empty:
            raise ValueError("透视后价格表为空，可能是'code'或'date'列数据无效")
        
        # 3. 计算相关性矩阵
        try:
            correlation_matrix = price_wide.corr()
        except Exception as e:
            raise RuntimeError(f"计算相关性矩阵失败：{str(e)}（可能是价格数据中存在非数值）")
        
        # 4. 构建代码-名称映射（增加校验）
        if 'ts_code' not in etf_name_list_df.columns or 'name' not in etf_name_list_df.columns:
            raise KeyError("名称数据必须包含'ts_code'和'name'列")
        
        code_name_map = dict(zip(etf_name_list_df['ts_code'], etf_name_list_df['name']))
        if not code_name_map:
            raise ValueError("名称映射为空，请检查industry_df2.csv数据")
        
        # 5. 过滤无效ETF（仅保留价格表和名称表都存在的ETF）
        valid_codes = [code for code in correlation_matrix.columns if str(code) in code_name_map]
        if len(valid_codes) == 0:
            raise ValueError("没有找到匹配的ETF代码！请检查：1. 价格表'code'与名称表'ts_code'格式是否一致（如是否都带.SS） 2. 是否有共同的ETF代码")
        
        correlation_matrix = correlation_matrix.loc[valid_codes, valid_codes]
        
        # 6. 重命名标签（名称+代码）
        etf_labels = {code: f"{code_name_map[str(code)]}\n({code})" for code in valid_codes}
        correlation_matrix.index = [etf_labels[code] for code in correlation_matrix.index]
        correlation_matrix.columns = [etf_labels[code] for code in correlation_matrix.columns]
        
        # 7. 按平均相关性排序
        if sort_by_correlation:
            avg_corr = correlation_matrix.mean().sort_values(ascending=False)
            correlation_matrix = correlation_matrix.loc[avg_corr.index, avg_corr.index]
        
        # 8. 打印矩阵（控制台友好格式）
        print("\n" + "="*120)
        print("📊 ETF相关性矩阵（行/列：名称+代码）")
        print("="*120)
        
        # 表头简化显示
        header = "          "
        for col in correlation_matrix.columns:
            short_name = col.split('\n')[0][:6]
            header += f"{short_name:<10}"
        print(header)
        print("-"*120)
        
        # 数据行
        for idx, row in correlation_matrix.iterrows():
            short_idx = idx.split('\n')[0][:6]
            line = f"{short_idx:<10}"
            for val in row:
                line += f"{val:.2f}      "
            print(line)
        
        print("="*120)
        return correlation_matrix, etf_labels  # 正常返回
        
    except Exception as e:
        # 捕获所有错误，打印详细信息，并返回空矩阵和空字典（避免返回None）
        print(f"\n❌ get_matrix函数执行失败：{str(e)}")
        return pd.DataFrame(), {}  # 关键：无论如何都返回可解包的tuple

def print_top_correlations(correlation_matrix, top_n=10):
    try:
        if correlation_matrix.empty:
            print("⚠️ 相关性矩阵为空，无法打印高相关性对")
            return
        
        corr_pairs = correlation_matrix.unstack()
        corr_pairs = corr_pairs[corr_pairs < 1.0]
        corr_pairs = corr_pairs.sort_values(ascending=False)
        
        print(f"\n🔥 相关性最高的{top_n}个ETF对")
        print("-"*80)
        display_count = min(top_n, len(corr_pairs))
        if display_count == 0:
            print("⚠️ 没有找到有效相关性对（可能矩阵数据异常）")
            return
        
        for i, ((etf1, etf2), corr) in enumerate(corr_pairs.head(display_count).items(), 1):
            etf1_short = etf1.split('\n')[0]
            etf2_short = etf2.split('\n')[0]
            print(f"{i:2d}. {etf1_short:<12} ↔ {etf2_short:<12} | 相关系数：{corr:.4f}")
        print("-"*80)
    
    except Exception as e:
        print(f"\n❌ 打印高相关性对失败：{str(e)}")

def get_matrix_with_heatmap(etf_price_history_df, etf_name_list_df, 
                           sort_by_correlation=True, save_dir=None):
    try:
        # 先获取矩阵（已修复返回值问题）
        correlation_matrix, etf_labels = get_matrix(etf_price_history_df, etf_name_list_df, sort_by_correlation)
        if correlation_matrix.empty:
            print("⚠️ 相关性矩阵为空，无法生成热力图")
            return correlation_matrix
        
        # 保存路径处理
        if save_dir is None:
            save_dir = os.getcwd()
        os.makedirs(save_dir, exist_ok=True)
        save_path = os.path.join(save_dir, "etf_correlation_heatmap.png")
        
        # 绘制热力图
        etf_count = len(correlation_matrix)
        fig_size = max(10, etf_count * 0.7)
        fig, ax = plt.subplots(figsize=(fig_size, fig_size * 0.9))
        
        # 颜色映射
        cmap = mcolors.LinearSegmentedColormap.from_list(
            "corr_colormap", ["#FF4444", "#FFFF44", "#4444FF"], N=256
        )
        
        # 热力图主体
        im = ax.imshow(correlation_matrix.values, cmap=cmap, vmin=-1, vmax=1)
        
        # 坐标轴标签
        ax.set_xticks(range(etf_count))
        ax.set_yticks(range(etf_count))
        ax.set_xticklabels(correlation_matrix.columns, rotation=45, ha='right', fontsize=8)
        ax.set_yticklabels(correlation_matrix.index, fontsize=8)
        
        # 格子内数值
        for i in range(etf_count):
            for j in range(etf_count):
                corr_value = correlation_matrix.iloc[i, j]
                text_color = "white" if abs(corr_value) > 0.7 else "black"
                ax.text(j, i, f"{corr_value:.2f}", 
                        ha="center", va="center", color=text_color, fontsize=7)
        
        # 颜色条和标题
        cbar = fig.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label("相关系数（-1=完全负相关，1=完全正相关）", rotation=270, labelpad=20, fontsize=10)
        ax.set_title("ETF相关性热力图（标签格式：名称\\n(代码)）", fontsize=14, pad=20)
        
        plt.tight_layout()
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"\n✅ 热力图已保存：{save_path}")
        return correlation_matrix
    
    except Exception as e:
        print(f"\n❌ 生成热力图失败：{str(e)}")
        return pd.DataFrame()

# --------------------------
# 示例用法（真实数据读取，与你的路径完全匹配）
# --------------------------
if __name__ == "__main__":
    print("="*60)
    print("🎯 ETF相关性分析（真实数据模式 - 修复版）")
    print("="*60)
    
    try:
        # 1. 读取真实价格数据（你的路径和处理逻辑）
        print("\n1. 读取价格数据文件...")
        price_file_path = 'D:\\Documents\\投资\\量化\\market_data2.csv'
        etf_price_history_df = pd.read_csv(price_file_path)
        
        print("价格数据前5行（原始）：")
        print(etf_price_history_df.head())
        
        # 第一列重命名为'date'
        etf_price_history_df.rename(columns={etf_price_history_df.columns[0]: 'date'}, inplace=True)
        print(f"\n修改后价格数据列名：{etf_price_history_df.columns.tolist()}")
        
        # 2. 读取真实名称数据（你的路径和处理逻辑）
        print("\n2. 读取ETF名称映射文件...")
        name_file_path = 'D:\\Documents\\投资\\量化\\industry_df2.csv'
        etf_name_list_df = pd.read_csv(name_file_path)
        
        # .SH替换为.SS（统一代码格式）
        etf_name_list_df['ts_code'] = etf_name_list_df['ts_code'].str.replace('.SH', '.SS', regex=False)
        print("名称数据前5行（已替换.SH为.SS）：")
        print(etf_name_list_df.head())
        
        # 3. 关键：提前校验代码格式一致性（最常见错误点）
        print("\n3. 校验ETF代码格式一致性...")
        # 获取价格表中的code（转为字符串，避免类型不匹配）
        price_codes = set(etf_price_history_df['code'].astype(str))
        # 获取名称表中的ts_code
        name_codes = set(etf_name_list_df['ts_code'].astype(str))
        # 计算交集
        common_codes = price_codes.intersection(name_codes)
        
        print(f"   - 价格表中ETF数量：{len(price_codes)}")
        print(f"   - 名称表中ETF数量：{len(name_codes)}")
        print(f"   - 两者共有的ETF数量：{len(common_codes)}")
        
        if len(common_codes) == 0:
            raise ValueError("⚠️ 严重错误：价格表和名称表没有共同的ETF代码！")
        else:
            print(f"   ✅ 找到{len(common_codes)}个共同ETF，代码格式一致")
        
        # 4. 执行核心分析（现在get_matrix已确保返回可解包的tuple）
        print("\n4. 开始计算ETF相关性矩阵...")
        correlation_matrix, etf_labels = get_matrix(etf_price_history_df, etf_name_list_df)
        
        # 检查矩阵是否有效
        if correlation_matrix.empty:
            print("⚠️ 相关性矩阵生成失败，已终止分析")
        else:
            # 5. 打印高相关性对
            print("\n5. 展示高相关性ETF对...")
            print_top_correlations(correlation_matrix, top_n=10)
            
            # 6. 生成热力图
            print("\n6. 生成相关性热力图...")
            get_matrix_with_heatmap(
                etf_price_history_df, 
                etf_name_list_df, 
                save_dir=os.getcwd()
            )
        
        print(f"\n" + "="*60)
        print("🎉 分析流程已完成（无论结果如何，已避免崩溃）")
        print("="*60)
    
    except FileNotFoundError as e:
        print(f"\n❌ 错误：文件未找到 - {e.filename}")
        print("   请检查：1. 文件路径是否正确 2. 文件是否被Excel等程序占用 3. 文件名是否正确（如market_data2.csv是否存在）")
    
    except KeyError as e:
        print(f"\n❌ 错误：数据列缺失 - {e}")
        print("   请检查：1. 价格表是否有'date'/'code'/'close'列 2. 名称表是否有'ts_code'/'name'列")
    
    except ValueError as e:
        print(f"\n❌ 错误：数据格式无效 - {e}")
    
    except Exception as e:
        print(f"\n❌ 未知错误：{str(e)}")
        print("   建议：1. 检查market_data2.csv的'close'列是否都是数值（无文字） 2. 检查日期格式是否正确")