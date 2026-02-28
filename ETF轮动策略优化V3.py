# 動態周期 ETF 輪動策略
# 核心：在跨資產 ETF 池中，每日通過動態評分模型選擇趨勢最強、最穩定的1只 ETF 進行持有。

def initialize(context):
    """
    初始化函數，設置全局參數、股票池、基準和交易成本。
    """
    # ========== 小白可修改的參數區 ==========
    # ETF 監控池
    g.etf_pool = [
        # 境外
        "513100.SS",  # 納指ETF
        "513520.SS",  # 日經ETF
        "513030.SS",  # 德國ETF
        # 商品
        "518880.SS",  # 黃金ETF
        "159980.SZ",  # 有色ETF
        "501018.SS",  # 南方原油
        # 債券
        "511090.SS",  # 30年國債ETF
        # 國內
        "512890.SS",  # 紅利低波
        '159915.SZ',  # 創業板100
    ]
    
    # 評分模型核心參數
    g.min_days = 20  # 最小動量計算周期 (天)
    g.max_days = 60  # 最大動量計算周期 (天)
    g.auto_day = True  # 是否開啟動態周期調整。True為開，使用get_rank2；False為關，使用固定周期評分。
    g.hold_num = 1    # 目標持倉數量，始終持有得分最高的前N只
    
    # 風控參數
    g.drop_3day_threshold = 0.05  # 3日累計跌幅風控閾值 (5%)
    g.single_day_drop_threshold = 0.05  # 單日跌幅風控閾值 (5%)
    g.premium_threshold = 0.05  # ETF溢價率懲罰閾值 (5%)
    g.score_floor = 0  # 得分下限，低於此分的ETF不考慮
    g.score_cap = 6   # 得分上限，高於此分的ETF視為異常，不考慮
    
    # ========== 策略固定設置 ==========
    # 設置股票池
    set_universe(g.etf_pool)
    # 設置基準為滬深300
    set_benchmark('000300.SS')
    # 設置佣金和滑點 (回測用)
    set_commission(commission_ratio=0.0003, min_commission=5)  # 萬三佣金，最低5元
    set_slippage(slippage=0.002)  # 0.2%的滑點
    # 初始化一些記錄變量
    g.last_rank = []  # 記錄上一期評分結果
    
def before_trading_start(context, data):
    """
    盤前運行函數 (可選)。這裏可以用來初始化每日的數據。
    """
    pass

def handle_data(context, data):
    """
    主策略邏輯，每日收盤前運行。
    """
    # 策略在每天收盤前運行，確保能獲取到當日收盤價
    current_time = context.blotter.current_dt.time()
    # 設置在收盤前10分鐘（14:50）執行調倉
    if current_time.hour == 14 and current_time.minute == 50:
        trade(context, data)
        
def after_trading_end(context, data):
    """
    盤後運行函數 (可選)。
    """
    pass

# ================== 核心評分函數 ==================
def get_rank2(security_list, context, data):
    """
    動態周期評分函數 (get_rank2)。
    根據波動率自適應調整動量計算周期，並疊加嚴格的風控規則。
    
    參數:
        security_list: 待評分的ETF代碼列表
        context: 策略上下文對象
        data: K線數據對象
        
    返回:
        final_list: 按得分從高到低排序的ETF代碼列表
    """
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta
    
    # 步驟1: 初始化數據容器
    score_df = pd.DataFrame(index=security_list, 
                            columns=['annualized_returns', 'r2', 'score'])
    
    # 步驟2: 遍歷ETF池，逐個計算評分
    for sec in security_list:
        try:
            # 子步驟2.1: 獲取並校驗歷史數據
            # 獲取過去 g.max_days+10 天的日線數據
            lookback_days = g.max_days + 10
            hist = get_history(lookback_days, '1d', ['close', 'high', 'low'], 
                              sec, fq=None, include=False)
            if hist is None or len(hist) < g.max_days:
                # 數據不足，跳過該ETF
                continue
                
            # 檢查缺失值
            if hist['close'].isna().sum() > 0.6 * len(hist) or \
               hist['high'].isna().sum() > 0.6 * len(hist) or \
               hist['low'].isna().sum() > 0.6 * len(hist):
                continue
                
            # 轉為numpy數組提高計算效率
            prices = hist['close'].values
            highs = hist['high'].values
            lows = hist['low'].values
            
            # 子步驟2.2: 動態計算動量參考周期 (lookback 天數)
            # 計算長期ATR (基於g.max_days)
            def calc_atr(highs, lows, closes, period):
                """計算給定周期的平均真實波幅(ATR)"""
                tr = np.maximum(
                    highs[1:] - lows[1:],
                    np.maximum(
                        np.abs(highs[1:] - closes[:-1]),
                        np.abs(lows[1:] - closes[:-1])
                    )
                )
                if len(tr) >= period:
                    return np.mean(tr[-period:])
                return np.mean(tr) if len(tr) > 0 else 0
                
            long_atr = calc_atr(highs, lows, prices, g.max_days)
            short_atr = calc_atr(highs[-g.min_days:], lows[-g.min_days:], 
                                 prices[-g.min_days:], g.min_days)
            
            # 計算短期/長期ATR比值，並限制上限
            if long_atr > 0:
                atr_ratio = min(0.9, short_atr / long_atr)
            else:
                atr_ratio = 0.9
                
            # 根據波動率反向調整lookback周期
            lookback = int(g.min_days + (g.max_days - g.min_days) * (1 - atr_ratio))
            lookback = max(g.min_days, min(g.max_days, lookback))  # 限制在[min_days, max_days]之間
            
            # 子步驟2.3: 提取用於評分的價格序列
            # 獲取當前價格
            current_price = data[sec]['close'] if sec in data else prices[-1]
            price_series = np.append(prices, current_price)
            # 截取最後lookback天的數據
            price_series = price_series[-lookback:]
            
            # 子步驟2.4: 計算加權線性回歸的年化收益率
            y = np.log(price_series)  # 因變量：對數價格
            x = np.arange(len(price_series))  # 自變量：時間序列
            weights = np.linspace(1, 2, len(price_series))  # 權重：線性遞增
            
            # 加權線性回歸
            x_mean = np.average(x, weights=weights)
            y_mean = np.average(y, weights=weights)
            
            cov = np.average((x - x_mean) * (y - y_mean), weights=weights)
            var = np.average((x - x_mean)**2, weights=weights)
            
            if var > 0:
                slope = cov / var  # 斜率 = 每日對數收益率
            else:
                slope = 0
                
            # 年化收益率轉換
            annualized_return = np.exp(slope * 250) - 1
            
            # 子步驟2.5: 計算加權 R² (判定系數)
            y_pred = slope * x + (y_mean - slope * x_mean)  # 預測值
            ss_res = np.average((y - y_pred)**2, weights=weights)  # 殘差平方和
            ss_tot = np.average((y - y_mean)**2, weights=weights)  # 總平方和
            
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            # 子步驟2.6: 計算基礎得分
            base_score = annualized_return * r2
            
            # 子步驟2.7: 風控規則
            score = base_score
            
            # 1. 跌幅風控 (觸發則得分清零)
            # 條件1: 近3天內任意一天跌幅超5%
            drop_1day = any(prices[i]/prices[i-1] < (1 - g.single_day_drop_threshold) 
                          for i in range(-3, 0) if i < -1)
            # 條件2: 連續3天下跌且累計跌幅超5%
            drop_3day_consecutive = (len(prices) >= 4 and 
                                    prices[-1]/prices[-4] < (1 - g.drop_3day_threshold) and
                                    all(prices[i] < prices[i-1] for i in range(-2, 1)))
            # 條件3: 連續3天下跌(跨4-5天)且累計跌幅超5%
            drop_3day_any = (len(prices) >= 5 and 
                           prices[-2]/prices[-5] < (1 - g.drop_3day_threshold))
            
            if drop_1day or drop_3day_consecutive or drop_3day_any:
                score = 0
                log.info("ETF %s 觸發跌幅風控，得分清零" % sec)
            
            # 2. 溢價率懲罰 (觸發則得分減1)
            # 註: 文檔中未提供獲取ETF實時單位淨值的API，此處需要您根據實際數據源實現
            # 以下是偽代碼邏輯，您需要替換為實際的溢價率計算
            try:
                # 假設通過某個API獲取ETF的實時IOPV(參考單位淨值)
                # iopv = get_etf_info(sec)['IOPV']  # 此函數需您根據實際API實現
                # premium_rate = (current_price - iopv) / iopv
                # if premium_rate >= g.premium_threshold:
                #     score -= 1
                #     log.info("ETF %s 溢價率 %.2f%%，觸發懲罰" % (sec, premium_rate*100))
                pass
            except:
                # 如果無法獲取溢價率，跳過此項檢查
                pass
            
            # 存儲結果
            score_df.loc[sec, 'annualized_returns'] = annualized_return
            score_df.loc[sec, 'r2'] = r2
            score_df.loc[sec, 'score'] = score
            
        except Exception as e:
            log.warn("計算ETF %s 評分時出錯: %s" % (sec, str(e)))
            continue
    
    # 步驟3: 篩選並排序最終標的
    # 移除得分為NaN的記錄
    score_df = score_df.dropna(subset=['score'])
    if score_df.empty:
        return []
    
    # 按得分降序排列
    score_df = score_df.sort_values('score', ascending=False)
    
    # 過濾得分
    filtered = score_df[(score_df['score'] > g.score_floor) & 
                        (score_df['score'] < g.score_cap)]
    
    # 返回ETF代碼列表
    final_list = filtered.index.tolist()
    
    # 記錄評分結果
    if len(final_list) > 0:
        log.info("本期評分前3名: %s" % str([(sec, filtered.loc[sec, 'score']) 
                                          for sec in final_list[:3]]))
    else:
        log.info("本期無符合條件的ETF")
        
    return final_list

def get_rank(security_list, context, data):
    """
    基礎版評分函數 (固定周期)。
    作為對比或備用方案，使用固定g.max_days周期計算動量。
    """
    import pandas as pd
    import numpy as np
    
    score_df = pd.DataFrame(index=security_list, 
                            columns=['annualized_returns', 'r2', 'score'])
    
    for sec in security_list:
        try:
            # 獲取歷史數據
            hist = get_history(g.max_days, '1d', 'close', sec, fq=None, include=False)
            if hist is None or len(hist) < g.max_days:
                continue
                
            prices = hist['close'].values
            current_price = data[sec]['close'] if sec in data else prices[-1]
            price_series = np.append(prices, current_price)[-g.max_days:]
            
            # 計算年化收益率 (簡單版)
            y = np.log(price_series)
            x = np.arange(len(price_series))
            weights = np.linspace(1, 2, len(price_series))
            
            x_mean = np.average(x, weights=weights)
            y_mean = np.average(y, weights=weights)
            
            cov = np.average((x - x_mean) * (y - y_mean), weights=weights)
            var = np.average((x - x_mean)**2, weights=weights)
            
            slope = cov / var if var > 0 else 0
            annualized_return = np.exp(slope * 250) - 1
            
            # 計算R²
            y_pred = slope * x + (y_mean - slope * x_mean)
            ss_res = np.average((y - y_pred)**2, weights=weights)
            ss_tot = np.average((y - y_mean)**2, weights=weights)
            r2 = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
            
            # 基礎得分
            base_score = annualized_return * r2
            
            # 簡單風控：如果當前價格低於20日均線，得分減半
            if len(prices) >= 20:
                ma20 = np.mean(prices[-20:])
                if current_price < ma20 * 0.95:  # 低於20日均線5%
                    base_score *= 0.5
            
            score_df.loc[sec, 'annualized_returns'] = annualized_return
            score_df.loc[sec, 'r2'] = r2
            score_df.loc[sec, 'score'] = base_score
            
        except Exception as e:
            continue
    
    score_df = score_df.dropna(subset=['score'])
    if score_df.empty:
        return []
    
    score_df = score_df.sort_values('score', ascending=False)
    filtered = score_df[score_df['score'] > 0]
    
    return filtered.index.tolist()

def trade(context, data):
    """
    交易執行函數。
    根據評分結果，賣出非目標持倉，買入評分最高的ETF。
    """
    # 獲取當前持倉
    current_positions = {pos.sid: pos for pos in context.portfolio.positions.values()}
    
    # 步驟1: 獲取目標ETF列表
    if g.auto_day:
        target_etfs = get_rank2(g.etf_pool, context, data)
    else:
        target_etfs = get_rank(g.etf_pool, context, data)
    
    # 如果沒有符合條件的ETF，則清倉觀望
    if not target_etfs:
        log.info("無符合條件的目標ETF，執行清倉")
        for sec, pos in current_positions.items():
            if pos.amount > 0:
                order_target(sec, 0)
                log.info("賣出 %s, 數量 %d" % (sec, pos.amount))
        return
    
    # 只取前g.hold_num只
    target_etfs = target_etfs[:g.hold_num]
    log.info("本期目標ETF: %s" % target_etfs)
    
    # 步驟2: 賣出非目標持倉
    for sec, pos in current_positions.items():
        if sec not in target_etfs and pos.amount > 0:
            order_target(sec, 0)
            log.info("賣出非目標持倉 %s, 數量 %d" % (sec, pos.amount))
    
    # 步驟3: 買入目標ETF
    available_cash = context.portfolio.cash
    for sec in target_etfs:
        # 如果已經持有，則跳過
        if sec in current_positions and current_positions[sec].amount > 0:
            log.info("已持有目標ETF %s，跳過" % sec)
            continue
            
        # 計算可買數量 (全倉買入)
        current_price = data[sec]['close'] if sec in data else 0
        if current_price > 0:
            # 計算可買數量 (向下取整)
            amount_to_buy = int(available_cash / current_price / 100) * 100  # 按手數(100股)取整
            if amount_to_buy >= 100:  # 至少買1手
                order(sec, amount_to_buy)
                log.info("買入 %s, 價格 %.3f, 數量 %d" % (sec, current_price, amount_to_buy))
                available_cash -= amount_to_buy * current_price
            else:
                log.info("現金不足，無法買入 %s" % sec)