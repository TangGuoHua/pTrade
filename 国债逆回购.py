import math

def initialize(context):
    # 初始化策略
    run_daily(context, get_nihui_gou, time='15:01')

def handle_data(context, data):
    pass

def get_nihui_gou(context):
    print('进入逆回购')

    shen = get_snapshot('131810.SZ')['131810.SZ']['last_px']
    hu = get_snapshot('204001.SS')['204001.SS']['last_px']

    if shen >= hu:
        option = '131810.SZ'
        print('借出深市, 价格: %f' % shen)
    else:
        option = '204001.SS'
        print('借出沪市, 价格: %f' % hu)

    cash = context.portfolio.cash
    amount = cash * 0.001
    amount = math.floor(amount)
    print('可用资金: ', cash)
    print('可用数量: ', amount)
    order(option, -10 * amount)