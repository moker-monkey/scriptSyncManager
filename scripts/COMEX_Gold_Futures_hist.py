import akshare as ak

# 不同类型的数据脚本有不同类型的初始化函数和周期函数，需要根据具体情况进行编写。


def init(script, handler, depend):
    """
    初始化函数，用于创建数据库表和插入初始数据
    """
    # COMEX黄金期货历史数据
    # futures_foreign_hist_df = ak.futures_foreign_hist(symbol="GC")
    symbol_list = get_symbol_list(script, handler)
    print(symbol_list)
    # return futures_foreign_hist_df


def period(script, handler, depend):
    """
    周期函数，用于获取COMEX黄金期货价格,每日12点后执行
    """
    return get_real_time_price(script, handler)


def get_symbol_list(script, handler):
    """
    获取外盘期货的交易符号列表
    """
    futures_foreign_symbol_df = ak.futures_foreign_commodity_subscribe_exchange_symbol()
    return futures_foreign_symbol_df


def get_real_time_price(script, handler):
    """
    获取COMEX黄金期货的实时价格
    """
    symbol_list = get_symbol_list(script, handler)
    futures_foreign_real_time_df = ak.futures_foreign_commodity_realtime(
        symbol=symbol_list
    )
    return futures_foreign_real_time_df
