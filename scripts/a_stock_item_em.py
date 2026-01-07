import akshare as ak

def init(script,handler,depend):
    # 初始化都是直接覆盖表
    return ak.stock_info_a_code_name()

def depend(script,handler):
    # 先找到依赖的股票代码
    return handler.config.get_table_data("a_stock_list")

def iteration(script,handler,depend_item):
    # 遍历依赖的股票代码
    print('遍历依赖的股票代码',depend_item['code'])
    # 查看股票详情并返回DataFrame用于存储
    return print_a_stock_item_em(depend_item['code'])


def print_a_stock_item_em(symbol):
    # 查看股票详情
    if symbol is None:
        raise ValueError("股票代码不能为空")
    # 获取股票详情并转换为DataFrame格式用于存储
    df = ak.stock_individual_info_em(symbol=symbol).set_index('item')['value'].to_frame().T
    print(df)
    return df

if __name__ == "__main__":
    print_a_stock_item_em("002700")

