import akshare as ak

def init(script, handler, depend):
    """
    初始化函数，用于初始化银行间同业拆借利率
    """
    rate_interbank_df = ak.rate_interbank(
        market="上海银行同业拆借市场", symbol="Shibor人民币", indicator="3月"
    )
    return rate_interbank_df

def depend(script, handler):
    """
    依赖函数，用于依赖银行间同业拆借利率
    """
    return get_foucs_params()

def iteration(script, handler, depend_item):
    """
    遍历函数，用于遍历银行间同业拆借利率的参数
    """
    market = depend_item["market"]
    symbol = depend_item["symbol"]
    indicator = depend_item["indicator"]
    rate_interbank_df = ak.rate_interbank(market=market, symbol=symbol, indicator=indicator)
    # 添加参数列
    rate_interbank_df["market"] = market
    rate_interbank_df["symbol"] = symbol
    rate_interbank_df["indicator"] = indicator
    return rate_interbank_df

def example(script, handler, depend_item):
    """
    示例函数，用于示例银行间同业拆借利率的参数
    """
    depend_item = depend(script, handler)[0]
    market = depend_item["market"]
    symbol = depend_item["symbol"]
    indicator = depend_item["indicator"]
    rate_interbank_df = ak.rate_interbank(market=market, symbol=symbol, indicator=indicator)
    # 添加参数列
    rate_interbank_df["market"] = market
    rate_interbank_df["symbol"] = symbol
    rate_interbank_df["indicator"] = indicator
    return rate_interbank_df

def get_foucs_params():
    return [
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "隔夜"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "3月"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "1年"},
         {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "3月"},
         {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "3月"},
         {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "1周"},
         {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "3月"},
         {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "1年"},
         {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "隔夜"},
         {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "3月"},
         {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "1年"},
         {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "隔夜"},
         {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "3月"},
         {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "1年"},
    ]

def print_params(script, handler, depend):
    """
    打印参数函数，用于打印银行间同业拆借利率的参数
    """
    # 定义所有银行间同业拆借市场的参数组合
    params_list = [
        # 上海银行同业拆借市场
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "隔夜"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "1周"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "2周"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "1月"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "3月"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "6月"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "9月"},
        {"market": "上海银行同业拆借市场", "symbol": "Shibor人民币", "indicator": "1年"},
        # 中国银行同业拆借市场
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "隔夜"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "1周"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "2周"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "3周"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "1月"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "2月"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "3月"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "4月"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "6月"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "9月"},
        {"market": "中国银行同业拆借市场", "symbol": "Chibor人民币", "indicator": "1年"},
        # 伦敦银行同业拆借市场
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "隔夜"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "1周"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "1月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "2月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "3月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor英镑", "indicator": "8月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "隔夜"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "1周"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "1月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "2月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "3月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor美元", "indicator": "8月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor欧元", "indicator": "隔夜"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor欧元", "indicator": "1周"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor欧元", "indicator": "1月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor欧元", "indicator": "2月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor欧元", "indicator": "3月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor欧元", "indicator": "8月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor日元", "indicator": "隔夜"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor日元", "indicator": "1周"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor日元", "indicator": "1月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor日元", "indicator": "2月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor日元", "indicator": "3月"},
        {"market": "伦敦银行同业拆借市场", "symbol": "Libor日元", "indicator": "8月"},
        # 欧洲银行同业拆借市场
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "1周"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "2周"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "3周"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "1月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "2月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "3月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "4月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "5月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "6月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "7月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "8月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "9月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "10月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "11月"},
        {"market": "欧洲银行同业拆借市场", "symbol": "Euribor欧元", "indicator": "1年"},
        # 香港银行同业拆借市场
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "隔夜"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "1周"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "2周"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "1月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "2月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "3月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "4月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "5月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "6月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "7月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "8月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "9月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "10月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "11月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor港币", "indicator": "1年"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "隔夜"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "1周"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "2周"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "1月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "2月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "3月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "4月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "5月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "6月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "7月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "8月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "9月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "10月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "11月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor美元", "indicator": "1年"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "隔夜"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "1周"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "2周"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "1月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "2月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "3月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "6月"},
        {"market": "香港银行同业拆借市场", "symbol": "Hibor人民币", "indicator": "1年"},
        # 新加坡银行同业拆借市场
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor星元", "indicator": "1月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor星元", "indicator": "2月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor星元", "indicator": "3月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor星元", "indicator": "6月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor星元", "indicator": "9月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor星元", "indicator": "1年"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor美元", "indicator": "1月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor美元", "indicator": "2月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor美元", "indicator": "3月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor美元", "indicator": "6月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor美元", "indicator": "9月"},
        {"market": "新加坡银行同业拆借市场", "symbol": "Sibor美元", "indicator": "1年"},
    ]
    
    # 如果script.params已包含参数，则打印它
    if hasattr(script, 'params') and script.params:
        print("script.params中已有的参数:")
        print(script.params)
        print()
    
    # 打印所有可能的参数组合
    print("银行间同业拆借利率的所有参数组合:")
    for i, params in enumerate(params_list, 1):
        print(f"参数组合 {i}:")
        print(f"  market: {params['market']}")
        print(f"  symbol: {params['symbol']}")
        print(f"  indicator: {params['indicator']}")
        print()