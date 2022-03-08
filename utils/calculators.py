#####################################################################
## Calculaters
#####################################################################

# 当日是否涨跌停
def set_up_stop(df):
    df.loc[:, 'up_stop_price'] = df.pre_close.map(up_stop_price)
    df.loc[:, 'dn_stop_price'] = df.pre_close.map(down_stop_price)
    df.loc[:, 'is_up_stop'] = (df.up_stop_price == df.close)
    return df


def up_stop_price(close_price):
    return cn_round_price(close_price * (1 + 0.1))


def down_stop_price(close_price):
    return cn_round_price(close_price * (1 - 0.1))


def cn_round_price(price):
    price *= 1000
    if str(price).split(".")[0][-1] == str(5):
        price += 5
    price /= 1000
    price = round(price, 2)
    return price


def to_pct(num):
    return cn_round_price(num * 100)
