

def print_auction(df, row_index=0):
    print(df.iloc[row_index:row_index+1,].filter(regex='a\d_v', axis=1))
    print(df.iloc[row_index:row_index+1,].filter(regex='b\d_v', axis=1))
    print(f'Price: {df.at[row_index, "current"]}, A1: {df.at[row_index, "a1_p"]}, B1: {df.at[row_index, "b1_p"]}, Volume: {df.at[row_index, "volume"]}')
