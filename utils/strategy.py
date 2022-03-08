# to update

import operator

OPS = { "+": operator.add, "-": operator.sub, '>': operator.gt, '<': operator.lt, '=': operator.eq, '>=': operator.ge, '<=': operator.le}


class Strategy():
    def __init__(self, name, stock_filter=None):
        self.name = name
        self.rules = {}
        self.stock_filter = stock_filter


    def add_condition(self, col, op, val=None, var=None, ratio=None):
        if col not in self.rules:
            self.rules[col] = []
        if var:
            if ratio is not None:
                self.rules[col].append({'op': op, 'var': var, 'ratio': ratio})
            else:
                self.rules[col].append({'op': op, 'var': var})
        elif val is not None:
            self.rules[col].append({'op': op, 'val': val})


    def get_result(self, df, trade_date=None, verbose=False):
        if verbose:
            print(self.rules)

        if trade_date:
            print(f'Searching {trade_date}...')
            df = df.xs(trade_date, level='trade_date')

        if self.stock_filter:
            df = self.stock_filter.filter(df)

        if len(df) == 0:
            print('There is no data.')
            result = df
        else:
            conditions = True
            for k, v in self.rules.items():
                for cond in v:
                    op = OPS[cond['op']]
                    if 'val' in cond:
                        tgt = cond['val']
                    else:
                        tgt = df[cond['var']]
                    if 'ratio' in cond:
                        tgt = tgt * cond['ratio']
                    conditions = op(df[k], tgt) & conditions
            result = df[conditions]

        return result, list(self.rules.keys())


def strategy_on_region(df, strategies, start_date, end_date, show_invalids=False, cols=None, sample_size=5):
    df = df.xs(slice(start_date, end_date), level='trade_date', drop_level=False)
    res_dfs = {}
    keys = []

    for strategy in strategies:
        tmp_res, tmp_keys = filter_by_strategy(df, strategy['filter'])
        if len(tmp_res) == 0:
            continue
        keys += tmp_keys
        bonus_filter = strategy['bonus'] if 'bonus' in strategy else {'current': [{'op': '>', 'val': np.inf}]}
        tmp_res, tmp_b_keys = filter_by_strategy(tmp_res, bonus_filter, only_mark=True)
        keys += tmp_b_keys
        tmp_res.loc[:,'stra'] = strategy['name']
        res_dfs[strategy['name']] = tmp_res

    if len(res_dfs) > 0:
        # merge result & strategy signatures
        res_all = pd.concat(list(res_dfs.values()), axis=0, join='outer', ignore_index=False, keys=None,
              levels=None, names=None, verify_integrity=False, copy=True, sort=True)
        res_all.sort_index(level='trade_date', inplace=True)
        res_all.loc[:, 'stras'] = res_all.reset_index().groupby(['ts_code','trade_date']).stra.apply(lambda x: ','.join(x.to_list())).sort_index(level='trade_date')

        # deduplicate
        res_all = res_all.loc[~res_all.index.duplicated(keep='last')]
        res_all.loc[:,'suggest_bid'] = res_all.current * 1.02

        # build display object
        if cols is None:
            cols = list(dict.fromkeys(['name', 'stras', 'bonus', 'pre_close', 'current', 'open_preclose_pct', 'suggest_bid'] + keys))
        else:
            cols = list(dict.fromkeys(['name', 'stras', 'pre_close', 'current', 'open_preclose_pct', 'suggest_bid'] + cols + keys))
        if len(res_all) > 0:
            display(res_all.tail(sample_size)[cols])
    else:
        res_all = pd.DataFrame()

    print(f'Strategies got {len(res_all)} results.')
    return res_all, res_dfs, df


def filter_by_strategy(df, filters, only_mark=False):
    if len(df) == 0:
        return df, list(filters.keys())

    ops = { "+": operator.add, "-": operator.sub, '>': operator.gt, '<': operator.lt}
    conditions = True
    print(filters)
    for k, v in filters.items():
        if isinstance(v, list):
            for cond in v:
                op = ops[cond['op']]
                if 'val' in cond:
                    tgt = cond['val']
                else:
                    tgt = df[cond['var']]
                if 'ratio' in cond:
                    tgt = tgt * cond['ratio']
                conditions = op(df[k], tgt) & conditions
        else:
            op = ops[v['op']]
            if 'val' in v:
                tgt = v['val']
            else:
                tgt = df[v['var']]
            if 'ratio' in v:
                tgt = tgt * v['ratio']
            conditions = op(df[k], tgt) & conditions
    if only_mark:
        tmp = df.copy()
        tmp.loc[conditions, 'bonus'] = True
        tmp.loc[~conditions, 'bonus'] = False
    else:
        tmp = df[conditions]
    return tmp, list(filters.keys())


def test_strategy(df, strategy, test_date_end, test_date_start=None, show_invalids=False, cols=None, sample_size=5):
    if test_date_start is None:
        test_date_start = test_date_end
    print(f'Showing strategy [{strategy["name"]}] results from {test_date_start} to {test_date_end}:')
    df = df.xs(slice(test_date_start, test_date_end), level='trade_date', drop_level=False)
    df = strategy['filter'].filter(df)

    strategy_conditions = strategy['conditions']
    res_df, keys = filter_by_strategy(df, strategy_conditions)
    res_df.loc[:,'suggest_bid'] = res_df.current * 1.02
    if cols is None:
        cols = list(dict.fromkeys(['name', 'current', 'suggest_bid', 'pre_close', 'open_preclose_pct', 'pre_auc_vol', 'auc_vol'] + keys))
    else:
        cols = list(dict.fromkeys(cols + keys))

    if len(res_df) > 0 and sample_size > 0:
        tmp = res_df.tail(sample_size)[cols]
        display(tmp)
    print(f'Strategy got {len(res_df)} results.')

    if show_invalids:
        print(f'Sample of invalid results:')
        invalids = df.tail(sample_size)[cols]
    return res_df, df


