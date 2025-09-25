import pandas as pd
import numpy as np
from collections import defaultdict
import ast



def cumulative_prices(prices_lists):
    cumulative = []
    result = []
    for prices in prices_lists:
        cumulative.extend(prices)
        result.append(cumulative.copy())
    return result


def calculate_grouped_sums(prices_per_buyer):
    sums = defaultdict(float)
    for id, price in prices_per_buyer:
        sums[id] += price
    return [price for id, price in sums.items()]


def calc_monetization_cumulatives(df):
    df.sort_values(by=['variation', 'dt'], inplace=True)
    df.to_csv('temp_df.csv')

    cumsum_columns = [
        'members', 'install_cnt', 'subscriber_cnt', 'access_cnt', 'access_instant_cnt',
        'access_ex_trial_cnt', 'access_trial_cnt', 'active_trial_cnt', 'trial_subscriber_cnt',
        'charged_trial_cnt', 'active_charged_trial_cnt', 
        'access_otp_cnt', 'cancel_trial_cnt', 'trial_buyer_cnt', 'late_charged_cnt',
        'buyer_cnt', 'charge_cnt', 'refund_14d_cnt', 'cancel_14d_cnt', 'cancel_1m_cnt', 'revenue', 'lifetime_revenue',
        'recurrent_charge_cnt', 'recurrent_revenue', 'trial_revenue', 'active_trial_revenue',
        'upgrade_cnt', 'upgrade_revenue'
    ]

    for col in cumsum_columns:
        df[f'{col}_cum'] = df.groupby('variation')[col].cumsum()

    df['accesses_per_subscriber'] = df['access_cnt_cum'] / df['subscriber_cnt_cum']
    df['member -> install, %'] = df['install_cnt_cum'] / df['members_cum'] * 100
    df['member -> subscriber, %'] = df['subscriber_cnt_cum'] / df['members_cum'] * 100
    df['trial -> cancel, %'] = df['cancel_trial_cnt_cum'] / df['access_trial_cnt_cum'] * 100
    df['trial -> charge, %'] = df['charged_trial_cnt_cum'] / df['access_trial_cnt_cum'] * 100
    df['active trial -> charge, %'] = df['active_charged_trial_cnt_cum'] / df['active_trial_cnt_cum'] * 100
    df['trial subscriber -> buyer, %'] = df['trial_buyer_cnt_cum'] / df['trial_subscriber_cnt_cum'] * 100
    df['subscriber -> buyer, %'] = df['buyer_cnt_cum'] / df['subscriber_cnt_cum'] * 100
    df['member -> buyer, %'] = df['buyer_cnt_cum'] / df['members_cum'] * 100
    df['subscription -> charge, %'] = df['charge_cnt_cum'] / df['access_cnt_cum'] * 100
    df['charge -> 14d cancel, %'] = df['cancel_14d_cnt_cum'] / df['charge_cnt_cum'] * 100
    df['charge -> 14d refund, %'] = df['refund_14d_cnt_cum'] / df['charge_cnt_cum'] * 100
    df['charge -> 1m cancel, %'] = df['cancel_1m_cnt_cum'] / df['charge_cnt_cum'] * 100
    df['arppu'] = df['revenue_cum'] / df['buyer_cnt_cum']
    df['aov'] = df['revenue_cum'] / df['charge_cnt_cum']
    df['exp_arpu'] = df['revenue_cum'] / df['members_cum']
    df['lifetime_arpu'] = df['lifetime_revenue_cum'] / df['members_cum']
    df['exp_trial_arpu'] = df['trial_revenue_cum'] / df['members_cum']
    df['exp_instant_arpu'] = (df['revenue_cum'] - df['trial_revenue_cum']) / df['members_cum']

    # df['prices'] = df['prices'].apply(ast.literal_eval)
    # df['prices_per_buyer'] = df['prices_per_buyer'].apply(ast.literal_eval)
    
    # df['prices_agg'] = df.groupby('variation')['prices'].transform(cumulative_prices)
    # df['prices_per_buyer_agg'] = df.groupby('variation')['prices_per_buyer'].transform(cumulative_prices)

    # df['grouped_sums'] = df['prices_per_buyer_agg'].apply(calculate_grouped_sums)
    arpu_df = calc_cum_mean_variance(df, 'exp_arpu', 'arpu_var', 'members')
    arpu_df.drop(columns=['exp_arpu_cum'], inplace=True)
    lifetime_arpu_df = calc_cum_mean_variance(df, 'lifetime_arpu', 'lifetime_arpu_var', 'members')
    lifetime_arpu_df.drop(columns=['lifetime_arpu_cum'], inplace=True)
    # rename arpu_var to exp_arpu_var
    arpu_df.rename(columns={'arpu_var_cum': 'exp_arpu_var_cum'}, inplace=True)
    arppu_df = calc_cum_mean_variance(df, 'arppu', 'arppu_var', 'members')
    arppu_df.drop(columns=['arppu_cum'], inplace=True)
    # todo: fix to aov later
    aov_df = calc_cum_mean_variance(df, 'aov', 'arppu_var', 'members')
    aov_df.drop(columns=['aov_cum'], inplace=True)
    # rename arppu_var to aov_var
    aov_df.rename(columns={'arppu_var_cum': 'aov_var_cum'}, inplace=True)
    df = df.merge(arpu_df, on=['dt', 'variation'], how='left')
    df = df.merge(lifetime_arpu_df, on=['dt', 'variation'], how='left')
    df = df.merge(arppu_df, on=['dt', 'variation'], how='left')
    df = df.merge(aov_df, on=['dt', 'variation'], how='left')
    df.to_csv("tt.csv")
    # df = df.assign(
    #     aov_var = lambda x: x.apply(lambda row: np.var(row['prices_agg']), axis=1),
    #     arppu_var = lambda x: x.apply(lambda row: np.var(row['grouped_sums']), axis=1),
    #     exp_arpu_var = lambda x: x.apply(lambda row: np.var(np.append(row['grouped_sums'], np.zeros(row['members_cum'] - row['buyer_cnt_cum']))), axis=1)
    # )
    
    df['trial share, %'] = df['access_trial_cnt_cum'] / df['access_cnt_cum'] * 100

    final_columns = [
        'dt', 'variation', 'members_cum', 'install_cnt_cum', 'subscriber_cnt_cum', 'access_cnt_cum',
        'access_instant_cnt_cum', 'access_ex_trial_cnt_cum', 'access_trial_cnt_cum',
        'active_trial_cnt_cum', 
        'trial share, %',
        'member -> install, %',
        'accesses_per_subscriber', 'member -> subscriber, %', 'trial -> cancel, %',
        'trial -> charge, %', 'active trial -> charge, %', 
        'trial subscriber -> buyer, %', 'subscriber -> buyer, %',
        'member -> buyer, %', 'subscription -> charge, %', 'charge -> 14d cancel, %',
        'charge -> 14d refund, %',
        'charged_trial_cnt_cum', 'active_charged_trial_cnt_cum', 'access_otp_cnt_cum', 
        'trial_subscriber_cnt_cum', 'cancel_trial_cnt_cum',
        'charge_cnt_cum', 'refund_14d_cnt_cum', 'buyer_cnt_cum', 'cancel_14d_cnt_cum', 'cancel_1m_cnt_cum', 'revenue_cum', 'lifetime_revenue_cum',
        'charge -> 1m cancel, %', 'arppu', 'aov', 'exp_arpu', 'lifetime_arpu', 'exp_trial_arpu', 'exp_instant_arpu',
        # 'aov_var', 'arppu_var', 
        # 'exp_arpu_var'
        'aov_var_cum', 'arppu_var_cum', 'exp_arpu_var_cum', 'lifetime_arpu_var_cum'
    ]
    

    result_df = df[final_columns]
    # rename columns. remove _cum suffix
    result_df.columns = [col.replace('_cum', '') for col in result_df.columns]
    # convert dt to date
    result_df['dt'] = pd.to_datetime(result_df['dt']).dt.date
    # convert to format dd/mm/yyyy
    # result_df['dt'] = result_df['dt'].apply(lambda x: x.strftime('%m/%d/%y'))
    result_df['dt'] = result_df['dt'].apply(lambda x: x.strftime('%d/%m/%y'))



    return result_df


def calc_retention_cumulatives(df):
    df.sort_values(by=['variation', 'dt'], inplace=True)

    cumsum_columns = [
        'members', 'retention_1d_cnt', 'retention_7d_cnt', 'retention_14d_cnt'
    ]

    for col in cumsum_columns:
        df[f'{col}_cum'] = df.groupby('variation')[col].cumsum()

    df['retention 1d, %'] = df['retention_1d_cnt_cum'] / df['members_cum'] * 100
    df['retention 7d, %'] = df['retention_7d_cnt_cum'] / df['members_cum'] * 100
    df['retention 14d, %'] = df['retention_14d_cnt_cum'] / df['members_cum'] * 100

    final_columns = [
        'dt', 'variation', 'members_cum', 
        'retention_1d_cnt_cum', 'retention 1d, %',
        'retention_7d_cnt_cum', 'retention 7d, %',
        'retention_14d_cnt_cum', 'retention 14d, %'
    ]

    result_df = df[final_columns]
    result_df.columns = [col.replace('_cum', '') for col in result_df.columns]
    result_df['dt'] = pd.to_datetime(result_df['dt']).dt.date
    result_df['dt'] = result_df['dt'].apply(lambda x: x.strftime('%d/%m/%y'))

    return result_df


def calc_cum_mean_variance(df, mean_col, var_col, members_col):
    df_sorted = df.sort_values('dt').copy()
    df_sorted[mean_col] = df_sorted[mean_col].replace('NaN', float('nan'))
    df_sorted[var_col] = df_sorted[var_col].replace('NaN', float('nan'))
    df_sorted[members_col] = df_sorted[members_col].replace('NaN', float('nan'))
    # if  float(df_sorted[var_col]) == float('nan') or float(df_sorted[members_col]) == float('nan'):
    #     return pd.DataFrame([])
    df_sorted['mean_2'] = df_sorted[var_col] * (df_sorted[members_col] - 1)
    # if df_sorted[members_col] == 0:
    #     df_sorted['mean_2'] = 0

    records = []
    df_sorted.to_csv(f'df_sorted.csv')
    for variation, group in df_sorted.groupby('variation'):
        n_total = 0
        mean_total = 0.0
        M2_total = 0.0

        # print(group)
        for _, row in group.iterrows():
            n2 = row[members_col]
            mean2 = float(row[mean_col])
            # print("mean_col=", mean_col)
            M2_2 = float(row['mean_2'])

            if n_total == 0:
                n_total = n2
                mean_total = mean2
                M2_total = M2_2
            elif n2 < 2:
                pass
            else:
                delta = mean2 - mean_total
                n_combined = n_total + n2
                mean_total = (n_total * mean_total + n2 * mean2) / n_combined
                # print("M2_total=", M2_total, "M2_2=", M2_2, "delta=", delta, "n_total=", n_total, "n2=", n2, "n_combined=", n_combined)
                M2_total = M2_total + M2_2 + delta**2 * n_total * n2 / n_combined
                n_total = n_combined
                # n_total = 10, n2=3, M2_2=0, delta=14.5, n_combined=13, mean_total=11,1538461538
                # M2_total = 
                

            # print("mean2=",mean2, "M2_2=", M2_2, "M2_total=", M2_total, "n_total=", n_total)
            var_sample = M2_total / (n_total - 1) if n_total > 1 else float('nan')

            records.append({
                'dt': row['dt'],
                'variation': variation,
                f'{mean_col}_cum': mean_total,
                f'{var_col}_cum': var_sample
            })

    result_df = pd.DataFrame(records)
    return result_df

def calc_long_tab_view_cumulatives(df):
    df.sort_values(by=['variation', 'dt'], inplace=True)
    tab_view_df = calc_cum_mean_variance(df, 'tab_view_avg', 'tab_view_var', 'members')
    tab_view_60_df = calc_cum_mean_variance(df, 'tab_view_60_avg', 'tab_view_60_var', 'members')
    tab_view_120_df = calc_cum_mean_variance(df, 'tab_view_120_avg', 'tab_view_120_var', 'members')
    tab_view_180_df = calc_cum_mean_variance(df, 'tab_view_180_avg', 'tab_view_180_var', 'members')
    tab_view_300_df = calc_cum_mean_variance(df, 'tab_view_300_avg', 'tab_view_300_var', 'members')
    tab_view_600_df = calc_cum_mean_variance(df, 'tab_view_600_avg', 'tab_view_600_var', 'members')

    cumsum_columns = [
        'members', 'tab_view_60_cnt', 'tab_view_120_cnt', 'tab_view_180_cnt', 'tab_view_300_cnt', 'tab_view_600_cnt'
    ]

    for col in cumsum_columns:
        df[f'{col}_cum'] = df.groupby('variation')[col].cumsum()

    df['tab view 60s, %'] = df['tab_view_60_cnt_cum'] / df['members_cum'] * 100
    df['tab view 120s, %'] = df['tab_view_120_cnt_cum'] / df['members_cum'] * 100
    df['tab view 180s, %'] = df['tab_view_180_cnt_cum'] / df['members_cum'] * 100
    df['tab view 300s, %'] = df['tab_view_300_cnt_cum'] / df['members_cum'] * 100
    df['tab view 600s, %'] = df['tab_view_600_cnt_cum'] / df['members_cum'] * 100

    # join all dfd to df by dt and variation
    df = df.merge(tab_view_df, on=['dt', 'variation'], how='left')
    df = df.merge(tab_view_60_df, on=['dt', 'variation'], how='left')
    df = df.merge(tab_view_120_df, on=['dt', 'variation'], how='left')
    df = df.merge(tab_view_180_df, on=['dt', 'variation'], how='left')
    df = df.merge(tab_view_300_df, on=['dt', 'variation'], how='left')
    df = df.merge(tab_view_600_df, on=['dt', 'variation'], how='left')
    

    final_columns = [
        'dt', 'variation', 'members_cum', 
        'tab_view_60_cnt_cum', 'tab view 60s, %',
        'tab_view_120_cnt_cum', 'tab view 120s, %',
        'tab_view_180_cnt_cum', 'tab view 180s, %',
        'tab_view_300_cnt_cum', 'tab view 300s, %',
        'tab_view_600_cnt_cum', 'tab view 600s, %',
        'tab_view_avg_cum', 'tab_view_var_cum',
        'tab_view_60_avg_cum', 'tab_view_60_var_cum',
        'tab_view_120_avg_cum', 'tab_view_120_var_cum',
        'tab_view_180_avg_cum', 'tab_view_180_var_cum',
        'tab_view_300_avg_cum', 'tab_view_300_var_cum',
        'tab_view_600_avg_cum', 'tab_view_600_var_cum'
    ]

    result_df = df[final_columns]
    result_df.columns = [col.replace('_cum', '') for col in result_df.columns]
    result_df['dt'] = pd.to_datetime(result_df['dt']).dt.date
    result_df['dt'] = result_df['dt'].apply(lambda x: x.strftime('%d/%m/%y'))

    return result_df


def calculate_custom_funnels(funnels_dict: dict) -> dict:
    results = {}
    for funnel_name, funnel_data in funnels_dict.items():
        # save column order in funnel_data
        original_columns = funnel_data.columns.tolist()
        # for every column that has not % in the end calcluate cumulative sum by date with group by variation
        funnel_data.sort_values(by=['variation', 'dt'], inplace=True)
        cumsum_columns = [col for col in funnel_data.columns if not col.endswith('%') and col not in ['dt', 'variation']]
        for col in cumsum_columns:
            funnel_data[f'{col}_cum'] = funnel_data.groupby('variation')[col].cumsum()
        # calculate percentage columns
        percentage_columns = [col for col in funnel_data.columns if col.endswith('%')]
        for col in percentage_columns:
            # trim the % sign from the column name, split by -> and trim spaces. find left and right parts in _cum and calculate percentage
            left_part, right_part = col.replace(', %', '').split(' -> ')
            left_part = left_part.strip()
            right_part = right_part.strip()
            if f'{left_part}_cum' in funnel_data.columns and f'{right_part}_cum' in funnel_data.columns:
                funnel_data[col] = (funnel_data[f'{right_part}_cum'] / funnel_data[f'{left_part}_cum']) * 100
            else:
                funnel_data[col] = np.nan
        funnel_data = funnel_data.drop(columns=cumsum_columns)
        results[funnel_name] = funnel_data
        results[funnel_name].columns = [col.replace('_cum', '') for col in results[funnel_name].columns]
        # return to initial order of columns based on original_columns
        results[funnel_name] = results[funnel_name][original_columns]
        results[funnel_name]['dt'] = pd.to_datetime(results[funnel_name]['dt']).dt.date
    return results