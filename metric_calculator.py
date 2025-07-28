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
        'members', 'subscriber_cnt', 'access_cnt', 'access_instant_cnt',
        'access_ex_trial_cnt', 'access_trial_cnt', 'trial_subscriber_cnt',
        'charged_trial_cnt', 'cancel_trial_cnt', 'trial_buyer_cnt', 'late_charged_cnt',
        'buyer_cnt', 'charge_cnt', 'refund_14d_cnt', 'cancel_14d_cnt', 'cancel_1m_cnt', 'revenue',
        'recurrent_charge_cnt', 'recurrent_revenue', 'trial_revenue',
        'upgrade_cnt', 'upgrade_revenue'
    ]

    for col in cumsum_columns:
        df[f'{col}_cum'] = df.groupby('variation')[col].cumsum()

    df['accesses_per_subscriber'] = df['access_cnt_cum'] / df['subscriber_cnt_cum']
    df['member -> subscriber, %'] = df['subscriber_cnt_cum'] / df['members_cum'] * 100
    df['trial -> cancel, %'] = df['cancel_trial_cnt_cum'] / df['access_trial_cnt_cum'] * 100
    df['trial -> charge, %'] = df['charged_trial_cnt_cum'] / df['access_trial_cnt_cum'] * 100
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
    df['exp_trial_arpu'] = df['trial_revenue_cum'] / df['members_cum']
    df['exp_instant_arpu'] = (df['revenue_cum'] - df['trial_revenue_cum']) / df['members_cum']

    df['prices'] = df['prices'].apply(ast.literal_eval)
    df['prices_per_buyer'] = df['prices_per_buyer'].apply(ast.literal_eval)
    
    df['prices_agg'] = df.groupby('variation')['prices'].transform(cumulative_prices)
    df['prices_per_buyer_agg'] = df.groupby('variation')['prices_per_buyer'].transform(cumulative_prices)

    df['grouped_sums'] = df['prices_per_buyer_agg'].apply(calculate_grouped_sums)
    df.to_csv("tt.csv")
    df = df.assign(
        aov_var = lambda x: x.apply(lambda row: np.var(row['prices_agg']), axis=1),
        arppu_var = lambda x: x.apply(lambda row: np.var(row['grouped_sums']), axis=1),
        exp_arpu_var = lambda x: x.apply(lambda row: np.var(np.append(row['grouped_sums'], np.zeros(row['members_cum'] - row['buyer_cnt_cum']))), axis=1)
    )

    final_columns = [
        'dt', 'variation', 'members_cum', 'subscriber_cnt_cum', 'access_cnt_cum',
        'access_instant_cnt_cum', 'access_ex_trial_cnt_cum', 'access_trial_cnt_cum',
        'accesses_per_subscriber', 'member -> subscriber, %', 'trial -> cancel, %',
        'trial -> charge, %', 'trial subscriber -> buyer, %', 'subscriber -> buyer, %',
        'member -> buyer, %', 'subscription -> charge, %', 'charge -> 14d cancel, %',
        'charge -> 14d refund, %',
        'charged_trial_cnt_cum', 'trial_subscriber_cnt_cum', 'cancel_trial_cnt_cum',
        'charge_cnt_cum', 'refund_14d_cnt_cum', 'buyer_cnt_cum', 'cancel_14d_cnt_cum', 'cancel_1m_cnt_cum', 'revenue_cum',
        'charge -> 1m cancel, %', 'arppu', 'aov', 'exp_arpu', 'exp_trial_arpu', 'exp_instant_arpu',
        'aov_var', 'arppu_var', 
        'exp_arpu_var'
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


def calc_long_tab_view_cumulatives(df):
    df.sort_values(by=['variation', 'dt'], inplace=True)

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

    final_columns = [
        'dt', 'variation', 'members_cum', 
        'tab_view_60_cnt_cum', 'tab view 60s, %',
        'tab_view_120_cnt_cum', 'tab view 120s, %',
        'tab_view_180_cnt_cum', 'tab view 180s, %',
        'tab_view_300_cnt_cum', 'tab view 300s, %',
        'tab_view_600_cnt_cum', 'tab view 600s, %'
    ]

    result_df = df[final_columns]
    result_df.columns = [col.replace('_cum', '') for col in result_df.columns]
    result_df['dt'] = pd.to_datetime(result_df['dt']).dt.date
    result_df['dt'] = result_df['dt'].apply(lambda x: x.strftime('%d/%m/%y'))

    return result_df