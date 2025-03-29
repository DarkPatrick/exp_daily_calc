select
    toDate(exp_start_dt) as dt,
    variation,
    uniqExact(unified_id) as members,
    uniqExactIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end}) as subscriber_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end}) as access_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial = 0) as access_instant_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(subscribed_dt) + interval 1 day) as access_ex_trial_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(subscribed_dt) + interval 1 day)) as access_trial_cnt,
    uniqExactIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0) as trial_subscriber_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDateTime(charge_dt) between toDateTime(subscribed_dt) + interval 1 day and toDateTime(subscribed_dt) + interval 9 day) as charged_trial_cnt,
    uniqExactIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDateTime(charge_dt) between toDateTime(subscribed_dt) + interval 1 day and toDateTime(subscribed_dt) + interval 9 day) as trial_buyer_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) > toDateTime(subscribed_dt) + interval 9 day) as late_charged_cnt,
    uniqExactIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt) as buyer_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt) as charge_cnt,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt) as revenue,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDateTime(charge_dt) between toDateTime(subscribed_dt) + interval 1 day and toDateTime(subscribed_dt) + interval 9 day) as trial_revenue,
    groupArrayIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt) as prices,
    groupArrayIf((unified_id, s.revenue), subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt) as prices_per_buyer,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as cancel_14d_cnt,
    uniqExactIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and charge_dt >= subscribed_dt and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 1 month) as cancel_1m_cnt
from
    members
left join
    subscriptions as s
using(unified_id)
group by
    dt,
    variation
