select
    toDate(exp_start_dt) as dt,
    variation,
    uniq(unified_id) as members,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end}) as subscriber_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end}) as access_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial = 0) as access_instant_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDate(charge_dt) = toDate(subscribed_dt)) as access_ex_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt))) as access_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(first_charge_expected_dt) + interval 1 day > exp_start_dt) as active_trial_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt))) as trial_subscriber_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as charged_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as active_charged_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDate(charge_dt) = toDateTime(0) and toDateTime(cancel_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as cancel_trial_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as trial_buyer_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) > toDateTime(first_charge_expected_dt) + interval 1 day) as late_charged_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as buyer_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as charge_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as refund_14d_cnt,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
        - sumIf(s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDate(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as revenue,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_charge_cnt,
    sumIf(s.revenue, subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_revenue,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as trial_revenue,
    sumIf(s.revenue, subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as active_trial_revenue,
    sumIf(s.lifetime_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
        - sumIf(s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDate(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as lifetime_revenue,
    uniqIf((subscription_id, product_id), upgrade_dt between exp_start_dt and {datetime_end}) as upgrade_cnt,
    sumIf(s.upgrade_revenue, upgrade_dt between exp_start_dt and {datetime_end}) as upgrade_revenue,
    -- groupArrayIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as prices,

    -- arrayConcat(
    --     groupArrayIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day),
    --     groupArrayIf(-s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day)
    -- ) as prices,
    varSamp(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0)) as arpu_var,
    varSamp(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.lifetime_revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0)) as lifetime_arpu_var,
    varSampIf(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as arppu_var,

    -- arrayMap(x -> arrayFilter(y -> y is not null, x),
    --     groupArray(
    --         arrayConcat(
    --             if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.revenue, null),
    --             if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, null)
    --         )
    --     )
    -- ) as prices,
    -- groupArrayIf((unified_id, s.revenue), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as prices_per_buyer,

    -- arrayConcat(
    --     groupArrayIf((unified_id, s.revenue), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day),
    --     groupArrayIf((unified_id, -s.refund_revenue), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day)
    -- ) as prices_per_buyer,

    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as cancel_14d_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 1 month) as cancel_1m_cnt
from
    members
left join
-- inner join
    subscriptions as s
    -- sandbox.ug_monetization_exp_calc_subscriptions_3 as s
using(unified_id)
-- where
--     subscribed_dt between exp_start_dt and {datetime_end}
group by
    dt,
    variation
