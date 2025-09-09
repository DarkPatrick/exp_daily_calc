select
    dt,
    variation,
    s1.members as members,
    s1.install_cnt as install_cnt,
    s1.subscriber_cnt + s2.subscriber_cnt as subscriber_cnt,
    s1.access_cnt + s2.access_cnt as access_cnt,
    s1.access_instant_cnt + s2.access_instant_cnt as access_instant_cnt,
    s1.access_ex_trial_cnt + s2.access_ex_trial_cnt as access_ex_trial_cnt,
    s1.access_trial_cnt + s2.access_trial_cnt as access_trial_cnt,
    s1.active_trial_cnt + s2.active_trial_cnt as active_trial_cnt,
    s1.trial_subscriber_cnt + s2.trial_subscriber_cnt as trial_subscriber_cnt,
    s1.charged_trial_cnt + s2.charged_trial_cnt as charged_trial_cnt,
    s1.active_charged_trial_cnt + s2.active_charged_trial_cnt as active_charged_trial_cnt,
    s1.access_otp_cnt + s2.access_otp_cnt as access_otp_cnt,
    s1.cancel_trial_cnt + s2.cancel_trial_cnt as cancel_trial_cnt,
    s1.trial_buyer_cnt + s2.trial_buyer_cnt as trial_buyer_cnt,
    s1.late_charged_cnt + s2.late_charged_cnt as late_charged_cnt,
    s1.buyer_cnt + s2.buyer_cnt as buyer_cnt,
    s1.charge_cnt + s2.charge_cnt as charge_cnt,
    s1.refund_14d_cnt + s2.refund_14d_cnt as refund_14d_cnt,
    s1.revenue + s2.revenue as revenue,
    s1.recurrent_charge_cnt + s2.recurrent_charge_cnt as recurrent_charge_cnt,
    s1.recurrent_revenue + s2.recurrent_revenue as recurrent_revenue,
    s1.trial_revenue + s2.trial_revenue as trial_revenue,
    s1.active_trial_revenue + s2.active_trial_revenue as active_trial_revenue,
    s1.lifetime_revenue + s2.lifetime_revenue as lifetime_revenue,
    s1.upgrade_cnt + s2.upgrade_cnt as upgrade_cnt,
    s1.upgrade_revenue + s2.upgrade_revenue as upgrade_revenue,
    case
        when members < 2 then 0
        when s2.members < 2 then s1.arpu_var
        when s1.members < 2 then s2.arpu_var
        else ((s1.members - 1) * s1.arpu_var + (s2.members - 1) * s2.arpu_var + s1.members * power((s1.revenue / s1.members - revenue / members), 2) + s2.members * power((s2.revenue / s2.members - revenue / members), 2)) / (members - 1)
    end as arpu_var,
    case
        when members < 2 then 0
        when s2.members < 2 then s1.lifetime_arpu_var
        when s1.members < 2 then s2.lifetime_arpu_var
        else ((s1.members - 1) * s1.lifetime_arpu_var + (s2.members - 1) * s2.lifetime_arpu_var + s1.members * power((s1.lifetime_revenue / s1.members - lifetime_revenue / members), 2) + s2.members * power((s2.lifetime_revenue / s2.members - lifetime_revenue / members), 2)) / (members - 1)
    end as lifetime_arpu_var,
    case
        when charge_cnt < 2 then 0
        when s2.charge_cnt < 2 then s1.arppu_var
        when s1.charge_cnt < 2 then s2.arppu_var
        when s2.charge_cnt > 1 then ((s1.charge_cnt - 1) * s1.arppu_var + (s2.charge_cnt - 1) * s2.arppu_var + s1.charge_cnt * power((s1.revenue / s1.charge_cnt - revenue / charge_cnt), 2) + s2.charge_cnt * power((s2.revenue / s2.charge_cnt - revenue / charge_cnt), 2)) / (charge_cnt - 1)
        else s1.arppu_var
    end as arppu_var,
    s1.cancel_14d_cnt + s2.cancel_14d_cnt as cancel_14d_cnt,
    s1.cancel_1m_cnt + s2.cancel_1m_cnt as cancel_1m_cnt
from (
select
    toDate(exp_start_dt) as dt,
    variation,
    uniq(unified_id) as members,
    uniq(payment_account_id) as install_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end}) as subscriber_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end}) as access_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial = 0 and duration_count > 0) as access_instant_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDate(charge_dt) = toDate(subscribed_dt) and duration_count > 0) as access_ex_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and duration_count > 0) as access_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(first_charge_expected_dt) + interval 1 day > exp_start_dt and duration_count > 0) as active_trial_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and duration_count > 0) as trial_subscriber_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as charged_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as active_charged_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial = 0 and service_name = '' and duration_count = 0) as access_otp_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDate(charge_dt) = toDateTime(0) and toDateTime(cancel_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as cancel_trial_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as trial_buyer_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) > toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as late_charged_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as buyer_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as charge_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as refund_14d_cnt,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
        - sumIf(s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDate(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as revenue,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_charge_cnt,
    sumIf(s.revenue, subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_revenue,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as trial_revenue,
    sumIf(s.revenue, subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as active_trial_revenue,
    sumIf(s.lifetime_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
        - sumIf(s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDate(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as lifetime_revenue,
    uniqIf((subscription_id, product_id), upgrade_dt between exp_start_dt and {datetime_end}) as upgrade_cnt,
    sumIf(s.upgrade_revenue, upgrade_dt between exp_start_dt and {datetime_end}) as upgrade_revenue,
    varSamp(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0, s.revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0)) as arpu_var,
    varSamp(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0, s.lifetime_revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0)) as lifetime_arpu_var,
    varSampIf(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0, s.revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as arppu_var,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as cancel_14d_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 1 month) as cancel_1m_cnt
from
    members
left join
    subscriptions as s
    -- sandbox.ug_monetization_exp_calc_subscriptions_3 as s
using(unified_id)
group by
    dt,
    variation
) as s1
left join (
    select
    toDate(exp_start_dt) as dt,
    variation,
    uniq(unified_id) as members,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end}) as subscriber_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end}) as access_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial = 0 and duration_count > 0) as access_instant_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDate(charge_dt) = toDate(subscribed_dt) and duration_count > 0) as access_ex_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and duration_count > 0) as access_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(first_charge_expected_dt) + interval 1 day > exp_start_dt and duration_count > 0) as active_trial_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and duration_count > 0) as trial_subscriber_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as charged_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as active_charged_trial_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial = 0 and service_name = '' and duration_count = 0) as access_otp_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and toDate(charge_dt) = toDateTime(0) and toDateTime(cancel_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as cancel_trial_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as trial_buyer_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) > toDateTime(first_charge_expected_dt) + interval 1 day) as late_charged_cnt,
    uniqIf(unified_id, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as buyer_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as charge_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as refund_14d_cnt,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
        - sumIf(s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDate(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as revenue,
    uniqIf((subscription_id, product_id), subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_charge_cnt,
    sumIf(s.revenue, subscribed_dt < exp_start_dt and charge_dt >= subscribed_dt) as recurrent_revenue,
    sumIf(s.revenue, subscribed_dt between exp_start_dt and {datetime_end} and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as trial_revenue,
    sumIf(s.revenue, subscribed_dt < exp_start_dt and trial > 0 and not (toDate(charge_dt) = toDate(subscribed_dt)) and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and duration_count > 0) as active_trial_revenue,
    sumIf(s.lifetime_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) 
        - sumIf(s.refund_revenue, subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDate(refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as lifetime_revenue,
    uniqIf((subscription_id, product_id), upgrade_dt between exp_start_dt and {datetime_end}) as upgrade_cnt,
    sumIf(s.upgrade_revenue, upgrade_dt between exp_start_dt and {datetime_end}) as upgrade_revenue,
    varSamp(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0)) as arpu_var,
    varSamp(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.lifetime_revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0)) as lifetime_arpu_var,
    varSampIf(if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day, s.revenue, 0) - if(subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(s.refund_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day, s.refund_revenue, 0), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day) as arppu_var,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 14 day) as cancel_14d_cnt,
    uniqIf((subscription_id, product_id), subscribed_dt between exp_start_dt and {datetime_end} and toDateTime(charge_dt) between toDateTime(subscribed_dt) and toDateTime(first_charge_expected_dt) + interval 1 day and toDateTime(cancel_dt) between toDateTime(charge_dt) and toDateTime(charge_dt) + interval 1 month) as cancel_1m_cnt
from
    members
inner join
    subscriptions as s
    -- sandbox.ug_monetization_exp_calc_subscriptions_3 as s
using(payment_account_id)
where
    members.payment_account_id > 0
and
    members.unified_id != s.unified_id
group by
    dt,
    variation
) as s2
using(dt, variation)
