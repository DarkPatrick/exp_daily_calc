select
    *,
    revenue_gross * case
        when lower(platform) like '%ios%' then 0.7
        when lower(platform) like '%and%' then 0.85
        else 1
    end as revenue,
    refund_revenue_gross * case
        when lower(platform) like '%ios%' then 0.7
        when lower(platform) like '%and%' then 0.85
        else 1
    end as refund_revenue
from (
    select
        s.subscription_id as subscription_id,
        s.product_id as product_id,
        argMinIf(s.unified_id, s.datetime, s.event = 'Subscribed') as unified_id,
        -- argMinIf(
        --     s.unified_id, s.datetime,
        --         s.event = 'Subscribed'
        --         or s.event = 'Charged' and d.notification_type in ('INITIAL_BUY', 'INTERACTIVE_RENEWAL', 'SUBSCRIBED:INITIAL_BUY', 'SUBSCRIBED:RESUBSCRIBE', 'SUBSCRIBED')
        -- ) as unified_id,
        argMinIf(s.payment_account_id, s.datetime, s.event = 'Subscribed') as payment_account_id,
        argMinIf(s.user_id, s.datetime, s.event = 'Subscribed') as user_id,
        argMinIf(s.trial, s.datetime, s.event = 'Subscribed') as trial,
        -- argMinIf(
        --     s.trial, s.datetime, 
        --         s.event = 'Subscribed'
        --         or s.event = 'Charged' and d.notification_type in ('INITIAL_BUY', 'INTERACTIVE_RENEWAL', 'SUBSCRIBED:INITIAL_BUY', 'SUBSCRIBED:RESUBSCRIBE', 'SUBSCRIBED')
        -- ) as trial,
        argMinIf(s.service_name, s.datetime, s.event = 'Subscribed') as service_name,
        argMinIf(s.platform, s.datetime, s.event = 'Subscribed') as platform,
        -- argMinIf(
        --     s.platform, s.datetime, 
        --         s.event = 'Subscribed'
        --         or s.event = 'Charged' and d.notification_type in ('INITIAL_BUY', 'INTERACTIVE_RENEWAL', 'SUBSCRIBED:INITIAL_BUY', 'SUBSCRIBED:RESUBSCRIBE', 'SUBSCRIBED')
        -- ) as platform,
        argMinIf(toUnixTimestamp(s.datetime_next_billing), s.datetime, s.event = 'Subscribed') as first_charge_expected_dt,
        -- argMinIf(
        --     if(s.event = 'Subscribed', toUnixTimestamp(s.datetime_next_billing), toUnixTimestamp(s.datetime)), s.datetime, 
        --         s.event = 'Subscribed'
        --         or s.event = 'Charged' and d.notification_type in ('INITIAL_BUY', 'INTERACTIVE_RENEWAL', 'SUBSCRIBED:INITIAL_BUY', 'SUBSCRIBED:RESUBSCRIBE', 'SUBSCRIBED')
        -- ) as first_charge_expected_dt,
        argMinIf(s.funnel_source, s.datetime, s.event = 'Subscribed') as funnel_source,
        -- argMinIf(
        --     s.funnel_source, s.datetime, 
        --         s.event = 'Subscribed'
        --         or s.event = 'Charged' and d.notification_type in ('INITIAL_BUY', 'INTERACTIVE_RENEWAL', 'SUBSCRIBED:INITIAL_BUY', 'SUBSCRIBED:RESUBSCRIBE', 'SUBSCRIBED')
        -- ) as funnel_source,
        argMinIf(s.funnel_start_action, s.datetime, s.event = 'Subscribed') as funnel_start_action,
        argMinIf(s.duration_count, s.datetime, s.event = 'Subscribed') as duration_count,
        argMinIf(s.base_price, s.datetime, s.event = 'Subscribed') as base_price,
        minIf(toUnixTimestamp(s.datetime), s.event = 'Subscribed') as subscribed_dt,
        -- minIf(
        --     toUnixTimestamp(s.datetime), 
        --         s.event = 'Subscribed'
        --         or s.event = 'Charged' and d.notification_type in ('INITIAL_BUY', 'INTERACTIVE_RENEWAL', 'SUBSCRIBED:INITIAL_BUY', 'SUBSCRIBED:RESUBSCRIBE', 'SUBSCRIBED')
        -- ) as subscribed_dt,
        minIf(toUnixTimestamp(s.datetime), s.event = 'Charged') as charge_dt,
        minIf(toUnixTimestamp(s.datetime), s.event = 'Canceled') as cancel_dt,
        minIf(toUnixTimestamp(s.datetime), s.event = 'Refunded') as refund_dt,
        minIf(toUnixTimestamp(s.datetime), s.event in ('Upgrade', 'Crossgrade')) as upgrade_dt,
        argMinIf(s.usd_price, s.datetime, s.event = 'Charged') as revenue_gross,
        argMinIf(s.usd_price, s.datetime, s.event = 'Refunded') as refund_revenue_gross,
        argMinIf(-toFloat32OrZero(s.`params.str_value`[indexOf(s.`params.key`, 'usd_refund')]), s.datetime, s.event in ('Upgrade', 'Crossgrade')) as upgrade_revenue
    from
        default.ug_subscriptions_events as s
    -- left join
    --     mysql_mob_api.subscription_ios_notification as n
    -- on
    --     s.subscription_id = toString(n.original_transaction_id)
    -- and
    --     s.product_code = n.product_code
    -- and
    --     toDate(s.datetime) = toDate(n.date_received)
    -- left join
    --     mysql_mob_api.subscription_ios_notification_type_dictionary as d
    -- on
    --     n.notification_type_id = d.id
    where
    --     s.date >= '{date}'
    -- and
        s.subscription_id != ''
    and
        s.product_id != ''
    and
        ({custom_sub_where})
    group by
        subscription_id,
        product_id
    having
        (lower(funnel_source) not like 'email%' or funnel_source in ('email_reg_offer', 'email_auth_offer'))
    and
        {funnel_source_include}
    and
        {funnel_source_exclude}
    and
        ({custom_sub_having})
    and
        unified_id > 0
    -- and
    --     funnel_start_action = 'WebView'
)
