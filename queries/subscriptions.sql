select
    *,
    revenue_gross * case
        when lower(platform) like '%ios%' then 0.7
        when lower(platform) like '%and%' then 0.85
        else 1
    end as revenue
from (
    select
        subscription_id,
        product_id,
        argMinIf(unified_id, datetime, event = 'Subscribed') as unified_id,
        argMinIf(payment_account_id, datetime, event = 'Subscribed') as payment_account_id,
        argMinIf(user_id, datetime, event = 'Subscribed') as user_id,
        argMinIf(trial, datetime, event = 'Subscribed') as trial,
        argMinIf(service_name, datetime, event = 'Subscribed') as service_name,
        argMinIf(platform, datetime, event = 'Subscribed') as platform,
        argMinIf(funnel_source, datetime, event = 'Subscribed') as funnel_source,
        argMinIf(duration_count, datetime, event = 'Subscribed') as duration_count,
        argMinIf(base_price, datetime, event = 'Subscribed') as base_price,
        minIf(toUnixTimestamp(datetime), event = 'Subscribed') as subscribed_dt,
        minIf(toUnixTimestamp(datetime), event = 'Charged') as charge_dt,
        minIf(toUnixTimestamp(datetime), event = 'Canceled') as cancel_dt,
        minIf(toUnixTimestamp(datetime), event = 'Refunded') as refund_dt,
        minIf(toUnixTimestamp(datetime), event in ('Upgrade', 'Crossgrade')) as upgrade_dt,
        argMinIf(usd_price, datetime, event = 'Charged') as revenue_gross,
        argMinIf(-toFloat32OrZero(`params.str_value`[indexOf(`params.key`, 'usd_refund')]), datetime, event in ('Upgrade', 'Crossgrade')) as upgrade_revenue
    from
        default.ug_subscriptions_events
    where
        date >= '{date}'
    and
        subscription_id != ''
    and
        product_id != ''
    and
        ({custom_where})
    group by
        subscription_id,
        product_id
    having
        (lower(funnel_source) not like 'email%' or funnel_source in ('email_reg_offer', 'email_auth_offer'))
    and ('{funnel_source_include}' = '' or funnel_source in ('{funnel_source_include}'))
    and ('{funnel_source_exclude}' = '' or funnel_source not in ('{funnel_source_exclude}'))
    and
        ({custom_having})
)