select
    subscription_id,
    product_code,
    minIf(datetime, event = 'Subscribed') as sub_dt,
    minIf(datetime, event = 'Charged') as ch_dt,
    minIf(datetime, event = 'Canceled') as can_dt,
    argMinIf(platform, datetime, event = 'Subscribed') as platform,
    argMinIf(trial, datetime, event = 'Subscribed') as trial,
    argMinIf(funnel_source, datetime, event = 'Subscribed') as funnel_source,
    argMinIf(service_name, datetime, event = 'Subscribed') as service_name,
    argMinIf(product_id, datetime, event = 'Subscribed') as product_id,
    argMinIf(user_id, datetime, event = 'Subscribed') as user_id,
    argMinIf(unified_id, datetime, event = 'Subscribed') as unified_id,
    argMinIf(payment_account_id, datetime, event = 'Subscribed') as payment_account_id,
    argMinIf(usd_price, datetime, event = 'Charged') as revenue_gross,
    argMaxIf(datetime_next_billing, datetime, event = 'Charged') as datetime_next_billing
from
    default.ug_subscriptions_events
where
    event in ('Subscribed', 'Charged', 'Canceled')
group by
    subscription_id,
    product_code
