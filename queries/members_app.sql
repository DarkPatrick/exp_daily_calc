select
    experiments.variation[indexOf(experiments.id, {exp_id})] as variation,
    unified_id,
    -- case
    --     when w.unified_id > 0 and w.unified_id is not null then w.unified_id
    --     else a.unified_id
    -- end as unified_id,
    argMinIf(user_id, datetime, user_id > 0) as user_id,
    argMinIf(payment_account_id, datetime, payment_account_id > 0) as payment_account_id,
    argMinIf(session_id, datetime, session_id > 0) as session_id,
    min(toUnixTimestamp(datetime)) AS exp_start_dt,
    argMin(rights,datetime) AS rights,
    argMin(country,datetime) AS country,
    argMin(source, datetime) as source,
    argMin(os, datetime) as os
from
    default.ug_rt_events_app as a
-- left join (
--     select
--         unified_id,
--         splitByChar('.', params.str_value[indexOf(params.key, 'app_unified_id')]) as uid,
--         toInt64OrNull(uid[2] || uid[3]) as app_unified_id
--     from
--         default.ug_rt_events_web as w
--     where
--         date between toDate({datetime_start}, 'UTC') and toDate({datetime_end}, 'UTC')
--     and
--         app_unified_id is not null
--     and
--         app_unified_id > 0
--     and
--         event = 'PURCHASE_SUCCESS'
--     and
--         ui = 'WebView'
-- ) as w
-- on
--     a.unified_id = w.app_unified_id
where
    date between toDate({datetime_start}, 'UTC') and '{date}'
and
    datetime between toDateTime({datetime_start}, 'UTC') and toDateTime({datetime_end}, 'UTC')
and
    has(experiments.id, {exp_id})
and
    unified_id > 0
and (
    '{exposure_event}' = 'App Experiment Start' and event = 'App Experiment Start' and item_id = {exp_id}
    or '{exposure_event}' != 'App Experiment Start' and event = '{exposure_event}'
)
and
    {include_values}
and
    {exclude_values}
and
    multiIf(
        '{platform}' = 'desktop',  platform = 1,
        '{platform}' = 'phone', platform = 2, 
        '{platform}' = 'tablet', platform = 3, 
        '{platform}' = 'mobile', platform > 1, 
        1
    )
and
    ({custom_where})
group by
    variation,
    unified_id
having
    ('{source}' = 'All' or '{source}' = source)
and
    {pro_rights} and {edu_rights} and {sing_rights} and {practice_rights} and {book_rights}
and
    multiIf(
        '{country}' in ('US', 'CA', 'GB', 'AU'), country = '{country}', 
        '{country}' = 'Europe', country in ('BY', 'BG', 'HU', 'XK', 'MD', 'PL', 'RU', 'RO', 'SK', 'UA', 'CZ', 'AT', 'BE', 'DE', 'LI', 'LU', 'MC', 'NL', 'FR', 'CH', 'AX', 'DK', 'IE', 'IS', 'LV', 'LT', 'NO', 'FI', 'SE', 'AL', 'AD', 'BA', 'VA', 'GR', 'ES', 'IT', 'MK', 'MT', 'PT', 'SM', 'RS', 'SI', 'HR', 'ME'), 
        '{country}' = 'Asia', country in ('JP', 'KR', 'PH', 'TR', 'TH', 'SG', 'MY', 'KZ', 'ID', 'VN', 'IN'), 
        '{country}' = 'Latam', country in ('AR', 'BO', 'BR', 'VE', 'HT', 'GP', 'GT', 'HN', 'DO', 'CO', 'CR', 'CU', 'MQ', 'MX', 'NI', 'PA', 'PY', 'PE', 'PR', 'SV', 'BL', 'MF', 'UY', 'GF', 'CL', 'EC'), 
        1
    )
and
    toDate(exp_start_dt, 'UTC') = '{date}'
and
    ({custom_having})
