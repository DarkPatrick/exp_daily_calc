select
    experiments.variation[indexOf(experiments.id, {exp_id})] as variation,
    unified_id,
    argMinIf(user_id, datetime, user_id > 0 and event != 'App Install') as user_id,
    argMinIf(session_id, datetime, session_id > 0 and event != 'App Install') as session_id,
    minIf(toUnixTimestamp(datetime), event != 'App Install') as exp_start_dt,
    argMinIf(rights, datetime, event != 'App Install') as rights,
    argMinIf(country, datetime, event != 'App Install') as country,
    argMinIf(source, datetime, event != 'App Install') as source,
    argMinIf(multiIf(os in ('ios', 'os x'), 'ios', os), datetime, event != 'App Install') as os,
    argMinIf(item_id, datetime, event = 'App Install') as payment_account_id
from
    default.ug_rt_events_web
where
    date between toDate({datetime_start}, 'UTC') and '{date}'
and
    datetime between toDateTime({datetime_start}, 'UTC') and toDateTime({datetime_end}, 'UTC')
and
    has(experiments.id, {exp_id})
and
    variation > 0
and
    unified_id > 0
and (
    (
        '{exposure_event}' = 'App Experiment Start' and event = 'App Experiment Start' and item_id = {exp_id}
        or '{exposure_event}' != 'App Experiment Start' and event = '{exposure_event}'
    )
and
    multiIf(
        '{platform}' = 'desktop',  platform = 1,
        '{platform}' = 'phone', platform = 2, 
        '{platform}' = 'tablet', platform = 3, 
        '{platform}' = 'mobile', platform > 1, 
        1
    )
and
    if('{exposure_event}' like 'Landing%', value not like 'email_%' or value in ('email_reg_offer', 'email_auth_offer'), 1)
and
    {include_values}
and
    {exclude_values}
and
    ({custom_where})

or event = 'App Install'
)
group by
    variation,
    unified_id
having
    {pro_rights} and {edu_rights} and {sing_rights} and {practice_rights} and {book_rights}
and
    toDate(exp_start_dt, 'UTC') = '{date}'
and
    multiIf(
        '{country}' in ('US', 'CA', 'GB', 'AU'), country = '{country}', 
        '{country}' = 'Europe', country in ('BY', 'BG', 'HU', 'XK', 'MD', 'PL', 'RU', 'RO', 'SK', 'UA', 'CZ', 'AT', 'BE', 'DE', 'LI', 'LU', 'MC', 'NL', 'FR', 'CH', 'AX', 'DK', 'IE', 'IS', 'LV', 'LT', 'NO', 'FI', 'SE', 'AL', 'AD', 'BA', 'VA', 'GR', 'ES', 'IT', 'MK', 'MT', 'PT', 'SM', 'RS', 'SI', 'HR', 'ME'), 
        '{country}' = 'Asia', country in ('JP', 'KR', 'PH', 'TR', 'TH', 'SG', 'MY', 'KZ', 'ID', 'VN', 'IN'), 
        '{country}' = 'Latam', country in ('AR', 'BO', 'BR', 'VE', 'HT', 'GP', 'GT', 'HN', 'DO', 'CO', 'CR', 'CU', 'MQ', 'MX', 'NI', 'PA', 'PY', 'PE', 'PR', 'SV', 'BL', 'MF', 'UY', 'GF', 'CL', 'EC'), 
        1
    )
and
    ({custom_having})
