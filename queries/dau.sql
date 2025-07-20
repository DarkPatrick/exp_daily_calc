with dau as (
    select
        unified_id,
        min(toUnixTimestamp(datetime)) as dau_dt
    from
        default.ug_rt_events_web
    where
        date between toDate({datetime_start}) and '{date}'
    and
        unified_id > 0
    and (
        '{exposure_event}' = 'App Experiment Start' and event = 'App Experiment Start' and item_id = {exp_id}
        or '{exposure_event}' != 'App Experiment Start' and event = '{exposure_event}'
    )
    and
        multiIf(
            '{platform}' = 'Desktop',  platform = 1,
            '{platform}' = 'Phone', platform = 2, 
            '{platform}' = 'Tablet', platform = 3, 
            '{platform}' = 'Mobile', platform > 1, 
            1
        )
    and
        if('{exposure_event}' like 'Landing%', value not like 'email_%' or value in ('email_reg_offer', 'email_auth_offer'), 1)
    and
        ('{include_values}' = '' or value in ('{include_values}'))
    and
        ('{exclude_values}' = '' or value not in ('{exclude_values}'))
    and
        ({custom_where})
    group by
        unified_id
    having
        session_id > 0
    and
        {pro_rights} and {edu_rights} and {sing_rights} and {practice_rights} and {book_rights}
    and
        toDate(dau_dt) = '{date}'
    and
        ({custom_having})
    union all 
    select
        case
            when w.unified_id > 0 and w.unified_id is not null then w.unified_id
            else a.unified_id
        end as unified_id,
        min(toUnixTimestamp(datetime)) AS dau_dt
    from
        default.ug_rt_events_app as a
    left join (
        select
            unified_id,
            splitByChar('.', params.str_value[indexOf(params.key, 'app_unified_id')]) as uid,
            toInt64OrNull(uid[2] || uid[3]) as app_unified_id
        from
            default.ug_rt_events_web as w
        where
            date between toDate({datetime_start}) and toDate({datetime_end})
        and
            app_unified_id is not null
        and
            app_unified_id > 0
        and
            event = 'PURCHASE_SUCCESS'
        and
            ui = 'WebView'
    ) as w
    on
        a.unified_id = w.app_unified_id
    where
        date between toDate({datetime_start}) and '{date}'
    and
        unified_id > 0
    and (
        '{exposure_event}' = 'App Experiment Start' and event = 'App Experiment Start' and item_id = {exp_id}
        or '{exposure_event}' != 'App Experiment Start' and event = '{exposure_event}'
    )
    and
        ('{include_values}' = '' or value in ('{include_values}'))
    and
        ('{exclude_values}' = '' or value not in ('{exclude_values}'))
    and
        multiIf(
            '{platform}' = 'Desktop',  platform = 1,
            '{platform}' = 'Phone', platform = 2, 
            '{platform}' = 'Tablet', platform = 3, 
            '{platform}' = 'Mobile', platform > 1, 
            1
        )
    and
        ({custom_where})
    group by
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
        toDate(dau_dt) = '{date}'
    and
        ({custom_having})
)


select
    toDate(dau_dt) as dt,
    uniqExact(unified_id) as dau
from
    dau
group by
    dt
order by
    dt
