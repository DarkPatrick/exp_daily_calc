with dau as (
    select
        unified_id,
        min(toUnixTimestamp(datetime)) as dau_dt
    from
        default.ug_rt_events_web
    where
        date between toDate({datetime_start}, 'UTC') and '{date}'
    and
        unified_id > 0
    and (
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
    group by
        unified_id
    having
        {pro_rights} and {edu_rights} and {sing_rights} and {practice_rights} and {book_rights}
    and
        toDate(dau_dt, 'UTC') = '{date}'
    and
        ({custom_having})
)


select
    toDate(dau_dt, 'UTC') as dt,
    uniqExact(unified_id) as dau
from
    dau
group by
    dt
order by
    dt
