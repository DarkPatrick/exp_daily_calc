select
    experiments.variation[indexOf(experiments.id, {exp_id})] as variation,
    unified_id,
    argMinIf(user_id, datetime, user_id > 0) as user_id,
    argMinIf(session_id, datetime, session_id > 0) as session_id,
    min(toUnixTimestamp(datetime)) as exp_start_dt,
    argMin(rights, datetime) as rights,
    argMin(country, datetime) as country,
    argMin(source, datetime) as source,
    argMin(multiIf(os in ('ios', 'os x'), 'ios', os), datetime) as os
from
    default.ug_rt_events_web
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
    {include_values}
and
    {exclude_values}
and
    ({custom_where})
group by
    variation,
    unified_id
having
    {pro_rights} and {edu_rights} and {sing_rights} and {practice_rights} and {book_rights}
and
    toDate(exp_start_dt, 'UTC') = '{date}'
and
    ({custom_having})
