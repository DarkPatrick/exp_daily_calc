select
    toDate(m.exp_start_dt, 'UTC') as dt,
    m.variation as variation,
    uniqExact(m.unified_id) as members,
    uniqExactIf(m.unified_id, e.datetime between toDateTime(m.exp_start_dt, 'UTC') + interval 24 hour and toDateTime(m.exp_start_dt, 'UTC') + interval 48 hour) as retention_1d_cnt,
    uniqExactIf(m.unified_id, e.datetime between toDateTime(m.exp_start_dt, 'UTC') + interval 24 hour and toDateTime(m.exp_start_dt, 'UTC') + interval 192 hour) as retention_7d_cnt,
    uniqExactIf(m.unified_id, e.datetime between toDateTime(m.exp_start_dt, 'UTC') + interval 24 hour and toDateTime(m.exp_start_dt, 'UTC') + interval 360 hour) as retention_14d_cnt
from
    default.ug_rt_events_app as e
inner join
    {members} as m
on
    e.unified_id = m.unified_id
where
    e.date between toDate({datetime_start}, 'UTC') and toDate({datetime_end}, 'UTC') + interval 15 day
and
    e.event in ('Tab Open', 'App Start', 'Courses Open', 'Shots Open', 'Tabs Open')
group by
    dt,
    variation
