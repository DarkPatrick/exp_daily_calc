select
    toDate(m.exp_start_dt, 'UTC') as dt,
    m.variation as variation,
    uniqExact(m.unified_id) as members,
    uniqExactIf(m.unified_id, e.datetime between toDateTime(m.exp_start_dt, 'UTC') + interval 24 hour and toDateTime(m.exp_start_dt, 'UTC') + interval 48 hour) as retention_1d_cnt,
    uniqExactIf(m.unified_id, e.datetime between toDateTime(m.exp_start_dt, 'UTC') + interval 24 hour and toDateTime(m.exp_start_dt, 'UTC') + interval 192 hour) as retention_7d_cnt,
    uniqExactIf(m.unified_id, e.datetime between toDateTime(m.exp_start_dt, 'UTC') + interval 24 hour and toDateTime(m.exp_start_dt, 'UTC') + interval 360 hour) as retention_14d_cnt
from
    default.ug_rt_events_app as e
inner join (
    select
        e.unified_id as unified_id,
        m.exp_start_dt as exp_start_dt,
        m.variation as variation
    from
        default.ug_rt_events_app as e
    inner join
        {members} as m
    on
        e.payment_account_id = m.payment_account_id
    and
        e.date = toDate(m.exp_start_dt)
    where
        e.date between toDate({datetime_start}, 'UTC') and toDate({datetime_end}, 'UTC') + interval 1 day
    and
        e.event = 'Tour Referral Start'
    and
        e.payment_account_id > 0
) as m
on
    e.unified_id = m.unified_id
where
    e.date between toDate({datetime_start}, 'UTC') and toDate({datetime_end}, 'UTC') + interval 15 day
and
    e.event in ('Tab Open', 'App Start', 'Courses Open', 'Shots Open', 'Tabs Open')
and
    e.unified_id > 0
group by
    dt,
    variation
