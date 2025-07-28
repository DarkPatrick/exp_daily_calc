tab_view as (
    select
        toDate(m.exp_start_dt) as dt,
        m.variation as variation, 
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt) and e.event = 'Tab View 60s') as tab_view_60_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt) and e.event = 'Tab View 120s') as tab_view_120_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt) and e.event = 'Tab View 180s') as tab_view_180_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt) and e.event = 'Tab View 300s') as tab_view_300_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt) and e.event = 'Tab View 600s') as tab_view_600_cnt
    from
        default.ug_rt_events_app as e
    inner join
        {members} as m
    on
        e.unified_id = m.unified_id
    where
        e.date between toDate({datetime_start}) and toDate({datetime_end})
    and
        e.event in ('Tab View 60s', 'Tab View 120s', 'Tab View 180s', 'Tab View 300s', 'Tab View 600s')
    group by
        dt,
        variation
),

members_agg as (
    select
        toDate(exp_start_dt) as dt,
        variation,
        uniqExact(m.unified_id) as members
    from
        {members} as m
    group by
        dt,
        variation
)


select
    m.dt as dt,
    m.variation as variation,
    m.members as members,
    t.tab_view_60_cnt as tab_view_60_cnt,
    t.tab_view_120_cnt as tab_view_120_cnt,
    t.tab_view_180_cnt as tab_view_180_cnt,
    t.tab_view_300_cnt as tab_view_300_cnt,
    t.tab_view_600_cnt as tab_view_600_cnt
from
    members_agg as m
left join
    tab_view as t
on
    m.dt = t.dt
and
    m.variation = t.variation
