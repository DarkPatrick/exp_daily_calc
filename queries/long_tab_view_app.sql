tab_view as (
    select
        toDate(m.exp_start_dt, 'UTC') as dt,
        m.variation as variation, 
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 60s') as tab_view_60_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 120s') as tab_view_120_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 180s') as tab_view_180_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 300s') as tab_view_300_cnt,
        uniqExactIf(m.unified_id, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 600s') as tab_view_600_cnt
    from
        default.ug_rt_events_app as e
    inner join
        {members} as m
    on
        e.unified_id = m.unified_id
    where
        e.date between toDate({datetime_start}, 'UTC') and toDate({datetime_end}, 'UTC')
    and
        e.event in ('Tab View 60s', 'Tab View 120s', 'Tab View 180s', 'Tab View 300s', 'Tab View 600s')
    group by
        dt,
        variation
),

tab_view_per_user as (
    select
        dt,
        variation,
        uniqExact(unified_id) as tab_view_user,
        avg(tab_view_cnt) as tab_view_avg,
        avg(tab_view_60_cnt) as tab_view_60_avg,
        avg(tab_view_120_cnt) as tab_view_120_avg,
        avg(tab_view_180_cnt) as tab_view_180_avg,
        avg(tab_view_300_cnt) as tab_view_300_avg,
        avg(tab_view_600_cnt) as tab_view_600_avg,
        varSamp(tab_view_cnt) as tab_view_var,
        varSamp(tab_view_60_cnt) as tab_view_60_var,
        varSamp(tab_view_120_cnt) as tab_view_120_var,
        varSamp(tab_view_180_cnt) as tab_view_180_var,
        varSamp(tab_view_300_cnt) as tab_view_300_var,
        varSamp(tab_view_600_cnt) as tab_view_600_var
    from (
        select
            toDate(m.exp_start_dt, 'UTC') as dt,
            m.variation as variation, 
            m.unified_id as unified_id,
            uniqExactIf(e.datetime, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View') as tab_view_cnt,
            uniqExactIf(e.datetime, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 60s') as tab_view_60_cnt,
            uniqExactIf(e.datetime, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 120s') as tab_view_120_cnt,
            uniqExactIf(e.datetime, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 180s') as tab_view_180_cnt,
            uniqExactIf(e.datetime, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 300s') as tab_view_300_cnt,
            uniqExactIf(e.datetime, e.datetime >= toDateTime(m.exp_start_dt, 'UTC') and e.event = 'Tab View 600s') as tab_view_600_cnt
        from
            default.ug_rt_events_app as e
        inner join
            {members} as m
        on
            e.unified_id = m.unified_id
        where
            e.date between toDate({datetime_start}, 'UTC') and toDate({datetime_end}, 'UTC')
        and
            e.event in ('Tab View', 'Tab View 60s', 'Tab View 120s', 'Tab View 180s', 'Tab View 300s', 'Tab View 600s')
        group by
            dt,
            variation,
            unified_id
    )
    group by
        dt,
        variation
),

members_agg as (
    select
        toDate(exp_start_dt, 'UTC') as dt,
        variation,
        uniqExact(m.unified_id) as members
    from
        {members} as m
    group by
        dt,
        variation
),

long_tab as (
    select
        m.dt as dt,
        m.variation as variation,
        m.members as members,
        t.tab_view_60_cnt as tab_view_60_cnt,
        t.tab_view_120_cnt as tab_view_120_cnt,
        t.tab_view_180_cnt as tab_view_180_cnt,
        t.tab_view_300_cnt as tab_view_300_cnt,
        t.tab_view_600_cnt as tab_view_600_cnt,
        (tv.tab_view_avg * tv.tab_view_user) / m.members as tab_view_avg,
        (tv.tab_view_60_avg * tv.tab_view_user) / m.members as tab_view_60_avg,
        (tv.tab_view_120_avg * tv.tab_view_user) / m.members as tab_view_120_avg,
        (tv.tab_view_180_avg * tv.tab_view_user) / m.members as tab_view_180_avg,
        (tv.tab_view_300_avg * tv.tab_view_user) / m.members as tab_view_300_avg,
        (tv.tab_view_600_avg * tv.tab_view_user) / m.members as tab_view_600_avg,
        (tv.tab_view_var * (tv.tab_view_user - 1) + tv.tab_view_user * pow((tv.tab_view_avg - tab_view_avg), 2) + (m.members - tv.tab_view_user) * pow((0 - tab_view_avg), 2)) / m.members as tab_view_var,
        (tv.tab_view_60_var * (tv.tab_view_user - 1) + tv.tab_view_user * pow((tv.tab_view_60_avg - tab_view_60_avg), 2) + (m.members - tv.tab_view_user) * pow((0 - tab_view_60_avg), 2)) / m.members as tab_view_60_var,
        (tv.tab_view_120_var * (tv.tab_view_user - 1) + tv.tab_view_user * pow((tv.tab_view_120_avg - tab_view_120_avg), 2) + (m.members - tv.tab_view_user) * pow((0 - tab_view_120_avg), 2)) / m.members as tab_view_120_var,
        (tv.tab_view_180_var * (tv.tab_view_user - 1) + tv.tab_view_user * pow((tv.tab_view_180_avg - tab_view_180_avg), 2) + (m.members - tv.tab_view_user) * pow((0 - tab_view_180_avg), 2)) / m.members as tab_view_180_var,
        (tv.tab_view_300_var * (tv.tab_view_user - 1) + tv.tab_view_user * pow((tv.tab_view_300_avg - tab_view_300_avg), 2) + (m.members - tv.tab_view_user) * pow((0 - tab_view_300_avg), 2)) / m.members as tab_view_300_var,
        (tv.tab_view_600_var * (tv.tab_view_user - 1) + tv.tab_view_user * pow((tv.tab_view_600_avg - tab_view_600_avg), 2) + (m.members - tv.tab_view_user) * pow((0 - tab_view_600_avg), 2)) / m.members as tab_view_600_var
    from
        members_agg as m
    left join
        tab_view as t
    on
        m.dt = t.dt
    and
        m.variation = t.variation
    left join
        tab_view_per_user as tv
    on
        m.dt = tv.dt
    and
        m.variation = tv.variation
)

select
    *
from
    vars
left join
    long_tab
using(dt, variation)
