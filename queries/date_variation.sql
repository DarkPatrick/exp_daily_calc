select
    toDate('{date}', 'UTC') as dt,
    arrayJoin({variations}) as variation
