select
    toDate('{date}') as dt,
    arrayJoin({variations}) as variation
