{{
    config(
        materialized = 'ephemeral'
    )
}}
select
    dateadd('d', seq4(), current_date() - 7) as date
from table(generator(rowcount=>7))