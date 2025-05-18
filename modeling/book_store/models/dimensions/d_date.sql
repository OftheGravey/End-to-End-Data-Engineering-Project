-- noqa: disable=RF04
SELECT
    cast(date AS date) AS date,
    gen_random_uuid() AS date_sk,
    date_part('year', date) AS year,
    date_part('quarter', date) AS quarter,
    date_part('month', date) AS month,
    strftime('%B', date) AS month_name,
    date_part('day', date) AS day,
    strftime('%A', date) AS day_name,
    date_part('week', date) AS week_of_year,
    CASE
        WHEN strftime('%w', date) IN ('0', '6') THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend_flag
FROM
    generate_series(
        date '2010-01-01',
        date '2030-12-31',
        INTERVAL 1 DAY
    ) AS t (date)
