SELECT
    date::date AS date,
    gen_random_uuid() AS date_sk,
    extract(YEAR FROM date) AS year,
    extract(QUARTER FROM date) AS quarter,
    extract(MONTH FROM date) AS month,
    to_char(date, 'Month') AS month_name,
    extract(DAY FROM date) AS day,
    to_char(date, 'Day') AS day_name,
    extract(WEEK FROM date) AS week_of_year,
    CASE
        WHEN extract(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend_flag
FROM
    generate_series(
        date '2010-01-01',
        date '2030-12-31',
        interval '1 day'
    ) AS t (date)
