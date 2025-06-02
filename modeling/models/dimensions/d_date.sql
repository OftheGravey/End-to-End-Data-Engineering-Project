SELECT
    date::date AS date,
    gen_random_uuid() AS date_sk,
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(QUARTER FROM date) AS quarter,
    EXTRACT(MONTH FROM date) AS month,
    TO_CHAR(date, 'Month') AS month_name,
    EXTRACT(DAY FROM date) AS day,
    TO_CHAR(date, 'Day') AS day_name,
    EXTRACT(WEEK FROM date) AS week_of_year,
    CASE
        WHEN EXTRACT(DOW FROM date) IN (0, 6) THEN 'Weekend'
        ELSE 'Weekday'
    END AS weekday_weekend_flag
FROM
    generate_series(
        DATE '2010-01-01',
        DATE '2030-12-31',
        INTERVAL '1 day'
    ) AS t(date)