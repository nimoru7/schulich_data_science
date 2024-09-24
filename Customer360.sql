USE mban_db;
-- CTE for Static Customer/Conversion Data
WITH CustomerConversionData AS (
    SELECT
        cd.customer_id,
        cd.first_name,
        cd.last_name,
        cnv.conversion_id,
        ROW_NUMBER() OVER (PARTITION BY cd.customer_id ORDER BY cnv.conversion_date) AS conversion_number,
        cnv.conversion_type,
        cnv.conversion_date,
        dd.year_week AS conversion_week,
        LEAD(dd.year_week) OVER (PARTITION BY cd.customer_id ORDER BY cnv.conversion_date) AS next_conversion_week,
        cnv.conversion_channel
    FROM fact_tables.conversions cnv
    JOIN dimensions.customer_dimension cd ON cnv.fk_customer = cd.sk_customer
    JOIN dimensions.date_dimension dd ON cnv.fk_conversion_date = dd.sk_date
),

-- CTE for First Order Placed Data
FirstOrderPlaced AS (
    SELECT
        cd.customer_id,
        cnv.conversion_id,
        MIN(dd.year_week) AS first_order_week,
        MIN(o.price_paid) AS first_order_total_paid,
        pd.product_name AS first_order_product
    FROM fact_tables.orders o
    JOIN fact_tables.conversions cnv ON o.order_number = cnv.order_number
    JOIN dimensions.date_dimension dd ON o.fk_order_date = dd.sk_date
    JOIN dimensions.customer_dimension cd ON o.fk_customer = cd.sk_customer
    JOIN dimensions.product_dimension pd ON o.fk_product = pd.sk_product
    GROUP BY cd.customer_id, cnv.conversion_id, pd.product_name
),

-- CTE for Order History
OrderHistory AS (
    SELECT
        cd.customer_id,
        dd.year_week AS order_week,
        COUNT(o.order_id) AS orders_placed,
        SUM(o.unit_price) AS total_before_discounts,
        SUM(o.discount_value) AS total_discounts,
        SUM(o.price_paid) AS total_paid_in_week
    FROM fact_tables.orders o
    JOIN dimensions.date_dimension dd ON o.fk_order_date = dd.sk_date
    JOIN dimensions.customer_dimension cd ON o.fk_customer = cd.sk_customer
    GROUP BY cd.customer_id, dd.year_week
),

-- CTE to Generate All Possible Weeks for Each Customer within Conversion Periods
AllWeeks AS (
    SELECT
        DISTINCT cd.customer_id,
        dd.year_week
    FROM dimensions.date_dimension dd
    CROSS JOIN dimensions.customer_dimension cd
),

-- CTE to Generate Weekly Data
WeeklyData AS (
    SELECT
        cc.customer_id,
        cc.first_name,
        cc.last_name,
        cc.conversion_id,
        cc.conversion_number,
        cc.conversion_type,
        cc.conversion_date,
        cc.conversion_week,
        cc.next_conversion_week,
        cc.conversion_channel,
        fo.first_order_week,
        fo.first_order_total_paid,
        fo.first_order_product,
        aw.year_week AS order_week,
        ROW_NUMBER() OVER (PARTITION BY cc.customer_id, cc.conversion_id ORDER BY aw.year_week) AS week_counter,
        COALESCE(oh.orders_placed, 0) AS order_placed,
        COALESCE(oh.total_before_discounts, 0) AS total_before_discounts,
        COALESCE(oh.total_discounts, 0) AS total_discounts,
        COALESCE(oh.total_paid_in_week, 0) AS total_paid,
        SUM(COALESCE(oh.total_paid_in_week, 0)) OVER (PARTITION BY cc.customer_id, cc.conversion_id ORDER BY aw.year_week) AS conversion_cumulative_revenue,
        SUM(COALESCE(oh.total_paid_in_week, 0)) OVER (PARTITION BY cc.customer_id ORDER BY aw.year_week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS lifetime_cumulative_revenue
    FROM CustomerConversionData cc
    LEFT JOIN FirstOrderPlaced fo ON cc.customer_id = fo.customer_id AND cc.conversion_id = fo.conversion_id
    LEFT JOIN AllWeeks aw ON cc.customer_id = aw.customer_id AND aw.year_week BETWEEN cc.conversion_week AND COALESCE(cc.next_conversion_week, aw.year_week)
    LEFT JOIN OrderHistory oh ON cc.customer_id = oh.customer_id AND aw.year_week = oh.order_week
)

-- Select from the final WeeklyData CTE
SELECT *
FROM WeeklyData
ORDER BY customer_id, conversion_number, week_counter;