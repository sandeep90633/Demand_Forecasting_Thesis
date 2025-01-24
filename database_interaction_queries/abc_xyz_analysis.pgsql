INSERT INTO abc_xyz_table
WITH revenue AS (
    SELECT 
        a.sku_id,
        COALESCE(b.revenue_per_id, 0) AS revenue_per_id
    FROM inventory a
    LEFT JOIN (
        SELECT 
            sku_id,
            SUM(revenue) AS revenue_per_id
        FROM sales
        GROUP BY sku_id
    ) b ON a.sku_id = b.sku_id
),

total_revenue_cal AS (
    SELECT 
        sku_id,
        revenue_per_id,
        COALESCE((revenue_per_id / total_revenue.total) * 100, 0) AS percentage_revenue_per_id
    FROM revenue, 
        (SELECT SUM(revenue_per_id) AS total FROM revenue) AS total_revenue
),

cumulative_share_cal AS (
    SELECT 
        a.sku_id,
        a.revenue_per_id,
        a.percentage_revenue_per_id,
        ROUND(SUM(b.percentage_revenue_per_id)) AS cumulative_share
    FROM total_revenue_cal a
    INNER JOIN total_revenue_cal b ON a.percentage_revenue_per_id <= b.percentage_revenue_per_id
    GROUP BY a.sku_id, a.revenue_per_id, a.percentage_revenue_per_id
),

date_range AS (
    SELECT 
        MIN(order_date) AS min_date,
        MAX(order_date) AS max_date
    FROM sales
),

weekly_dates AS (
    SELECT 
        generate_series(
            (SELECT min_date - (EXTRACT(DOW FROM min_date)::INTEGER) FROM date_range),
            (SELECT max_date FROM date_range),
            '7 days'::INTERVAL
        ) AS week_start_date
),

merge_dates_ids AS (
    SELECT 
        a.week_start_date, 
        b.sku_id
    FROM weekly_dates a
    CROSS JOIN inventory b
),

orders_per_week AS (
    SELECT 
        a.sku_id,
        b.week_start_date,
        COALESCE(SUM(b.order_quantity), 0) AS order_quantity_per_id
    FROM merge_dates_ids a
    LEFT JOIN (
        SELECT 
            sku_id, 
            order_date - (EXTRACT(DOW FROM order_date)::INTEGER) * INTERVAL '1 day' AS week_start_date,
            order_quantity
        FROM sales
    ) b ON a.week_start_date = b.week_start_date AND a.sku_id = b.sku_id
    GROUP BY a.sku_id, b.week_start_date
),

avg_weekly_orders AS (
    SELECT 
        sku_id,
        CAST(AVG(order_quantity_per_id) AS numeric) AS avg_weekly_orders
    FROM orders_per_week
    GROUP BY sku_id
),

avg_weekly_demand AS (
    SELECT 
        a.sku_id,
        a.revenue_per_id,
        b.avg_weekly_orders
    FROM cumulative_share_cal a
    JOIN avg_weekly_orders b ON a.sku_id = b.sku_id
),

sd_of_weekly_orders AS (
    SELECT 
        a.*,
        b.sd_of_weekly_demand
    FROM avg_weekly_demand a
    JOIN (
        SELECT 
            sku_id,
            STDDEV(order_quantity_per_id) AS sd_of_weekly_demand
        FROM orders_per_week
        GROUP BY sku_id
    ) b ON a.sku_id = b.sku_id
),

cv_of_weekly_orders AS (
    SELECT 
        *,
        CASE 
            WHEN avg_weekly_orders > 0 THEN (sd_of_weekly_demand / avg_weekly_orders)
            ELSE 100 
        END AS cv_of_weekly_orders
    FROM sd_of_weekly_orders
),

rank_of_cv AS (
    SELECT 
        *,
        RANK() OVER (ORDER BY cv_of_weekly_orders) AS rank_of_cv
    FROM cv_of_weekly_orders
)

-- Final Table: ABC_XYZ_ANALYSIS
SELECT 
    a.sku_id,
    CASE
        WHEN a.cumulative_share <= 70 THEN 'A[HV]'
        WHEN a.cumulative_share <= 90 THEN 'B[MV]'
        ELSE 'C[LV]'
    END AS abc_category,
    CASE 
        WHEN b.rank_of_cv <= 0.25 * MAX(b.rank_of_cv) OVER () THEN 'X[SD]'
        WHEN b.rank_of_cv <= 0.50 * MAX(b.rank_of_cv) OVER () THEN 'Y[VD]'
        ELSE 'Z[UD]'
    END AS xyz_category
FROM cumulative_share_cal a
JOIN rank_of_cv b ON a.sku_id = b.sku_id;