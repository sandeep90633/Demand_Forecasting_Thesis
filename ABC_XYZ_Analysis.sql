CREATE OR REPLACE PROCEDURE CalculateABCXYZClassification()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Drop temporary tables if they exist
    DROP TABLE IF EXISTS revenue;
    DROP TABLE IF EXISTS total_revenue_cal;
    DROP TABLE IF EXISTS cumulative_share_cal;
    DROP TABLE IF EXISTS date_range;
    DROP TABLE IF EXISTS weekly_dates;
    DROP TABLE IF EXISTS merge_dates_ids;
    DROP TABLE IF EXISTS orders_per_week;
    DROP TABLE IF EXISTS avg_weekly_orders;
    DROP TABLE IF EXISTS avg_weekly_demand;
    DROP TABLE IF EXISTS sd_of_weekly_orders;
    DROP TABLE IF EXISTS cv_of_weekly_orders;
    DROP TABLE IF EXISTS rank_of_cv;

    CREATE TEMPORARY TABLE revenue AS (
        SELECT 
            a.sku_id,
            COALESCE(b.revenue_per_id, 0) AS revenue_per_id
        FROM 
            stock a
        LEFT JOIN 
            (
                SELECT 
                    sku_id,
                    SUM(revenue) AS revenue_per_id
                FROM
                    sales
                GROUP BY sku_id
                ORDER BY revenue_per_id DESC
            ) AS b 
        ON a.sku_id = b.sku_id 
    );

    CREATE TEMPORARY TABLE total_revenue_cal AS (
        SELECT 
            sku_id,
            revenue_per_id,
            COALESCE((revenue_per_id / total_revenue.total) * 100, 0) AS percentage_revenue_per_id
        FROM 
            revenue,
            (SELECT SUM(revenue_per_id) AS total FROM revenue) AS total_revenue
        ORDER BY percentage_revenue_per_id DESC
    );

    CREATE TEMPORARY TABLE cumulative_share_cal AS (
        SELECT 
            a.sku_id,
            a.revenue_per_id,
            a.percentage_revenue_per_id,
            ROUND(SUM(b.percentage_revenue_per_id)) AS cumulative_share
        FROM
            total_revenue_cal AS a
        INNER JOIN 
            total_revenue_cal AS b ON a.percentage_revenue_per_id <= b.percentage_revenue_per_id
        GROUP BY
            a.sku_id, a.revenue_per_id, a.percentage_revenue_per_id
        ORDER BY
            a.percentage_revenue_per_id DESC
    );

    CREATE TEMPORARY TABLE date_range AS (
        SELECT 
            MIN(order_date) AS min_date,
            MAX(order_date) AS max_date
        FROM sales
    );

    CREATE TEMPORARY TABLE weekly_dates AS (
        SELECT 
            generate_series(
                (SELECT min_date - (EXTRACT(DOW FROM min_date)::INTEGER) FROM date_range),
                (SELECT max_date FROM date_range),
                '7 days'::INTERVAL
            ) AS week_start_date
    );

    CREATE TEMPORARY TABLE merge_dates_ids AS (
        SELECT 
            a.week_start_date, 
            b.sku_id,
            b.sku_name
        FROM 
            weekly_dates a
        CROSS JOIN sku_items b
        ORDER BY a.week_start_date
    );

    CREATE TEMPORARY TABLE	orders_per_week AS(
        SELECT 
            a.sku_id,
            a.sku_name,
            b.week_start_date,
            COALESCE(SUM(b.order_quantity), 0) AS order_quantity_per_id
        FROM
            merge_dates_ids a
        LEFT JOIN 
            (SELECT 
                sku_id, 
                order_date - (EXTRACT(DOW FROM order_date)::INTEGER) * INTERVAL '1 day' AS week_start_date,
                order_quantity
            FROM 
                sales
            ) AS b
        ON
            a.week_start_date = b.week_start_date AND a.sku_id = b.sku_id
        GROUP BY 
            a.sku_id, a.sku_name, b.week_start_date
        ORDER BY 
            b.week_start_date
    );

    CREATE TEMPORARY TABLE avg_weekly_orders AS (
        SELECT 
            sku_id,
            CAST(AVG(order_quantity_per_id) AS numeric) + 0 AS avg_weekly_orders
        FROM
            orders_per_week
        GROUP BY sku_id
    );

    CREATE TEMPORARY TABLE avg_weekly_demand AS (
        SELECT 
            a.sku_id,
            a.revenue_per_id,
            b.avg_weekly_orders
        FROM 
            cumulative_share_cal a
        JOIN 
            avg_weekly_orders b
        ON a.sku_id = b.sku_id
    );

    CREATE TEMPORARY TABLE sd_of_weekly_orders AS (
        SELECT 
            a.*,
            b.sd_of_weekly_demand
        FROM 
            avg_weekly_demand a
        JOIN
            (SELECT 
                sku_id,
                stddev(order_quantity_per_id) AS sd_of_weekly_demand
            FROM orders_per_week
            GROUP BY sku_id) b
        ON a.sku_id = b.sku_id
    );

    CREATE TEMPORARY TABLE cv_of_weekly_orders AS (
        SELECT 
            *,
            CASE 
                WHEN avg_weekly_orders > 0 THEN (sd_of_weekly_demand / avg_weekly_orders)
                ELSE 100 
            END AS cv_of_weekly_orders
        FROM
            sd_of_weekly_orders
    );

    CREATE TEMPORARY TABLE rank_of_cv AS (
        SELECT 
            *,
            RANK() OVER (ORDER BY cv_of_weekly_orders) AS rank_of_cv
        FROM 
            cv_of_weekly_orders
    );

    DROP TABLE IF EXISTS ABC_XYZ_ANALYSIS;

    CREATE TABLE ABC_XYZ_ANALYSIS AS (
        SELECT 
            a.sku_id,
            a.revenue_per_id,
            CASE
                WHEN a.cumulative_share <= 70 THEN 'A[HV]'
                WHEN a.cumulative_share <= 90 THEN 'B[MV]'
                ELSE 'C[LV]'
            END AS ABC,
			b.avg_weekly_orders,
            b.sd_of_weekly_demand,
            b.cv_of_weekly_orders,
            CASE 
                WHEN b.rank_of_cv <= 0.25 * MAX(b.rank_of_cv) OVER () THEN 'X[SD]'
                WHEN b.rank_of_cv <= 0.50 * MAX(b.rank_of_cv) OVER () THEN 'Y[VD]'
                ELSE 'Z[UD]'
            END AS XYZ
        FROM 
            cumulative_share_cal a
        JOIN 
            rank_of_cv b
        ON a.sku_id = b.sku_id
    );
END;
$$;

CALL CalculateABCXYZClassification();

select * from ABC_XYZ_ANALYSIS
