INSERT INTO abc_xyz_table
WITH overall_revenue_per_id AS (
    SELECT 
        b.sku_id,
        COALESCE(a.revenue_per_id, 0) as revenue_per_id
    FROM 
        (SELECT 
            sku_id,
            SUM(revenue) AS revenue_per_id
        FROM
            sales
        GROUP BY sku_id
        ORDER BY revenue_per_id DESC) a
    FULL OUTER JOIN
        inventory AS b 
    ON a.sku_id = b.sku_id
),
    cumulative_percentage AS (
    SELECT 
        sku_id,
        revenue_per_id,
        SUM(revenue_per_id) OVER(ORDER BY revenue_per_id DESC) / SUM(revenue_per_id) OVER() AS cumulative_percentage
    FROM 
        overall_revenue_per_id
),
    abc_classification AS (
    SELECT 
        sku_id,
        CASE 
            WHEN cumulative_percentage <= 0.8 THEN 'A'
            WHEN cumulative_percentage <= 0.95 THEN 'B'
            ELSE 'C'
        END AS abc

    FROM cumulative_percentage
),
    weekly_sales AS (
    SELECT  
        sku_id,
        date_trunc('week', order_date) AS week,
        SUM(order_quantity) AS weekly_sales_per_id
    FROM 
        sales
    GROUP BY
        sku_id, date_trunc('week', order_date)
), 
    standard_deviation AS (
    SELECT
        sku_id,
        STDDEV(weekly_sales_per_id) as sales_variability
    FROM 
        weekly_sales
    GROUP BY
        sku_id
),
    xyz_classification AS (
    SELECT 
        sku_id,
        CASE 
            WHEN sales_variability < 10 THEN 'X[SD]'
            WHEN sales_variability BETWEEN 10 AND 20 THEN 'Y[VD]'
            ELSE 'Z[UD]'
        END AS xyz
    FROM    
        standard_deviation
)

SELECT
    a.sku_id,
    a.abc as abc_category,
    b.xyz as xyz_category
FROM    
    abc_classification a
JOIN 
    xyz_classification b
ON a.sku_id=b.sku_id