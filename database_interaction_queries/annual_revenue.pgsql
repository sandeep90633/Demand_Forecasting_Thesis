ALTER TABLE inventory ADD COLUMN annual_sales_quantity REAL;

UPDATE inventory a
SET annual_sales_quantity = COALESCE(b.order_quantity, 0)
FROM (
    SELECT 
        i.sku_id,
        COALESCE(SUM(s.order_quantity), 0) AS order_quantity
    FROM inventory i
    LEFT JOIN sales s
    ON i.sku_id = s.sku_id
    WHERE s.order_date > (
        SELECT MAX(order_date) FROM sales
    ) - INTERVAL '365' DAY
    GROUP BY i.sku_id
) b
WHERE a.sku_id = b.sku_id;

ALTER TABLE inventory ADD COLUMN annual_revenue REAL;

UPDATE inventory
SET annual_revenue = unit_price * annual_sales_quantity