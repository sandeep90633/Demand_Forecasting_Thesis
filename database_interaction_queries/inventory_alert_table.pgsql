INSERT INTO inventory_alert_table
with join_table as (
    SELECT
        a.sku_id,
        a.current_inventory_quantity,
        COALESCE(b.predicted_quantity,0) as predicted_quantity
    FROM
        inventory a
    LEFT JOIN
        weekly_predicted_data b
    ON a.sku_id = b.sku_id
)

SELECT
    *,
    CASE
        WHEN current_inventory_quantity > 3*predicted_quantity
            THEN 'Excessive_Stock'
        WHEN current_inventory_quantity BETWEEN predicted_quantity and 1.3*predicted_quantity
            THEN 'Just_Enough'
        WHEN current_inventory_quantity < predicted_quantity
            THEN 'Shortage_Risk'
        ELSE 'Sufficient_Stock'
    END AS stock_status
FROM
    join_table
ORDER BY sku_id