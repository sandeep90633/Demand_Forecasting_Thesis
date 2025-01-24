
INSERT INTO stock_status
SELECT
    stock_status,
    COUNT(*) as item_count
FROM
    inventory_alert_table
GROUP BY stock_status
ORDER BY item_count DESC