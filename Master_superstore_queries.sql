
-- Query to return the total sales, total profit, profit margin, % of sales and % of total profit by each of the categories

SELECT 
  p.category,
  ROUND(SUM(o.sales), 2) AS total_sales,
  ROUND(SUM(o.profit), 2) AS total_profit,
  ROUND(SUM(o.profit) / SUM(o.sales), 2) AS profit_margin,
  ROUND(SUM(o.sales) * 100.0 / SUM(SUM(o.sales)) OVER (), 2) AS sales_pct_of_total,
  ROUND(SUM(o.profit) * 100.0 / SUM(SUM(o.profit)) OVER (), 2) AS profit_pct_of_total
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category
ORDER BY total_sales DESC;

-- Query to show the least profitable products, and there corrosponding profit margin, sales, orders and quantity sold

SELECT 
  p.category,
  p.product_name,
  COUNT(o.order_id) AS number_of_orders,
  SUM(o.quantity) AS total_quantity_sold,
  ROUND(SUM(o.sales), 2) AS total_sales,
  ROUND(SUM(o.profit), 2) AS total_profit,
  ROUND(SUM(o.profit) / SUM(o.sales), 2) AS profit_margin
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category, p.product_name
HAVING total_profit < 0
ORDER BY total_profit ASC;

-- Query to show the most profitable products, and there corrosponding profit margin, sales, orders and quantity sold

 SELECT 
  p.category,
  p.product_name,
  COUNT(o.order_id) AS number_of_orders,
  SUM(o.quantity) AS total_quantity_sold,
  ROUND(SUM(o.sales), 2) AS total_sales,
  ROUND(SUM(o.profit), 2) AS total_profit,
  ROUND(SUM(o.profit) / SUM(o.sales), 2) AS profit_margin
FROM orders o
JOIN products p ON o.product_id = p.product_id
GROUP BY p.category, p.product_name
ORDER BY total_profit DESC;

-- Query to show top 5 customers by sales amount 

SELECT 
  c.customer_id,
  c.customer_name,
  COUNT(DISTINCT o.order_id) AS number_of_orders,
  SUM(o.quantity) AS total_quantity_purchased,
  ROUND(SUM(o.sales), 2) AS total_sales,
  ROUND(SUM(o.sales) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM orders o
JOIN customers AS c
ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY total_sales DESC
LIMIT 5;

-- Query to show top 5 customers by order amount 

SELECT 
  c.customer_id,
  c.customer_name,
  COUNT(DISTINCT o.order_id) AS number_of_orders,
  SUM(o.quantity) AS total_quantity_purchased,
  ROUND(SUM(o.sales), 2) AS total_sales,
  ROUND(SUM(o.sales) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM orders o
JOIN customers AS c
ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY number_of_orders DESC
LIMIT 5;

-- Query to show eahc States sales, profit, prof margins and sales as % of total sales 

SELECT 
  l.region,
  l.state,
  ROUND(SUM(o.sales), 2) AS total_sales,
  ROUND(SUM(o.profit), 2) AS total_profit,
  ROUND(SUM(o.profit) * 100.0 / SUM(SUM(o.profit)) OVER (), 2) AS profit_pct_of_total,
  ROUND(SUM(o.sales) * 100.0 / SUM(SUM(o.sales)) OVER (), 2) AS sales_pct_of_total
FROM orders o
JOIN customers AS c
ON o.customer_id = c.customer_id
JOIN locations AS l 
ON c.postal_code = l.postal_code AND c.postal_code = l.postal_code
GROUP BY l.region, l.state
ORDER BY total_sales DESC;

-- Query to show the breakdown of shipping modes and the effect they have on orders, items sold, sales, profit etc

SELECT 
  ship_mode,
  COUNT(DISTINCT customer_id) AS unique_customers,
  COUNT(DISTINCT order_id) AS total_orders,
  SUM(quantity) AS total_items_sold,
  ROUND(SUM(sales), 2) AS total_sales,
  ROUND(SUM(profit), 2) AS total_profit,
  ROUND(SUM(profit) / NULLIF(SUM(sales), 0), 2) AS profit_margin,
  ROUND(SUM(sales) / COUNT(DISTINCT order_id), 2) AS avg_order_value,
  ROUND(SUM(quantity) / COUNT(DISTINCT order_id), 2) AS avg_items_per_order
FROM orders
GROUP BY ship_mode
ORDER BY total_sales DESC;

-- Query to bundle the differenet discount rates and return num of sales, average sales & profit and profit margin
-- Key skill here was the use of the case statments, so was good to get practice in here. 

SELECT 
  discount_band,
  COUNT(*) AS number_of_orders,
  ROUND(AVG(sales), 2) AS avg_sales,
  ROUND(AVG(profit), 2) AS avg_profit,
  ROUND(SUM(profit) / NULLIF(SUM(sales), 0), 2) AS profit_margin
FROM (
  SELECT 
    *,
    CASE 
      WHEN discount = 0 THEN 'No Discount'
      WHEN discount > 0 AND discount <= 0.1 THEN 'Low (0–10%)'
      WHEN discount > 0.1 AND discount <= 0.3 THEN 'Medium (11–30%)'
      WHEN discount > 0.3 THEN 'High (>30%)'
    END AS discount_band
  FROM orders
) AS sub
GROUP BY discount_band
ORDER BY 
  CASE 
    WHEN discount_band = 'No Discount' THEN 1
    WHEN discount_band = 'Low (0–10%)' THEN 2
    WHEN discount_band = 'Medium (11–30%)' THEN 3
    WHEN discount_band = 'High (>30%)' THEN 4
    ELSE 5
  END;
  
  
  -- Query to show the sales & profit broken down month on month 
  
  SELECT 
  monthly_data.order_month,
  monthly_data.total_sales,
  monthly_data.total_profit,
  ROUND(monthly_data.total_profit / NULLIF(monthly_data.total_sales, 0), 2) AS profit_margin
FROM (
  SELECT 
    DATE_FORMAT(STR_TO_DATE(order_date, '%d/%m/%Y'), '%Y-%m') AS order_month,
    SUM(sales) AS total_sales,
    SUM(profit) AS total_profit
  FROM orders
  GROUP BY DATE_FORMAT(STR_TO_DATE(order_date, '%d/%m/%Y'), '%Y-%m')
) AS monthly_data
ORDER BY monthly_data.order_month;

-- Query to show cumulative orders, month on month by category
-- This query was especially good for praticing my partitioning

SELECT
    DATE_FORMAT(STR_TO_DATE(o.order_date, '%d/%m/%Y'), '%Y-%m') AS order_month,
    p.category,
    SUM(o.quantity) AS items_this_month,
    SUM(SUM(o.quantity)) OVER (PARTITION BY p.category ORDER BY DATE_FORMAT(STR_TO_DATE(o.order_date, '%d/%m/%Y'), '%Y-%m')
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_items_ordered
FROM Orders o
JOIN Products p ON o.product_ID = p.product_ID
GROUP BY
    DATE_FORMAT(STR_TO_DATE(o.order_Date, '%d/%m/%Y'), '%Y-%m'),
    p.Category
ORDER BY
    p.Category,
    order_month;

-- After researching how to take my SQL code to the next level I came across scheduled queries, and thought it useful to include one
-- As this is a static data set this wont have an affect here, but could be used ti automatically query, 
-- and update a table which could pulled straight into Power BI / Excel

CREATE TABLE daily_sales_summary (
    summary_date DATE,
    total_sales DECIMAL(10,2),
    total_profit DECIMAL(10,2)
);

CREATE EVENT daily_summary_event
ON SCHEDULE EVERY 1 DAY
STARTS CURRENT_TIMESTAMP
DO
  INSERT INTO daily_sales_summary (summary_date, total_sales, total_profit)
  SELECT CURDATE(), SUM(sales), SUM(profit)
  FROM orders
  WHERE DATE(order_date) = CURDATE();
