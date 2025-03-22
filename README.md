# stephens-repository

-- code for insights -- 425 end

CREATE DATABASE IF NOT EXISTS ecommerce;

USE ecommerce;

CREATE TABLE orders /* creation of first table */
(
order_id VARCHAR(255) NOT NULL,
customer_id VARCHAR(255) NOT NULL,
order_status VARCHAR(255),
order_purchase_timestamp TIMESTAMP,
order_approved_at TIMESTAMP,
order_delivered_carrier_date TIMESTAMP,
order_delivered_customer_date TIMESTAMP,
order_estimated_delivery_date DATE,
PRIMARY KEY (order_id),
FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
); -- end of creation of first table

SELECT * FROM orders;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_orders_dataset.csv' INTO TABLE orders
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into orders table 

UPDATE orders SET order_approved_at = NULL WHERE order_approved_at = 0000-00-00;
UPDATE orders SET order_delivered_carrier_date = NULL WHERE order_delivered_carrier_date = 0000-00-00;
UPDATE orders SET order_delivered_customer_date = NULL WHERE order_delivered_customer_date = 0000-00-00;

SHOW VARIABLES LIKE "local_infile";
/* check for local infile value and enable it */
SET GLOBAL local_infile = 1;

-- --------------------------------------------------------------------------------------------------

CREATE TABLE product_category_name /* creation of second table */
(
product_category_name VARCHAR(255),
product_category_name_english VARCHAR(255)
); -- end of creation of second table

SELECT * FROM product_category_name;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/product_category_name_translation.csv' INTO TABLE product_category_name
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into product_category_name table

-- --------------------------------------------------------------------------------------------------
  
CREATE TABLE sellers /* creation of third table */
(
seller_id VARCHAR(255) NOT NULL,
seller_zip_code_prefix INT,
seller_city VARCHAR(255),
seller_state VARCHAR(255),
PRIMARY KEY (seller_id)
); -- end of creation of third table
  
select * FROM sellers;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_sellers_dataset.csv' INTO TABLE sellers
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel into sellers table

-- --------------------------------------------------------------------------------------------------

CREATE TABLE products /* creation of fourth table */
(
product_id VARCHAR(255),
product_category_name VARCHAR(255),
product_description_lenght INT,
product_photos_quantity INT,
product_weight_g INT,
product_length_cm INT,
product_height_cm INT,
product_width_cm INT
); -- end of creation of fourth table 

SELECT * FROM products;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_products_dataset.csv' INTO TABLE products
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data excel from file into products table

UPDATE products SET product_category_name = NULL WHERE product_category_name = '' OR 0;
UPDATE products SET product_description_lenght = NULL WHERE product_description_lenght = '' OR 0;
UPDATE products SET product_photos_quantity = NULL WHERE product_photos_quantity = '' OR 0;
UPDATE products SET product_weight_g = NULL WHERE product_weight_g = '' OR 0;
UPDATE products SET product_length_cm = NULL WHERE product_length_cm = '' OR 0;
UPDATE products SET product_height_cm = NULL WHERE product_height_cm = '' OR 0;
UPDATE products SET product_width_cm = NULL WHERE product_width_cm = '' OR 0;
UPDATE products SET product_id = TRIM(both '"' FROM product_id);

-- --------------------------------------------------------------------------------------------------

CREATE TABLE order_reviews /* creation of fifth table */
(
review_id VARCHAR(255),
order_id VARCHAR(255),
review_score INT,
review_comment_title VARCHAR(255),
review_comment_message VARCHAR(9999),
review_creation_date date,
review_answer_timestamp TIMESTAMP
); -- end of creation of fifth table

SELECT * FROM order_reviews;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_order_reviews_dataset.csv' INTO TABLE order_reviews
FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
ESCAPED BY ''
LINES TERMINATED BY '\n'
IGNORE 1 LINES; -- load data from excel file into order_reviews table

UPDATE order_reviews SET review_comment_title = NULL WHERE review_comment_title = '' OR 0;
UPDATE order_reviews SET review_comment_message = NULL WHERE review_comment_message = '' OR 0;

-- --------------------------------------------------------------------------------------------------

create table order_payments /* creation of sixth table */
(
order_id VARCHAR(255),
payment_sequential INT,
payment_type VARCHAR(255),
payment_installations INT,
payment_value DECIMAL(65,2)
); -- end of creation of sixth table

SELECT * FROM order_payments;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_order_payments_dataset.csv' INTO TABLE order_payments
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into order_payment table

UPDATE order_payments SET payment_value = NULL WHERE payment_value = 0.00;
UPDATE order_payments SET order_id = TRIM(both '"' FROM order_id);

-- --------------------------------------------------------------------------------------------------

CREATE TABLE order_items /* creation of seventh table */
(
order_id VARCHAR(255),
order_item_ount INT,
product_id VARCHAR(255),
seller_id VARCHAR(255),
shipping_limit_date TIMESTAMP,
price DECIMAL(65,2),
freight_value DECIMAL(65,2)
); -- end of creation of seventh table

SELECT * FROM order_items;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_order_items_dataset.csv' INTO TABLE order_items
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into order_items table

UPDATE order_items SET freight_value = NULL WHERE freight_value = 0.00;

-- --------------------------------------------------------------------------------------------------

CREATE TABLE geolocation /* creation of eighth table*/
(
geolocation_zip_code_prefix INT,
geolocation_lat DECIMAL(65,8),
geolocation_lon DECIMAL(65,8),
geolocation_city VARCHAR(255),
geolocation_state VARCHAR(255)
); -- end of creation of eighth table

SELECT * FROM geolocation;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_geolocation_dataset.csv' INTO TABLE geolocation
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel into geolocation table

-- --------------------------------------------------------------------------------------------------

CREATE TABLE customers /* creation of ninth table */
(
customer_id VARCHAR(255) NOT NULL,
customer_unique_id VARCHAR(255) NOT NULL,
customer_zip_code_prefix INT,
customer_city VARCHAR(255),
customer_state VARCHAR(255),
PRIMARY KEY (customer_id)
); -- end of creation of ninth table

SELECT * FROM customers;

LOAD DATA LOCAL INFILE 'C:/Users/gigam/Documents/mysql ecommerce datasets/olist_customers_dataset.csv' INTO TABLE customers
FIELDS TERMINATED BY ','
IGNORE 1 LINES; -- load data from excel file into customers table

-- -------------------------------------------------------------------------------------------------------------------------------------------------------------------------

code for insights laid out in order they were mentioned

SELECT AVG(price_per_month), AVG(orders) 
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders,
          SUM(price) AS price_per_month
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY month
          ORDER BY month
          ) sub; -- average revenue per month for dataset 826,058.522500 avg orders 6874

SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(orders.order_id) AS orders
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year, month; -- orders per month with year for entire dataset

SELECT AVG(sub1_avg_review_score), AVG(sub2_avg_review_score), AVG(sub3_avg_review_score) 
    FROM (
          SELECT customer_state AS sub1_customer_state, AVG(review_score) AS sub1_avg_review_score
              FROM (
                    SELECT *  
				        FROM (
                              SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
					          order_delivered_carrier_date,
                              LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                              order_estimated_delivery_date, review_score
                              FROM ecommerce.orders
                              LEFT JOIN order_reviews
                              ON orders.order_id = order_reviews.order_id
                              LEFT JOIN customers
                              ON orders.customer_id = customers.customer_id
                              ) sub1
                    WHERE order_delivered_customer_date > order_estimated_delivery_date
                    ORDER BY review_score DESC
                    ) sub2
          GROUP BY sub1_customer_state
          ORDER BY sub1_avg_review_score DESC
          ) sub1
LEFT JOIN (
           SELECT customer_state AS sub2_customer_state, AVG(review_score) AS sub2_avg_review_score
               FROM (
                     SELECT *  
					     FROM (
                               SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
                               order_delivered_carrier_date,
                               LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                               order_estimated_delivery_date, review_score
                               FROM ecommerce.orders
							   LEFT JOIN order_reviews
                               ON orders.order_id = order_reviews.order_id
                               LEFT JOIN customers
                               ON orders.customer_id = customers.customer_id
                               ) sub1
					 WHERE order_delivered_customer_date < order_estimated_delivery_date
                     ORDER BY review_score DESC
                     ) sub2
           GROUP BY sub2_customer_state
           ORDER BY sub2_avg_review_score DESC
           ) sub2
ON sub1.sub1_customer_state = sub2.sub2_customer_state
LEFT JOIN (
           SELECT customer_state AS sub3_customer_state, AVG(review_score) AS sub3_avg_review_score
               FROM (
                     SELECT *  
						 FROM (
                               SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
					           order_delivered_carrier_date,
                               LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,    
                               order_estimated_delivery_date, review_score
                               FROM ecommerce.orders
                               LEFT JOIN order_reviews
                               ON orders.order_id = order_reviews.order_id
                               LEFT JOIN customers
                               ON orders.customer_id = customers.customer_id
                               ) sub1
					 WHERE order_delivered_customer_date = order_estimated_delivery_date
					 ORDER BY review_score DESC
                     ) sub1
           GROUP BY sub3_customer_state
           ORDER BY sub3_avg_review_score DESC
           ) sub3
ON sub2.sub2_customer_state = sub3.sub3_customer_state;
/* average review score when dilivery time is greater than estimated dilivery time is 2.2
average review score when dilivery time is less than estimated dilivery time 4.2 
average review score when dilivery time is equal to estimated dilivery time 4.0*/

SELECT (SELECT SUM(total_orders) 
FROM (SELECT customer_state, SUM(price) AS sum_price, SUM(freight_value) AS sum_freight_value,
COUNT(order_id) AS total_orders
FROM (
      SELECT order_purchase_timestamp, price, freight_value,
      customer_zip_code_prefix, customer_state, orders.order_id 
      FROM ecommerce.orders
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      LEFT JOIN customers 
      ON orders.customer_id = customers.customer_id
      ) sub
GROUP BY customer_state
ORDER BY total_orders DESC
LIMIT 2) sub2)/
(SELECT SUM(total_orders) 
FROM (SELECT customer_state, SUM(price) AS sum_price, SUM(freight_value) AS sum_freight_value,
COUNT(order_id) AS total_orders
FROM (
      SELECT order_purchase_timestamp, price, freight_value, customer_state, orders.order_id 
      FROM ecommerce.orders
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      LEFT JOIN customers 
      ON orders.customer_id = customers.customer_id
      ) sub
GROUP BY customer_state
ORDER BY total_orders DESC
) sub2)*100 AS percent; -- top 2 states SP and RJ by total sales together produce 55 percent of the orders

SELECT year, ROUND(sales) AS sales, order_count, ROUND(aov) AS aov, COALESCE(ROUND(sales_growth), '') AS sales_growth, 
COALESCE(ROUND(order_count_growth), '') AS order_count_growth, TRIM('.' FROM LEFT(COALESCE(aov_growth, ''), 4)) AS aov_growth 
FROM (
SELECT year, sales, order_count, aov, 
sale_difference/LAG(sales) OVER(ORDER BY year)*100 AS sales_growth,
order_count_difference/LAG(order_count) OVER(ORDER BY year)*100 AS order_count_growth,
aov_difference/LAG(aov) OVER(ORDER BY year)*100 AS aov_growth 
 FROM (
SELECT *
    FROM (
          SELECT *, sales/order_count AS aov 
              FROM (
                    SELECT LEFT(order_purchase_timestamp, 4) AS year, SUM(price) AS sales,
                    COUNT(orders.order_id) AS order_count
                    FROM ecommerce.order_items
                    LEFT JOIN ecommerce.orders
                    ON order_items.order_id = orders.order_id
                    GROUP BY year
                    ORDER BY year 
                    ) sub1
          ) sub2 LEFT JOIN
(SELECT *, sub4_sale - LAG(sub4_sale) OVER(ORDER BY sub4_year) AS sale_difference,
sub4_order_count - LAG(sub4_order_count) OVER(ORDER BY sub4_year) AS order_count_difference,
sub4_aov - LAG(sub4_aov) OVER(order by sub4_year) AS aov_difference
    FROM (
          SELECT *, sub4_sale/sub4_order_count AS sub4_aov 
              FROM (
                    SELECT LEFT(order_purchase_timestamp, 4) AS sub4_year, SUM(price) AS sub4_sale,
                    COUNT(orders.order_id) AS sub4_order_count
                    FROM ecommerce.order_items
                    LEFT JOIN ecommerce.orders
                    ON order_items.order_id = orders.order_id
                    GROUP BY sub4_year
                    ORDER BY sub4_year 
                    ) sub1
          ) sub2) sub4
ON sub2.year = sub4.sub4_year) sub5) sub6; -- AOV, sales growth, aov growth, order count growth per year

SELECT *, DENSE_RANK() OVER (PARTITION BY customer_state ORDER BY avg_review_score DESC) AS review_rank 
    FROM (
          SELECT year, month, customer_state, AVG(review_score) AS avg_review_score
		      FROM(
                   SELECT LEFT(order_purchase_timestamp, 4) AS year,
                   SUBSTR(order_purchase_timestamp, 6, 2) AS month,
                   SUBSTR(order_purchase_timestamp, 9, 2) AS day, customer_state, review_score
                   FROM ecommerce.orders
                   LEFT JOIN order_reviews
				   ON orders.order_id = order_reviews.order_id
                   LEFT JOIN customers     
                   ON orders.customer_id = customers.customer_id
                   ) sub1
		  GROUP BY year, month, customer_state
          ORDER BY customer_state, year, month
          ) sub2
; -- avg review per state per day with review score and dense rank

SELECT AVG(review_score) AS avg_score_per_state, customer_state 
    FROM (
          SELECT order_purchase_timestamp, review_score, customer_state
          FROM ecommerce.order_reviews
          LEFT JOIN orders
          ON order_reviews.order_id = orders.order_id
          LEFT JOIN customers
          ON orders.customer_id = customers.customer_id
          LEFT JOIN order_items 
          ON orders.order_id = order_items.order_id
          LEFT JOIN products 
          ON order_items.product_id = products.product_id
          )sub1
GROUP BY customer_state
ORDER BY avg_score_per_state DESC;
-- avg review score per state

SELECT product_category_name_english, total_revenue, ROUND(total_revenue_percent) AS "total_revenue(%)",
ROUND(AOV) AS AOV, order_count, ROUND(order_count_percent) AS "order_count(%)" FROM (
SELECT product_category_name_english, total_revenue, total_revenue/13591643.70*100 AS total_revenue_percent,
total_revenue/order_count AS AOV, order_count, order_count/99441*100 AS order_count_percent
    FROM (
          SELECT product_category_name_english, SUM(price) AS total_revenue, COUNT(*) AS order_count
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN product_category_name
          ON products.product_category_name = product_category_name.product_category_name
          GROUP BY product_category_name_english
          ORDER BY total_revenue DESC
          LIMIT 8
          ) sub1) sub2; -- top 8 categories with total revenue, AOV, order count

SELECT *, ROUND((repeat_customer/unique_customer)*100, 1) AS repeat_rate
    FROM (
          SELECT year, unique_customer, total_customer - unique_customer AS repeat_customer 
              FROM (
                    SELECT year, COUNT(customer_unique_id) AS total_customer,
                    COUNT(DISTINCT(customer_unique_id)) AS unique_customer  
                        FROM (
                              SELECT LEFT(order_purchase_timestamp, 4) AS year, customer_unique_id  
	                          from ecommerce.orders
							  LEFT JOIN ecommerce.customers
                              ON orders.customer_id = customers.customer_id
                              ) sub1
                    GROUP BY year
                    ) sub2
          ) sub2; -- total repeat purchases and unique customers in each year with repeat rate

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------

rest of code for exploratory analysis

------ orders ------------
SELECT * FROM ecommerce.orders;

SELECT max(order_purchase_timestamp), MIN(order_purchase_timestamp) FROM ecommerce.orders;

SELECT TIMESTAMPDIFF(SECOND, '2016-09-04 21:15:19', '2018-10-17 17:30:18') FROM ecommerce.orders;
-- gives time difference of 66773699 seconds which comes to 772 days 20 hours 14 minutes 59 seconds

SELECT TIMESTAMPADD(DAY, 386, '2016-09-04 21:15:19'); -- adds to 2017-09-25 21:15:19
SELECT TIMESTAMPADD(HOUR, 10, '2017-09-25 21:15:19'); -- adds to 2017-09-26 07:15:19
SELECT TIMESTAMPADD(MINUTE, 7, '2017-09-26 07:15:19'); -- adds to 2017-09-26 07:22:19
SELECT TIMESTAMPADD(SECOND, 29.5, '2017-09-26 07:22:19'); -- adds to 2017-09-26 07:22:48.5
-- half way point of ecommerce dataset

SELECT COUNT(*) FROM ecommerce.orders
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'; -- total orders from first year of ecommerce dataset 26891 orders

SELECT COUNT(*) FROM ecommerce.orders
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'; -- total orders from second year of ecommerce dataset 72550 orders
 
SELECT (SELECT COUNT(*) FROM ecommerce.orders WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5')
/(SELECT COUNT(*) FROM ecommerce.orders WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5')*100 AS percentage_increase;
-- to find percentage increase in total orders by end of second year 269%

SELECT year, sales, order_count, ROUND(aov) AS aov, COALESCE(ROUND(sales_growth), '') AS sales_growth, 
COALESCE(ROUND(order_count_growth), '') AS order_count_growth, TRIM('.' FROM LEFT(COALESCE(aov_growth, ''), 4)) AS aov_growth 
FROM (
SELECT year, sales, order_count, aov, 
sale_difference/LAG(sales) OVER(ORDER BY year)*100 AS sales_growth,
order_count_difference/LAG(order_count) OVER(ORDER BY year)*100 AS order_count_growth,
aov_difference/LAG(aov) OVER(ORDER BY year)*100 AS aov_growth 
 FROM (
SELECT *
    FROM (
          SELECT *, sales/order_count AS aov 
              FROM (
                    SELECT LEFT(order_purchase_timestamp, 4) AS year, SUM(price) AS sales,
                    COUNT(orders.order_id) AS order_count
                    FROM ecommerce.order_items
                    LEFT JOIN ecommerce.orders
                    ON order_items.order_id = orders.order_id
                    GROUP BY year
                    ORDER BY year 
                    ) sub1
          ) sub2 LEFT JOIN
(SELECT *, sub4_sale - LAG(sub4_sale) OVER(ORDER BY sub4_year) AS sale_difference,
sub4_order_count - LAG(sub4_order_count) OVER(ORDER BY sub4_year) AS order_count_difference,
sub4_aov - LAG(sub4_aov) OVER(order by sub4_year) AS aov_difference
    FROM (
          SELECT *, sub4_sale/sub4_order_count AS sub4_aov 
              FROM (
                    SELECT LEFT(order_purchase_timestamp, 4) AS sub4_year, SUM(price) AS sub4_sale,
                    COUNT(orders.order_id) AS sub4_order_count
                    FROM ecommerce.order_items
                    LEFT JOIN ecommerce.orders
                    ON order_items.order_id = orders.order_id
                    GROUP BY sub4_year
                    ORDER BY sub4_year 
                    ) sub1
          ) sub2) sub4
ON sub2.year = sub4.sub4_year) sub5) sub6; -- AOV, sales growth, aov growth, order count growth per year

SELECT year, month, percent  
FROM (                    
SELECT year, month, ROUND(percent, 0) AS percent 
FROM (SELECT year, month, sale_difference/LAG(sales) OVER(ORDER BY year)*100 AS percent 
FROM (
SELECT * FROM (SELECT *
FROM (SELECT year AS sub2_year, month AS sub2_month, sales - LAG(sales) OVER(ORDER BY year) AS sale_difference
FROM (
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
SUM(price) AS sales
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year 
LIMIT 23) sub1 ) sub2
                                       ) sub3
LEFT JOIN (
SELECT * FROM (
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
SUM(price) AS sales
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year 
LIMIT 23) sub1) sub4
ON sub3.sub2_year = sub4.year
AND sub3.sub2_month = sub4.month
                        ) sub5
                   ) sub6
          ) sub7
; -- sales growth per month in each year

SELECT year, month, percent FROM (                    
SELECT year, month, ROUND(percent, 0) AS percent FROM (
SELECT year, month, order_count_diff/LAG(order_count) OVER(ORDER BY year)*100 AS percent FROM (SELECT * FROM (SELECT *
FROM (SELECT year AS sub2_year, month AS sub2_month, order_count - LAG(order_count) OVER(ORDER BY year) AS order_count_diff
FROM (
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
COUNT(orders.order_id) AS order_count
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year 
LIMIT 23) sub1 ) sub2) sub3
LEFT JOIN (
SELECT * FROM (
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
COUNT(orders.order_id) AS order_count
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year 
LIMIT 23) sub1) sub4
ON sub3.sub2_year = sub4.year
AND sub3.sub2_month = sub4.month) sub5) sub6) sub7
; -- order count growth per month in each year

SELECT year, month, ROUND(percent) AS percent FROM(
SELECT year, month, aov_diff/LAG(aov) OVER(ORDER BY year)*100 AS percent FROM (
SELECT year, month, aov, aov - LAG(sub3_aov) OVER(ORDER BY year) AS aov_diff FROM (
SELECT sub1.year, sub1.month, sales/order_count AS aov, sub3_aov FROM (
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
COUNT(orders.order_id) AS order_count, SUM(price) AS sales
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year 
LIMIT 23) sub1 LEFT JOIN (SELECT year, month, sales/order_count AS sub3_aov FROM (
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
COUNT(orders.order_id) AS order_count, SUM(price) AS sales
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year 
LIMIT 23)sub2 ) sub3
ON sub1.year = sub3.year
AND sub1.month = sub3.month) sub4) sub5) sub6; -- aov per month in each year

SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders 
FROM ecommerce.orders
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY month
ORDER BY month; -- orders per month in first half

SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders 
FROM ecommerce.orders
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY month
ORDER BY month; -- orders per month in second half

SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(orders.order_id) AS orders
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY year, month
ORDER BY year, month; -- orders per month with year for entire dataset

SELECT * FROM (SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
SUM(price) AS sum_price, customer_state, COUNT(order_id) AS orders
FROM (SELECT order_purchase_timestamp, price,
customer_state, orders.order_id, product_category_name 
FROM ecommerce.orders
LEFT JOIN order_items
ON orders.order_id = order_items.order_id
LEFT JOIN customers 
ON orders.customer_id = customers.customer_id
LEFT JOIN products
ON order_items.product_id = products.product_id) sub1
GROUP BY customer_state, year, month
ORDER BY customer_state, year, month) sub; -- orders per state per year per month with sum

SELECT sub.year, 
	   AVG(count) AS avg_per_month
    FROM (
          SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
		  COUNT(*) AS count 
		  FROM ecommerce.orders 
		  WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5' 
		  GROUP BY month, year
		  ORDER BY year, month
          ) sub 
GROUP BY sub.year; -- average orders for first half by each year 2016 has 109 and 2017 has 2951

SELECT sub.year, 
       AVG(count) AS avg_per_month
    FROM (
          SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month, 
          COUNT(*) AS count 
          FROM ecommerce.orders WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5' 
          GROUP BY month, year
          ORDER BY year, month
		  ) sub 
GROUP BY sub.year; -- average orders for second half by each year 2017 has 4624 and 2018 has 5401

SELECT AVG(sub.count) AS avg_per_month
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS count 
          FROM ecommerce.orders
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
	      GROUP BY month
          ORDER BY month
          ) sub; -- average orders per month for first half of dataset 2444

SELECT AVG(sub.count) AS avg_per_month
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS count 
          FROM ecommerce.orders
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY month
          ORDER BY month
          ) sub; -- average orders per month for second half of dataset 6045
 
SELECT DISTINCT (SELECT AVG(sub.count) AS avg_per_month
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS count 
          FROM ecommerce.orders
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
		  GROUP BY month
		  ORDER BY month
          ) sub)/
       (SELECT AVG(sub.count) AS avg_per_month
	FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS count 
          FROM ecommerce.orders
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY month
          ORDER BY month
          ) sub)*100 AS percentage_increase; -- percentage increase for average orders per month from first half to second 247%

SELECT AVG(sub.count) AS avg_per_month
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS count 
          FROM ecommerce.orders
          GROUP BY month
          ORDER BY month
          ) sub; -- average orders per month for entire dataset 8286
          
SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month, SUM(price)
FROM ecommerce.orders 
LEFT JOIN order_items
ON orders.order_id = order_items.order_id
GROUP BY month, year
ORDER BY year, month; -- shows monthly revenue beginning to stabilize at 2018 January

SELECT AVG(total_price) FROM (SELECT * FROM (SELECT LEFT(order_purchase_timestamp, 4) AS year,
SUBSTR(order_purchase_timestamp, 6, 2) AS month, SUM(price) AS total_price
FROM ecommerce.orders 
LEFT JOIN order_items
ON orders.order_id = order_items.order_id
GROUP BY month, year
ORDER BY year, month) sub
WHERE year >= 2018 AND month >= 01
LIMIT 8) sub; -- average price for when it starts to stabilize is 923238.225

          
-- join customers table --------

SELECT * from ecommerce.orders
LEFT JOIN ecommerce.customers
ON orders.customer_id = customers.customer_id;

SELECT (SELECT SUM(sub.total_orders) 
    FROM (
          SELECT COUNT(DISTINCT(order_id)) AS total_orders, product_category_name
	      FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          GROUP BY product_category_name
          ORDER BY total_orders DESC
          LIMIT 4
          ) sub
          ) /
(SELECT COUNT(DISTINCT(orders.order_id))
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent; -- top 4 products produce 33% of total orders

SELECT * 
    FROM (
          SELECT *, (orders/112650)*100 AS percent_of_orders, (sum_price/13591643.70)*100 AS percent_of_revenue
		      FROM (
                    SELECT product_category_name, COUNT(orders.order_id) AS orders, SUM(price) AS sum_price
                    FROM ecommerce.order_items
					LEFT JOIN ecommerce.products
                    ON order_items.product_id = products.product_id
                    LEFT JOIN orders
                    ON order_items.order_id = orders.order_id
                    WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
                    GROUP BY product_category_name
                    ORDER BY orders DESC
					) SUB1
) sub2; -- percent of orders and revenues of total per category with sum

SELECT product_category_name_english, total_revenue, ROUND(total_revenue_percent) AS "total_revenue(%)",
ROUND(AOV) AS AOV, order_count, ROUND(order_count_percent) AS "order_count(%)" FROM (
SELECT product_category_name_english, total_revenue, total_revenue/13591643.70*100 AS total_revenue_percent,
total_revenue/order_count AS AOV, order_count, order_count/99441*100 AS order_count_percent
    FROM (
          SELECT product_category_name_english, SUM(price) AS total_revenue, COUNT(*) AS order_count
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN product_category_name
          ON products.product_category_name = product_category_name.product_category_name
          GROUP BY product_category_name_english
          ORDER BY total_revenue DESC
          LIMIT 8
          ) sub1) sub2; -- top 8 categories with total revenue, AOV, order count

------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------ order_items ---------------------------

SELECT * FROM ecommerce.order_items;

SELECT * FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id; -- join tables

SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id; -- total revenue for entire dataset 13,591,643.70

SELECT LEFT(order_purchase_timestamp, 4) AS year, SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY year; -- total revenue per year for first half of dataset 2017 has 3,629,155.51 and 2016 has 49,785.92

SELECT LEFT(order_purchase_timestamp, 4) AS year, SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY year; -- total revenue per year for second half of dataset 2018 has 7,386,050.80 and 2017 has 2,526,651.47

SELECT SUM(price) AS total_revenue, product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
GROUP BY product_category_name
ORDER BY total_revenue DESC; -- total revenue by category for entire dataset

SELECT SUM(price) AS total_revenue, product_category_name FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY product_category_name
ORDER BY total_revenue DESC; -- total revenue for first half by category 

SELECT SUM(price) AS total_revenue, product_category_name 
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY product_category_name
ORDER BY total_revenue DESC; -- total revenue for second half by category

SELECT MAX(shipping_limit_date), MIN(shipping_limit_date) from order_items; -- max and min shipping limit dates 

SELECT * FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id; -- join tables

SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders, SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY month
ORDER BY month; -- total revenue per month with orders for entire dataset

SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders, SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY month
ORDER BY month; -- total revenue per month with orders for first half of dataset

SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders, SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY month
ORDER BY month; -- total revenue per month with orders for second half of dataset

SELECT AVG(price_per_month) 
	FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders,
          SUM(price) AS price_per_month
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          GROUP BY month
          ORDER BY month
          ) sub; -- average price per month for entire dataset 1,132,636.975

SELECT AVG(price_per_month) 
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders,
		  SUM(price) AS price_per_month
		  FROM ecommerce.order_items
		  LEFT JOIN ecommerce.products
		  ON order_items.product_id = products.product_id
		  LEFT JOIN orders
		  ON order_items.order_id = orders.order_id
		  WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
		  GROUP BY month
		  ORDER BY month
          ) sub; -- average revenue per month for first half of dataset 334,449.220909

SELECT AVG(price_per_month) 
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders,
          SUM(price) AS price_per_month
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY month
          ORDER BY month
          ) sub; -- average revenue per month for second half of dataset 826,058.522500

SELECT AVG(total_price)
FROM (
      SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month, SUM(price) AS total_price
	  FROM ecommerce.orders 
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      GROUP BY month, year
      ORDER BY year, month
      ) sub; -- average revenue per month for entire dataset by year 566,318.487500

SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, product_category_name, SUM(price) AS revenue
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY month, product_category_name
ORDER BY month, revenue DESC; -- revenue per product per month for entire dataset

SELECT LEFT(order_purchase_timestamp, 4) AS year, SUBSTR(order_purchase_timestamp, 6, 2) AS month,
product_category_name, SUM(price) AS revenue
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY year, month, product_category_name
ORDER BY year, month; -- revenue per product per month with year for entire dataset 

SELECT LEFT(order_purchase_timestamp, 4) AS year,
product_category_name, SUM(price) AS revenue
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY year, product_category_name
ORDER BY year; -- revenue per product per year for entire dataset
          
SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, product_category_name, SUM(price) AS revenue
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY month, product_category_name
ORDER BY month, revenue DESC; -- revenue per product per month for first half of dataset
          
SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, product_category_name, SUM(price) AS revenue
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY month, product_category_name
ORDER BY month, revenue DESC; -- revenue per product per month for second half of dataset 


SELECT AVG(orders) FROM (SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders, SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY month
ORDER BY month) sub1; -- from second half there was an average of 6874.3333 orders per month

SELECT AVG(sum_price) FROM (SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, COUNT(*) AS orders, SUM(price) AS sum_price
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY month
ORDER BY month) sub1; -- from second half there was an average of 826,058 revenue per month


-- top categories by revenue ------------------------------

SELECT COUNT(DISTINCT(product_category_name)) FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id; -- count distinct category (73)

SELECT SUM(price) AS total_revenue, product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 8; -- top 8 categories by total revenue for entire dataset

SELECT SUM(total_revenue) AS total,
SUM(CASE WHEN product_category_name = 'cama_mesa_banho' THEN total_revenue ELSE NULL END) AS cama_mesa_banho,
SUM(CASE WHEN product_category_name = 'beleza_saude' THEN total_revenue ELSE NULL END) AS beleza_saude,
SUM(CASE WHEN product_category_name = 'esporte_lazer' THEN total_revenue ELSE NULL END) AS esporte_lazer,
SUM(CASE WHEN product_category_name = 'relogios_presentes' THEN total_revenue ELSE NULL END) AS relogios_presentes,
SUM(CASE WHEN product_category_name = 'cool_stuff' THEN total_revenue ELSE NULL END) AS cool_stuff,
SUM(CASE WHEN product_category_name = 'informatica_acessorios' THEN total_revenue ELSE NULL END) AS informatica_acessorios,
SUM(CASE WHEN product_category_name = 'moveis_decoracao' THEN total_revenue ELSE NULL END) AS moveis_decoracao,
SUM(CASE WHEN product_category_name = 'ferramentas_jardim' THEN total_revenue ELSE NULL END) AS ferramentas_jardim
FROM (
      SELECT SUM(price) AS total_revenue, product_category_name
	  FROM ecommerce.order_items
      LEFT JOIN ecommerce.products
      ON order_items.product_id = products.product_id
      LEFT JOIN orders
      ON order_items.order_id = orders.order_id
      WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
      GROUP BY product_category_name
      ORDER BY total_revenue DESC
	  LIMIT 8
      ) sub; -- top 8 categories by total revenue for first half of dataset including sum of top 8

SELECT SUM(total_revenue) AS total,
SUM(CASE WHEN product_category_name = 'beleza_saude' THEN total_revenue ELSE NULL END) AS beleza_saude,
SUM(CASE WHEN product_category_name = 'relogios_presentes' THEN total_revenue ELSE NULL END) AS relogios_presentes,
SUM(CASE WHEN product_category_name = 'cama_mesa_banho' THEN total_revenue ELSE NULL END) AS cama_mesa_banho,
SUM(CASE WHEN product_category_name = 'esporte_lazer' THEN total_revenue ELSE NULL END) AS esporte_lazer,
SUM(CASE WHEN product_category_name = 'informatica_acessorios' THEN total_revenue ELSE NULL END) AS informatica_acessorios,
SUM(CASE WHEN product_category_name = 'moveis_decoracao' THEN total_revenue ELSE NULL END) AS moveis_decoracao,
SUM(CASE WHEN product_category_name = 'utilidades_domesticas' THEN total_revenue ELSE NULL END) AS utilidades_domesticas,
SUM(CASE WHEN product_category_name = 'automotivo' THEN total_revenue ELSE NULL END) AS automotivo
FROM (
      SELECT SUM(price) AS total_revenue, product_category_name
      FROM ecommerce.order_items
      LEFT JOIN ecommerce.products
      ON order_items.product_id = products.product_id
      LEFT JOIN orders
      ON order_items.order_id = orders.order_id
      WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
      GROUP BY product_category_name
      ORDER BY total_revenue DESC
      LIMIT 8
      ) sub; -- top 8 categories by total revenue for second half of dataset including sum of top 8

SELECT SUM(price) AS total_revenue, product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
GROUP BY product_category_name
ORDER BY total_revenue DESC
LIMIT 4; -- top 4 categories by total revenue for entire dataset

SELECT SUM(total_revenue) AS total,
SUM(CASE WHEN product_category_name = 'beleza_saude' THEN total_revenue ELSE NULL END) AS beleza_saude,
SUM(CASE WHEN product_category_name = 'relogios_presentes' THEN total_revenue ELSE NULL END) AS relogios_presentes,
SUM(CASE WHEN product_category_name = 'cama_mesa_banho' THEN total_revenue ELSE NULL END) AS cama_mesa_banho,
SUM(CASE WHEN product_category_name = 'esporte_lazer' THEN total_revenue ELSE NULL END) AS esporte_lazer
FROM (
      SELECT SUM(price) AS total_revenue, product_category_name
	  FROM ecommerce.order_items
      LEFT JOIN ecommerce.products
      ON order_items.product_id = products.product_id
      LEFT JOIN orders
      ON order_items.order_id = orders.order_id
      WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
      GROUP BY product_category_name
      ORDER BY total_revenue DESC
      LIMIT 4
      ) sub; -- top 4 categories by total revenue for first half of dataset including sum of top 4

SELECT SUM(total_revenue) AS total,
SUM(CASE WHEN product_category_name = 'beleza_saude' THEN total_revenue ELSE NULL END) AS beleza_saude,
SUM(CASE WHEN product_category_name = 'relogios_presentes' THEN total_revenue ELSE NULL END) AS relogios_presentes,
SUM(CASE WHEN product_category_name = 'cama_mesa_banho' THEN total_revenue ELSE NULL END) AS cama_mesa_banho,
SUM(CASE WHEN product_category_name = 'esporte_lazer' THEN total_revenue ELSE NULL END) AS esporte_lazer
FROM (
      SELECT SUM(price) AS total_revenue, product_category_name
	  FROM ecommerce.order_items
      LEFT JOIN ecommerce.products
      ON order_items.product_id = products.product_id
      LEFT JOIN orders
      ON order_items.order_id = orders.order_id
      WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
      GROUP BY product_category_name
      ORDER BY total_revenue DESC
      LIMIT 4
      ) sub; -- top 4 categories by total revenue for first half of dataset including sum of top 4

-- percentage by revenue -----------------

SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
		  LIMIT 8
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent;
 -- percentage of revenue produced by top 8 categories combined for entire dataset 54.430363%
 
SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
		  LIMIT 8
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent; 
-- percentage of revenue produced by the top 8 categories combined from first half of dataset 14.774245%

SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
		  LIMIT 8
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent; 
-- percentage of revenue produced by the top 8 categories combined from second half of dataset 40.230948%

SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
	      FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
          LIMIT 4
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent; 
-- percentage of revenue produced by the top 4 categories combined for entire dataset 33.025621%

SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
	      FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
          LIMIT 4
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent; 
-- percentage of revenue produced by the top 4 categories combined for first half of dataset 8.316979%

SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
	      FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
          LIMIT 4
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent; 
-- percentage of revenue produced by the top 4 categories combined for second half of dataset 24.708642%

SELECT (SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
	      FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
          LIMIT 4
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent)/
(SELECT (SELECT SUM(sub.total_revenue) 
    FROM (
          SELECT SUM(price) AS total_revenue, product_category_name
	      FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY product_category_name
          ORDER BY total_revenue DESC
          LIMIT 4
          ) sub
          ) /
(SELECT SUM(price)
FROM ecommerce.order_items
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id)*100 AS percent)*100 AS percentage_increase;
-- percentage increase in revenue for the top 4 categories from first half of dataset to second total at 297%

SELECT sub.product_category_name, AVG(revenue) AS avg_revenue, AVG(sub.orders) AS avg_orders
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, product_category_name, SUM(price) AS revenue, COUNT(*) AS orders
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
		  LEFT JOIN orders
          ON order_items.order_id = orders.order_id
		  GROUP BY month, product_category_name
          ORDER BY month, revenue DESC
          ) sub
GROUP BY product_category_name
ORDER BY avg_revenue DESC
LIMIT 4; -- average revenue and orders per month for the top 4 categories for entire dataset

SELECT sub.product_category_name, AVG(revenue) AS avg_revenue, AVG(sub.orders) AS avg_orders
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, product_category_name, SUM(price) AS revenue, COUNT(*) AS orders
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
		  LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
		  GROUP BY month, product_category_name
          ORDER BY month, revenue DESC
          ) sub
GROUP BY product_category_name
ORDER BY avg_revenue DESC
LIMIT 4; -- average revenue and orders per month for the top 4 catgeories for first half of dataset

SELECT sub.product_category_name, AVG(revenue) AS avg_revenue, AVG(sub.orders) AS avg_orders
    FROM (
          SELECT SUBSTR(order_purchase_timestamp, 6, 2) AS month, product_category_name, SUM(price) AS revenue, COUNT(*) AS orders
          FROM ecommerce.order_items
          LEFT JOIN ecommerce.products
          ON order_items.product_id = products.product_id
		  LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
		  GROUP BY month, product_category_name
          ORDER BY month, revenue DESC
          ) sub
GROUP BY product_category_name
ORDER BY avg_revenue DESC
LIMIT 4; -- average revenue and orders per month for the top 4 categories for second half of dataset

-------------------------------------------------------------------------------------------------------------------------------------------------------
----------- customers --------------
SELECT * FROM ecommerce.customers;

SELECT year, unique_customer, total_customer - unique_customer AS repeat_customer 
    FROM (
          SELECT year, COUNT(customer_unique_id) AS total_customer, COUNT(DISTINCT(customer_unique_id)) AS unique_customer  
              FROM (
                    SELECT LEFT(order_purchase_timestamp, 4) AS year, customer_unique_id  
                    from ecommerce.orders  
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id
		            ) sub1
          GROUP BY year
          ) sub2; -- total repeat purchases and unique customers in each year

SELECT *, ROUND((repeat_customer/unique_customer)*100, 1) AS repeat_rate
    FROM (
          SELECT year, unique_customer, total_customer - unique_customer AS repeat_customer 
              FROM (
                    SELECT year, COUNT(customer_unique_id) AS total_customer,
                    COUNT(DISTINCT(customer_unique_id)) AS unique_customer  
                        FROM (
                              SELECT LEFT(order_purchase_timestamp, 4) AS year, customer_unique_id  
	                          from ecommerce.orders
							  LEFT JOIN ecommerce.customers
                              ON orders.customer_id = customers.customer_id
                              ) sub1
                    GROUP BY year
                    ) sub2
          ) sub2; -- total repeat purchases and unique customers in each year with repeat rate
SELECT *, sub2.total_customer_unique_id - sub2.customer_unique_id AS repeat_purchases 
    FROM (
          SELECT COUNT(sub.customer_unique_id) AS total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS customer_unique_id, product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
					) sub
          GROUP BY product_category_name
          ) sub2; -- total repeat purchases for the top 4 categories by revenue for the entire dataset

SELECT *, sub2.total_customer_unique_id - sub2.customer_unique_id AS repeat_purchases 
    FROM (
          SELECT COUNT(sub.customer_unique_id) AS total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS customer_unique_id, product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
                    AND order_purchase_timestamp < '2017-09-26 07:22:48.5'
					) sub
          GROUP BY product_category_name
          ) sub2; -- total repeat purchases for the top 4 categories by revenue for first half of dataset

SELECT *, sub2.total_customer_unique_id - sub2.customer_unique_id AS repeat_purchases 
    FROM (
          SELECT COUNT(sub.customer_unique_id) AS total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS customer_unique_id, product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
                    AND order_purchase_timestamp > '2017-09-26 07:22:48.5'
					) sub
          GROUP BY product_category_name
          ) sub2; -- total repeat purchases for the top 4 categories by revenue for second half of dataset

SELECT one_product_category_name, one_repeat_purchases, two_repeat_purchases,
one_repeat_purchases/two_repeat_purchases*100 AS percentage_increase 
    FROM (SELECT *, sub2.one_total_customer_unique_id - sub2.one_customer_unique_id AS one_repeat_purchases 
		  FROM (
          SELECT COUNT(sub.customer_unique_id) AS one_total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS one_customer_unique_id, product_category_name AS one_product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
                    AND order_purchase_timestamp > '2017-09-26 07:22:48.5'
					) sub
          GROUP BY product_category_name
          ) sub2) sub3
LEFT JOIN (SELECT *, subA.two_total_customer_unique_id - subA.two_customer_unique_id AS two_repeat_purchases 
    FROM (
          SELECT COUNT(sub.customer_unique_id) AS two_total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS two_customer_unique_id, product_category_name AS two_product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
                    AND order_purchase_timestamp < '2017-09-26 07:22:48.5'
					) sub
          GROUP BY product_category_name
          ) subA) subC on
sub3.one_product_category_name = subC.two_product_category_name; 
/* percentage increase in repeat purchases for the top 4 categories by revenue for entire dataset
from the first to the second half of the dataset */

SELECT total_customer_unique_id, customer_unique_id, product_category_name, total_revenue, repeat_purchases,
DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS total_revenue_rank,
DENSE_RANK() OVER (ORDER BY repeat_purchases DESC) AS repeat_purchase_rank  
FROM (SELECT *, sub2.total_customer_unique_id - sub2.customer_unique_id AS repeat_purchases 
	FROM (
          SELECT COUNT(sub.customer_unique_id) AS total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS customer_unique_id, product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
					) sub
          GROUP BY product_category_name
          ) sub2
          LEFT JOIN 
(SELECT SUM(price) AS total_revenue, product_category_name AS two_product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
GROUP BY two_product_category_name) sub3
ON sub2.product_category_name = sub3.two_product_category_name) sub
ORDER BY total_revenue DESC; 
/* shows categories by revenue from entire dataset with total revenue and repeat purchase rank */

SELECT sub3.product_category_name,sub3.customer_unique_id/sub4.customer_unique_id*100 AS percentage_increase
FROM (SELECT customer_unique_id, product_category_name FROM 
   (SELECT *, sub2.total_customer_unique_id - sub2.customer_unique_id AS repeat_purchases 
    FROM (
          SELECT COUNT(sub.customer_unique_id) AS total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS customer_unique_id, product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
                    AND order_purchase_timestamp > '2017-09-26 07:22:48.5'
					) sub
          GROUP BY product_category_name
          ) sub2) subA) sub3
          LEFT JOIN
(SELECT customer_unique_id, product_category_name 
FROM (SELECT *, sub3.total_customer_unique_id - sub3.customer_unique_id AS repeat_purchases 
    FROM (
          SELECT COUNT(sub.customer_unique_id) AS total_customer_unique_id, 
          COUNT(DISTINCT(sub.customer_unique_id)) AS customer_unique_id, product_category_name
              FROM (
                    SELECT order_purchase_timestamp, customer_unique_id, product_category_name 
                    from ecommerce.orders
                    LEFT JOIN ecommerce.customers
                    ON orders.customer_id = customers.customer_id 
                    LEFT JOIN order_items
                    ON order_items.order_id = orders.order_id
                    LEFT JOIN products
                    ON order_items.product_id = products.product_id  
                    WHERE product_category_name IN ('beleza_saude', 'cama_mesa_banho', 'esporte_lazer', 'relogios_presentes')
                    AND order_purchase_timestamp < '2017-09-26 07:22:48.5'
					) sub
          GROUP BY product_category_name
          ) sub3) subB) sub4 ON
sub3.product_category_name = sub4.product_category_name;
-- percentage increase for top 4 categories for unique sales from first to second half of dataset

SELECT seller_city, seller_state, SUM(price) AS sum_price,
SUM(freight_value) AS sum_freight_value, COUNT(order_id) AS total_orders 
FROM(
     SELECT seller_city, seller_state, order_id, order_item_count, price, freight_value 
     FROM ecommerce.sellers  
     LEFT JOIN order_items
     ON sellers.seller_id = order_items.seller_id
     ) sub
GROUP BY seller_city, seller_state
ORDER BY sum_price DESC; -- shows what seller had the most orders with their state and sum of revenue per seller city/state

SELECT customer_city, customer_state, SUM(price) AS sum_price, SUM(freight_value) AS sum_freight_value,
COUNT(DISTINCT(order_id)) AS distinct_orders
FROM (
      SELECT order_purchase_timestamp, price, freight_value,
      customer_zip_code_prefix, customer_city, customer_state, orders.order_id 
      FROM ecommerce.orders
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      LEFT JOIN customers 
      ON orders.customer_id = customers.customer_id
      ) sub
GROUP BY customer_state, customer_city
ORDER BY distinct_orders DESC; -- shows what city/state had the most orders with sum of revenue per city

SELECT (SELECT SUM(total_orders) 
FROM (SELECT customer_state, SUM(price) AS sum_price, SUM(freight_value) AS sum_freight_value,
COUNT(order_id) AS total_orders
FROM (
      SELECT order_purchase_timestamp, price, freight_value,
      customer_zip_code_prefix, customer_state, orders.order_id 
      FROM ecommerce.orders
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      LEFT JOIN customers 
      ON orders.customer_id = customers.customer_id
      ) sub
GROUP BY customer_state
ORDER BY total_orders DESC
LIMIT 2) sub2)/
(SELECT SUM(total_orders) 
FROM (SELECT customer_state, SUM(price) AS sum_price, SUM(freight_value) AS sum_freight_value,
COUNT(order_id) AS total_orders
FROM (
      SELECT order_purchase_timestamp, price, freight_value, customer_state, orders.order_id 
      FROM ecommerce.orders
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      LEFT JOIN customers 
      ON orders.customer_id = customers.customer_id
      ) sub
GROUP BY customer_state
ORDER BY total_orders DESC
) sub2)*100 AS percent; -- top 2 states SP and RJ by total sales together produce 55 percent of the orders

SELECT *, total_orders/112650*100 AS percent_per_city 
FROM (SELECT customer_state, SUM(price) AS sum_price, SUM(freight_value) AS sum_freight_value,
COUNT(order_id) AS total_orders
FROM (
      SELECT order_purchase_timestamp, price, freight_value,
      customer_zip_code_prefix, customer_state, orders.order_id 
      FROM ecommerce.orders
      LEFT JOIN order_items
      ON orders.order_id = order_items.order_id
      LEFT JOIN customers 
      ON orders.customer_id = customers.customer_id
      ) sub
GROUP BY customer_state
ORDER BY total_orders DESC) sub; -- percentage of orders produced per state compared to total orders

SELECT *, sub1_orders/sub2_orders*100 AS increase_in_orders
    FROM (
          SELECT * FROM (SELECT customer_state AS sub1_customer_state,
          SUM(price) AS sub1_sum_price, COUNT(orders.order_id) AS sub1_orders 
          FROM ecommerce.orders
          LEFT JOIN order_items
          ON orders.order_id = order_items.order_id
          LEFT JOIN customers   
          ON orders.customer_id = customers.customer_id 
          LEFT JOIN products
          ON order_items.product_id = products.product_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY sub1_customer_state
          ORDER BY sub1_orders DESC
          ) sub1
LEFT JOIN (
           SELECT customer_state AS sub2_customer_state,
           SUM(price) AS sub2_sum_price, COUNT(orders.order_id) AS sub2_orders 
		   FROM ecommerce.orders
           LEFT JOIN order_items
           ON orders.order_id = order_items.order_id
           LEFT JOIN customers 
           ON orders.customer_id = customers.customer_id
           LEFT JOIN products
           ON order_items.product_id = products.product_id
           WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
           GROUP BY sub2_customer_state
           ORDER BY sub2_orders DESC
           ) sub2 
ON sub1.sub1_customer_state = sub2.sub2_customer_state) sub3;
-- increase in orders per state from first to second part of dataset

SELECT *, sub1_orders/sub2_orders*100 AS increase_in_orders
	FROM (
          SELECT * FROM (SELECT customer_state AS sub1_customer_state,
          SUM(price) AS sub1_sum_price, COUNT(orders.order_id) AS sub1_orders 
          FROM ecommerce.orders
          LEFT JOIN order_items
          ON orders.order_id = order_items.order_id    
          LEFT JOIN customers 
          ON orders.customer_id = customers.customer_id
          LEFT JOIN products
          ON order_items.product_id = products.product_id
		  WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY sub1_customer_state
          ORDER BY sub1_orders DESC
          ) sub1
LEFT JOIN (
           SELECT customer_state AS sub2_customer_state,
           SUM(price) AS sub2_sum_price, COUNT(orders.order_id) AS sub2_orders    
           FROM ecommerce.orders
           LEFT JOIN order_items
           ON orders.order_id = order_items.order_id
           LEFT JOIN customers 
           ON orders.customer_id = customers.customer_id
           LEFT JOIN products
		   ON order_items.product_id = products.product_id
		   WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5' 
           AND order_purchase_timestamp < '2018-01-01 07:22:48.5'
           GROUP BY sub2_customer_state
           ORDER BY sub2_orders DESC
           ) sub2 
ON sub1.sub1_customer_state = sub2.sub2_customer_state) sub3;
-- increase in orders per state when sales started to stabilize 

-----------------------------------------------------------------------------------------------------------------------------------------------------------
--------------- order reviews ---------------
SELECT * FROM ecommerce.order_reviews;

SELECT *, COUNT(*) AS review_count 
    FROM (
          SELECT review_score, customer_state FROM ecommerce.order_reviews
          LEFT JOIN orders
          ON order_reviews.order_id = orders.order_id
          LEFT JOIN customers
          ON orders.customer_id = customers.customer_id
          LEFT JOIN order_items     
          ON orders.order_id = order_items.order_id
          WHERE review_score <= '5'
          ) sub
GROUP BY review_score, customer_state
ORDER BY customer_state DESC, review_score DESC; -- shows the count of each review score given per state per score

SELECT review_score, customer_state, COUNT(review_score) AS review_count, product_category_name 
FROM ecommerce.order_reviews
LEFT JOIN orders
ON order_reviews.order_id = orders.order_id
LEFT JOIN customers
ON orders.customer_id = customers.customer_id
LEFT JOIN order_items 
ON orders.order_id = order_items.order_id
LEFT JOIN products
ON order_items.product_id = products.product_id
WHERE review_score <= '5'
GROUP BY review_score, customer_state, product_category_name
ORDER BY customer_state DESC, review_score DESC; 
-- shows the count of each review score given per state per score per category

SELECT review_score, customer_state, MAX(review_count) AS max_review_count
    FROM (
          SELECT review_score, customer_state, COUNT(review_score) AS review_count, product_category_name FROM ecommerce.order_reviews
          LEFT JOIN orders
          ON order_reviews.order_id = orders.order_id
          LEFT JOIN customers
          ON orders.customer_id = customers.customer_id     
          LEFT JOIN order_items  
          ON orders.order_id = order_items.order_id
          LEFT JOIN products
          ON order_items.product_id = products.product_id
          WHERE review_score <= '5'
          GROUP BY review_score, customer_state, product_category_name
          ORDER BY customer_state DESC, review_score DESC
          ) sub
GROUP BY review_score, customer_state; 
-- shows the count of each review score given per state per score per category

SELECT *, DENSE_RANK() OVER (PARTITION BY customer_state ORDER BY avg_review_score DESC) AS review_rank 
    FROM (
          SELECT year, month, customer_state, AVG(review_score) AS avg_review_score
		      FROM(
                   SELECT LEFT(order_purchase_timestamp, 4) AS year,
                   SUBSTR(order_purchase_timestamp, 6, 2) AS month,
                   SUBSTR(order_purchase_timestamp, 9, 2) AS day, customer_state, review_score
                   FROM ecommerce.orders
                   LEFT JOIN order_reviews
				   ON orders.order_id = order_reviews.order_id
                   LEFT JOIN customers     
                   ON orders.customer_id = customers.customer_id
                   ) sub1
		  GROUP BY year, month, customer_state
          ORDER BY customer_state, year, month
          ) sub2
; -- avg review per state per day with review score and dense rank

SELECT AVG(review_score) AS avg_score_per_state, customer_state 
    FROM (
          SELECT order_purchase_timestamp, review_score, customer_state
          FROM ecommerce.order_reviews
          LEFT JOIN orders
          ON order_reviews.order_id = orders.order_id
          LEFT JOIN customers
          ON orders.customer_id = customers.customer_id
          LEFT JOIN order_items 
          ON orders.order_id = order_items.order_id
          LEFT JOIN products 
          ON order_items.product_id = products.product_id
          )sub1
GROUP BY customer_state
ORDER BY avg_score_per_state DESC;
-- avg review score per state

SELECT customer_state, AVG(review_score) AS avg_review_score 
    FROM (
          SELECT *  
              FROM (
                    SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
                    order_delivered_carrier_date,
                    LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                    order_estimated_delivery_date, review_score  
                    FROM ecommerce.orders
                    LEFT JOIN order_reviews
                    ON orders.order_id = order_reviews.order_id    
                    LEFT JOIN customers
                    ON orders.customer_id = customers.customer_id
                    ) sub1  
		  WHERE order_delivered_customer_date > order_estimated_delivery_date
		  ORDER BY review_score DESC
          ) sub2
GROUP BY customer_state
ORDER BY avg_review_score DESC; -- average review score per state when delivery time is greater than estimated time

SELECT customer_state, AVG(review_score) AS avg_review_score 
    FROM (
          SELECT *  
              FROM (
                    SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
                    order_delivered_carrier_date,
                    LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                    order_estimated_delivery_date, review_score
                    FROM ecommerce.orders
                    LEFT JOIN order_reviews
                    ON orders.order_id = order_reviews.order_id
                    LEFT JOIN customers
                    ON orders.customer_id = customers.customer_id
                    ) sub1
		  WHERE order_delivered_customer_date < order_estimated_delivery_date
          ORDER BY review_score DESC
          ) sub2
GROUP BY customer_state
ORDER BY avg_review_score DESC;-- average review score per state when delivery time is less than estimated time

SELECT customer_state, AVG(review_score) AS avg_review_score 
    FROM (
          SELECT *  
              FROM (
                    SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
                    order_delivered_carrier_date,
                    LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                    order_estimated_delivery_date, review_score
                    FROM ecommerce.orders
                    LEFT JOIN order_reviews
					ON orders.order_id = order_reviews.order_id
                    LEFT JOIN customers
                    ON orders.customer_id = customers.customer_id
                    ) sub1
          WHERE order_delivered_customer_date < order_estimated_delivery_date
          ORDER BY review_score DESC
          ) sub2
GROUP BY customer_state
ORDER BY avg_review_score DESC;-- average review score per state when delivery time is equal to estimated time

SELECT AVG(sub1_avg_review_score), AVG(sub2_avg_review_score), AVG(sub3_avg_review_score) 
    FROM (
          SELECT customer_state AS sub1_customer_state, AVG(review_score) AS sub1_avg_review_score
              FROM (
                    SELECT *  
				        FROM (
                              SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
					          order_delivered_carrier_date,
                              LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                              order_estimated_delivery_date, review_score
                              FROM ecommerce.orders
                              LEFT JOIN order_reviews
                              ON orders.order_id = order_reviews.order_id
                              LEFT JOIN customers
                              ON orders.customer_id = customers.customer_id
                              ) sub1
                    WHERE order_delivered_customer_date > order_estimated_delivery_date
                    ORDER BY review_score DESC
                    ) sub2
          GROUP BY sub1_customer_state
          ORDER BY sub1_avg_review_score DESC
          ) sub1
LEFT JOIN (
           SELECT customer_state AS sub2_customer_state, AVG(review_score) AS sub2_avg_review_score
               FROM (
                     SELECT *  
					     FROM (
                               SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
                               order_delivered_carrier_date,
                               LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,
                               order_estimated_delivery_date, review_score
                               FROM ecommerce.orders
							   LEFT JOIN order_reviews
                               ON orders.order_id = order_reviews.order_id
                               LEFT JOIN customers
                               ON orders.customer_id = customers.customer_id
                               ) sub1
					 WHERE order_delivered_customer_date < order_estimated_delivery_date
                     ORDER BY review_score DESC
                     ) sub2
           GROUP BY sub2_customer_state
           ORDER BY sub2_avg_review_score DESC
           ) sub2
ON sub1.sub1_customer_state = sub2.sub2_customer_state
LEFT JOIN (
           SELECT customer_state AS sub3_customer_state, AVG(review_score) AS sub3_avg_review_score
               FROM (
                     SELECT *  
						 FROM (
                               SELECT orders.order_id, order_status, order_purchase_timestamp, order_approved_at,
					           order_delivered_carrier_date,
                               LEFT(order_delivered_customer_date, 10) AS order_delivered_customer_date, customer_state,    
                               order_estimated_delivery_date, review_score
                               FROM ecommerce.orders
                               LEFT JOIN order_reviews
                               ON orders.order_id = order_reviews.order_id
                               LEFT JOIN customers
                               ON orders.customer_id = customers.customer_id
                               ) sub1
					 WHERE order_delivered_customer_date = order_estimated_delivery_date
					 ORDER BY review_score DESC
                     ) sub1
           GROUP BY sub3_customer_state
           ORDER BY sub3_avg_review_score DESC
           ) sub3
ON sub2.sub2_customer_state = sub3.sub3_customer_state;
/* average review score when delivery time is greater than estimated delivery time is 2.2
average review score when delivery time is less than estimated delivery time 4.2 
average review score when delivery time is equal to estimated delivery time 4.0*/

-----------------------------------------------------------------------------------------------------------------------------------------------------------
--------- products -----------
SELECT * FROM ecommerce.products;

SELECT product_category_name, COUNT(product_id) AS product_count
FROM ecommerce.products
GROUP BY product_category_name
ORDER BY product_count DESC; -- number of products per category

SELECT sub1_product_category_name, sub1_total_orders/sub2_total_orders*100 AS percent_increase 
 FROM (
       SELECT * 
           FROM (
                 SELECT COUNT(DISTINCT(orders.order_id)) AS sub1_total_orders,
                 product_category_name AS sub1_product_category_name
	             FROM ecommerce.order_items
                 LEFT JOIN ecommerce.products
				 ON order_items.product_id = products.product_id
                 LEFT JOIN ecommerce.orders
                 ON order_items.order_id = orders.order_id
                 WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
                 GROUP BY sub1_product_category_name
                 ORDER BY sub1_total_orders DESC
                 ) sub1
LEFT JOIN (
           SELECT COUNT(DISTINCT(orders.order_id)) AS sub2_total_orders,
           product_category_name AS sub2_product_category_name
		   FROM ecommerce.order_items
           LEFT JOIN ecommerce.products
           ON order_items.product_id = products.product_id
           LEFT JOIN ecommerce.orders
           ON order_items.order_id = orders.order_id
           WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
           GROUP BY sub2_product_category_name
           ORDER BY sub2_total_orders DESC
           )sub2 
ON sub1_product_category_name = sub2_product_category_name
WHERE sub1_total_orders < sub2_total_orders) sub2
ORDER BY percent_increase DESC; -- percent increase in orders for categories from first to second part of dataset

SELECT COUNT(DISTINCT(orders.order_id)) AS total_orders, product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
GROUP BY product_category_name
ORDER BY total_orders DESC; -- total orders per category for entire dataset
          
SELECT COUNT(DISTINCT(orders.order_id)) AS total_orders, product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY product_category_name
ORDER BY total_orders DESC; -- total orders per category for first half dataset
          
SELECT COUNT(DISTINCT(orders.order_id)) AS total_orders, product_category_name
FROM ecommerce.order_items
LEFT JOIN ecommerce.products
ON order_items.product_id = products.product_id
LEFT JOIN ecommerce.orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY product_category_name
ORDER BY total_orders DESC; -- total orders per category for second half dataset

SELECT COUNT(DISTINCT(customer_unique_id)), SUM(price), product_category_name 
    FROM (
          SELECT customers.customer_unique_id, order_purchase_timestamp, price, product_category_name
		  from ecommerce.orders
          LEFT JOIN ecommerce.customers
          ON orders.customer_id = customers.customer_id 
          LEFT JOIN order_items
          ON order_items.order_id = orders.order_id
          LEFT JOIN products
          ON order_items.product_id = products.product_id
          ) sub
GROUP BY product_category_name
ORDER BY 1 DESC; -- total unique orders per category for entire dataset

SELECT COUNT(DISTINCT(customer_unique_id)), SUM(price), product_category_name 
    FROM (
          SELECT customers.customer_unique_id, order_purchase_timestamp, price, product_category_name
		  from ecommerce.orders
          LEFT JOIN ecommerce.customers
          ON orders.customer_id = customers.customer_id 
          LEFT JOIN order_items
          ON order_items.order_id = orders.order_id
          LEFT JOIN products
          ON order_items.product_id = products.product_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          ) sub
GROUP BY product_category_name
ORDER BY 1 DESC; -- total unique orders per category for first half of dataset

SELECT COUNT(DISTINCT(customer_unique_id)), SUM(price), product_category_name 
    FROM (
          SELECT customers.customer_unique_id, order_purchase_timestamp, price, product_category_name
		  from ecommerce.orders
          LEFT JOIN ecommerce.customers
          ON orders.customer_id = customers.customer_id 
          LEFT JOIN order_items
          ON order_items.order_id = orders.order_id
          LEFT JOIN products
          ON order_items.product_id = products.product_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          ) sub
GROUP BY product_category_name
ORDER BY 1 DESC; -- total unique orders per category for second half of dataset

SELECT sub1_product_category_name, sub1_customer_unique_id/sub2_customer_unique_id*100 AS percentage_increase
FROM (SELECT COUNT(DISTINCT(customer_unique_id)) AS sub1_customer_unique_id,
      SUM(price) AS sub1_sum_price, product_category_name AS sub1_product_category_name
		  FROM (
                SELECT customers.customer_unique_id, order_purchase_timestamp, price, product_category_name
			    from ecommerce.orders
                LEFT JOIN ecommerce.customers
                ON orders.customer_id = customers.customer_id 
                LEFT JOIN order_items
                ON order_items.order_id = orders.order_id
                LEFT JOIN products
                ON order_items.product_id = products.product_id
                WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
                ) sub
          GROUP BY sub1_product_category_name
          ORDER BY 1 DESC) sub1 
LEFT JOIN
(SELECT COUNT(DISTINCT(customer_unique_id)) AS sub2_customer_unique_id,
SUM(price) AS sub2_sum_price, product_category_name AS sub2_product_category_name 
     FROM (
           SELECT customers.customer_unique_id, order_purchase_timestamp, price, product_category_name
		   from ecommerce.orders
           LEFT JOIN ecommerce.customers
           ON orders.customer_id = customers.customer_id 
           LEFT JOIN order_items
           ON order_items.order_id = orders.order_id
           LEFT JOIN products
           ON order_items.product_id = products.product_id
           WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
           ) sub
GROUP BY sub2_product_category_name
ORDER BY 1 DESC) sub2
ON sub1.sub1_product_category_name = sub2.sub2_product_category_name; 
-- increase of unique purchases per category from first to second part of dataset

SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
GROUP BY product_id, product_category_name
ORDER BY product_category_name DESC, orders_for_product DESC; 
-- total orders and sum revenue per product in each category for entire dataset

SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY product_id, product_category_name
ORDER BY product_category_name DESC, orders_for_product DESC; 
-- total orders and sum revenue per product in each category for first part dataset

SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
GROUP BY product_id, product_category_name
ORDER BY product_category_name DESC, orders_for_product DESC; 
-- total orders and sum revenue per product in each category for second part dataset

SELECT COUNT(product_id), product_category_name
    FROM (
          SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
          FROM ecommerce.products
          LEFT JOIN order_items
          ON products.product_id = order_items.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
          GROUP BY product_id, product_category_name
          ORDER BY product_category_name DESC, orders_for_product DESC
          ) sub
GROUP BY product_category_name
ORDER BY 1 DESC; 
/* total unique products in each category sold for the first part of dataset */

SELECT COUNT(product_id), product_category_name
    FROM (
          SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
          FROM ecommerce.products
          LEFT JOIN order_items
          ON products.product_id = order_items.product_id
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
          GROUP BY product_id, product_category_name
		  ORDER BY product_category_name DESC, orders_for_product DESC
          ) sub
GROUP BY product_category_name
ORDER BY 1 DESC; 
/* total unique products in each category sold for the second part of dataset */

SELECT sub1_product_category_name, sub2_unique_productS_sold - sub1_unique_productS_sold AS num_of_products_increase
FROM (SELECT COUNT(product_id) AS sub1_unique_productS_sold, product_category_name AS sub1_product_category_name
	      FROM (
                SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
                FROM ecommerce.products
                LEFT JOIN order_items
                ON products.product_id = order_items.product_id
                LEFT JOIN orders
                ON order_items.order_id = orders.order_id
                WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
                GROUP BY product_id, product_category_name
                ORDER BY product_category_name DESC, orders_for_product DESC
                ) sub
GROUP BY sub1_product_category_name
ORDER BY 1 DESC) sub1
LEFT JOIN (SELECT COUNT(product_id) AS sub2_unique_productS_sold, product_category_name AS sub2_product_category_name
		  FROM (
                SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
                FROM ecommerce.products
                LEFT JOIN order_items
                ON products.product_id = order_items.product_id
                LEFT JOIN orders
                ON order_items.order_id = orders.order_id
                WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
                GROUP BY product_id, product_category_name
                ORDER BY product_category_name DESC, orders_for_product DESC
                ) sub
GROUP BY sub2_product_category_name
ORDER BY 1 DESC) sub2
ON sub1.sub1_product_category_name = sub2.sub2_product_category_name
ORDER BY 2 DESC; -- shows the increase in number of unique products sold per category from first to second part of dataset

SELECT product_category_name, num_of_products_increase/product_count*100 AS percent_increase_products
    FROM (
          SELECT product_category_name, COUNT(product_id) AS product_count
          FROM ecommerce.products
          GROUP BY product_category_name
          ORDER BY product_count DESC
          ) sub1 LEFT JOIN
(SELECT sub1_product_category_name, sub2_unique_productS_sold - sub1_unique_productS_sold AS num_of_products_increase
   FROM (
         SELECT COUNT(product_id) AS sub1_unique_productS_sold, product_category_name AS sub1_product_category_name
		     FROM (
				   SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
                   FROM ecommerce.products
                   LEFT JOIN order_items
		           ON products.product_id = order_items.product_id
                   LEFT JOIN orders
                   ON order_items.order_id = orders.order_id
                   WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
                   GROUP BY product_id, product_category_name
                   ORDER BY product_category_name DESC, orders_for_product DESC
                   ) sub
		 GROUP BY sub1_product_category_name
		 ORDER BY 1 DESC
         ) sub1 LEFT JOIN 
    (SELECT COUNT(product_id) AS sub2_unique_productS_sold, product_category_name AS sub2_product_category_name
     FROM (
          SELECT products.product_id, product_category_name, SUM(price), COUNT(products.product_id) AS orders_for_product
          FROM ecommerce.products
          LEFT JOIN order_items
          ON products.product_id = order_items.product_id  
          LEFT JOIN orders
          ON order_items.order_id = orders.order_id
          WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'  
          GROUP BY product_id, product_category_name
          ORDER BY product_category_name DESC, orders_for_product DESC
          ) sub 
	 GROUP BY sub2_product_category_name
	 ORDER BY 1 DESC
	 ) sub2
ON sub1.sub1_product_category_name = sub2.sub2_product_category_name
ORDER BY 2 DESC
) sub3
ON sub1.product_category_name = sub3.sub1_product_category_name; 
-- percentage increase of unique products sold per category from the first to second part of dataset

SELECT SUM(sub1_sum_price) FROM (SELECT * FROM (SELECT * 
    FROM (
          SELECT products.product_id AS sub1_product_id, product_category_name AS sub1_product_category_name,
          SUM(price) AS sub1_sum_price, COUNT(products.product_id) AS sub1_orders_for_product
		  FROM ecommerce.products
		  LEFT JOIN order_items
		  ON products.product_id = order_items.product_id
		  LEFT JOIN orders
		  ON order_items.order_id = orders.order_id
		  WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
		  GROUP BY sub1_product_id, sub1_product_category_name
		  ORDER BY sub1_product_category_name DESC, sub1_orders_for_product DESC
          ) sub1 LEFT JOIN 
(SELECT products.product_id AS sub2_product_id, product_category_name AS sub2_product_category_name,
SUM(price) AS sub2_sum_price, COUNT(products.product_id) AS sub2_orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY sub2_product_id, sub2_product_category_name
ORDER BY sub2_product_category_name DESC, sub2_orders_for_product DESC
) sub2 
ON sub1.sub1_product_id = sub2.sub2_product_id) sub
WHERE sub2_product_id IS NULL) sub2; -- sum of revenue of expanded product lines 6,958,843.30

SELECT SUM(sub1_orders_for_product) FROM (SELECT * FROM (SELECT * 
    FROM (
          SELECT products.product_id AS sub1_product_id, product_category_name AS sub1_product_category_name,
          SUM(price) AS sub1_sum_price, COUNT(products.product_id) AS sub1_orders_for_product
		  FROM ecommerce.products
		  LEFT JOIN order_items
		  ON products.product_id = order_items.product_id
		  LEFT JOIN orders
		  ON order_items.order_id = orders.order_id
		  WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
		  GROUP BY sub1_product_id, sub1_product_category_name
		  ORDER BY sub1_product_category_name DESC, sub1_orders_for_product DESC
          ) sub1 LEFT JOIN 
(SELECT products.product_id AS sub2_product_id, product_category_name AS sub2_product_category_name,
SUM(price) AS sub2_sum_price, COUNT(products.product_id) AS sub2_orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY sub2_product_id, sub2_product_category_name
ORDER BY sub2_product_category_name DESC, sub2_orders_for_product DESC
) sub2 
ON sub1.sub1_product_id = sub2.sub2_product_id) sub
WHERE sub2_product_id IS NULL) sub2; -- sum of orders of expanded product lines 55,583

SELECT (SELECT SUM(sub1_orders_for_product) FROM (SELECT * FROM (SELECT * 
    FROM (
          SELECT products.product_id AS sub1_product_id, product_category_name AS sub1_product_category_name,
          SUM(price) AS sub1_sum_price, COUNT(products.product_id) AS sub1_orders_for_product
		  FROM ecommerce.products
		  LEFT JOIN order_items
		  ON products.product_id = order_items.product_id
		  LEFT JOIN orders
		  ON order_items.order_id = orders.order_id
		  WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
		  GROUP BY sub1_product_id, sub1_product_category_name
		  ORDER BY sub1_product_category_name DESC, sub1_orders_for_product DESC
          ) sub1 LEFT JOIN 
(SELECT products.product_id AS sub2_product_id, product_category_name AS sub2_product_category_name,
SUM(price) AS sub2_sum_price, COUNT(products.product_id) AS sub2_orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY sub2_product_id, sub2_product_category_name
ORDER BY sub2_product_category_name DESC, sub2_orders_for_product DESC
) sub2 
ON sub1.sub1_product_id = sub2.sub2_product_id) sub
WHERE sub2_product_id IS NULL) sub2)/
(SELECT COUNT(order_id) FROM ecommerce.orders)*100 AS percent_of_orders;
-- shows that the expanded products account for 55.8955 percent of total orders

SELECT (SELECT SUM(sub1_orders_for_product) FROM (SELECT * FROM (SELECT * 
    FROM (
          SELECT products.product_id AS sub1_product_id, product_category_name AS sub1_product_category_name,
          SUM(price) AS sub1_sum_price, COUNT(products.product_id) AS sub1_orders_for_product
		  FROM ecommerce.products
		  LEFT JOIN order_items
		  ON products.product_id = order_items.product_id
		  LEFT JOIN orders
		  ON order_items.order_id = orders.order_id
		  WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5'
		  GROUP BY sub1_product_id, sub1_product_category_name
		  ORDER BY sub1_product_category_name DESC, sub1_orders_for_product DESC
          ) sub1 LEFT JOIN 
(SELECT products.product_id AS sub2_product_id, product_category_name AS sub2_product_category_name,
SUM(price) AS sub2_sum_price, COUNT(products.product_id) AS sub2_orders_for_product
FROM ecommerce.products
LEFT JOIN order_items
ON products.product_id = order_items.product_id
LEFT JOIN orders
ON order_items.order_id = orders.order_id
WHERE order_purchase_timestamp < '2017-09-26 07:22:48.5'
GROUP BY sub2_product_id, sub2_product_category_name
ORDER BY sub2_product_category_name DESC, sub2_orders_for_product DESC
) sub2 
ON sub1.sub1_product_id = sub2.sub2_product_id) sub
WHERE sub2_product_id IS NULL) sub2)/
(SELECT COUNT(order_id) FROM ecommerce.orders
WHERE order_purchase_timestamp > '2017-09-26 07:22:48.5')*100 AS percent_of_orders;
-- shows that the expanded products account for 76.6134 percent of total orders in second part of dataset

SELECT product_id, sub1_product_category_name, (sum_price/sum_per_category)*100 AS percent_revenue_of_category,
(orders_for_product/sum_orders_per_category)*100 AS percent_orders_of_category
    FROM (
          SELECT *
              FROM (
                    SELECT products.product_id, product_category_name AS sub1_product_category_name, 
                    SUM(price) AS sum_price,
					COUNT(products.product_id) AS orders_for_product
                    FROM ecommerce.products
                    LEFT JOIN order_items
                    ON products.product_id = order_items.product_id
                    LEFT JOIN orders
                    ON order_items.order_id = orders.order_id
                    GROUP BY product_id, sub1_product_category_name
                    ORDER BY sub1_product_category_name DESC, orders_for_product DESC
                    ) sub1
LEFT JOIN (
           SELECT product_category_name, SUM(sum_price) AS sum_per_category, 
               SUM(orders_for_product) AS sum_orders_per_category
		       FROM (
                     SELECT products.product_id, product_category_name, SUM(price) AS sum_price,
                     COUNT(products.product_id) AS orders_for_product
                     FROM ecommerce.products
                     LEFT JOIN order_items
                     ON products.product_id = order_items.product_id
                     LEFT JOIN orders
                     ON order_items.order_id = orders.order_id
					 GROUP BY product_id, product_category_name
                     ORDER BY product_category_name DESC, orders_for_product DESC
					 ) sub1
           GROUP BY product_category_name
           ORDER BY product_category_name DESC
           ) sub2
          ON sub1.sub1_product_category_name = sub2.product_category_name
          ) sub3
ORDER BY sub1_product_category_name DESC, percent_revenue_of_category DESC; 
-- percent of orders and revenue produced by each product in each category


SELECT *, ROW_NUMBER() OVER (PARTITION BY sub1_product_category_name ORDER BY percent_revenue_of_category DESC) AS row_num
FROM (SELECT product_id, sub1_product_category_name, (sum_price/sum_per_category)*100 AS percent_revenue_of_category,
(orders_for_product/sum_orders_per_category)*100 AS percent_orders_of_category
    FROM (
          SELECT *
              FROM (
                    SELECT products.product_id, product_category_name AS sub1_product_category_name, 
                    SUM(price) AS sum_price,
					COUNT(products.product_id) AS orders_for_product
                    FROM ecommerce.products
                    LEFT JOIN order_items
                    ON products.product_id = order_items.product_id
                    LEFT JOIN orders
                    ON order_items.order_id = orders.order_id
                    GROUP BY product_id, sub1_product_category_name
                    ORDER BY sub1_product_category_name DESC, orders_for_product DESC
                    ) sub1
LEFT JOIN (
           SELECT product_category_name, SUM(sum_price) AS sum_per_category, 
               SUM(orders_for_product) AS sum_orders_per_category
		       FROM (
                     SELECT products.product_id, product_category_name, SUM(price) AS sum_price,
                     COUNT(products.product_id) AS orders_for_product
                     FROM ecommerce.products
                     LEFT JOIN order_items
                     ON products.product_id = order_items.product_id
                     LEFT JOIN orders
                     ON order_items.order_id = orders.order_id
					 GROUP BY product_id, product_category_name
                     ORDER BY product_category_name DESC, orders_for_product DESC
					 ) sub1
           GROUP BY product_category_name
           ORDER BY product_category_name DESC
           ) sub2
          ON sub1.sub1_product_category_name = sub2.product_category_name
          ) sub3
ORDER BY sub1_product_category_name DESC, percent_revenue_of_category DESC) sub4; 
-- percent of orders and revenue produced by each product in each category with row num
