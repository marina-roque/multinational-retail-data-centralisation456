SELECT CASE WHEN
    data_type = 'character varying' THEN MAX(LENGTH(column_name))
    ELSE NULL
    END AS max_length, column_name
FROM information_schema.columns
WHERE table_name = 'orders_table'
GROUP BY column_name, data_type;

ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID,
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
    ALTER COLUMN card_number TYPE VARCHAR (19),
    ALTER COLUMN store_code TYPE VARCHAR (12),
    ALTER COLUMN product_code TYPE VARCHAR (11),
    ALTER COLUMN product_quantity TYPE SMALLINT;
	
SELECT MAX(LENGTH('country_code')) AS max_length
FROM information_schema.columns
WHERE table_name = 'dim_users';
-- max length = 12

DELETE FROM dim_users
WHERE user_uuid !~* '^[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}$';
--There was values in the uuid column that didn't match with the uuid patern, so first i droped these values so i could alter the table later.

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255),
    ALTER COLUMN last_name TYPE VARCHAR(255),
    ALTER COLUMN country_code TYPE VARCHAR(12),
 	ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID
	ALTER COLUMN join_date TYPE DATE USING join_date::DATE,
	ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE;
	
--Merge latitude columns
ALTER TABLE dim_store_details
ADD COLUMN new_latitude FLOAT;

UPDATE dim_store_details
SET new_latitude = CASE WHEN
	latitude <> 'N/A' THEN latitude::FLOAT
     ELSE NULL
     END;

-- Drop the latitude and longitude columns and rename the new merged column to latitude
ALTER TABLE dim_store_details
DROP COLUMN latitude,
DROP COLUMN longitude
RENAME COLUMN new_latitude TO latitude;

SELECT 
    MAX(LENGTH(country_code)) AS max_country_code_length,
    MAX(LENGTH(store_code)) AS max_store_code_length
FROM dim_store_details;

ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255),
    ALTER COLUMN store_code TYPE VARCHAR(12),
    ALTER COLUMN staff_numbers TYPE SMALLINT,
    ALTER COLUMN opening_date TYPE DATE,
    ALTER COLUMN store_type TYPE VARCHAR(255),
    ALTER COLUMN latitude TYPE FLOAT,
    ALTER COLUMN country_code TYPE VARCHAR(2),
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Change 'N/A' values in location column to NULL
UPDATE dim_store_details
SET locality = NULL
WHERE locality = 'N/A';

-- Rename the columns to remove () from the column name
ALTER TABLE dim_products
RENAME COLUMN "product_price(£)" TO product_price;
RENAME COLUMN "weight(kg)" TO weight_in_kg;

SELECT MAX(LENGTH(CAST(weight_in_kg AS VARCHAR))) AS max_length
FROM dim_products;
-- Max length 5

-- Add a new weight_class column
ALTER TABLE dim_products
ADD COLUMN weight_class VARCHAR(15); -- Adjust the length accordingly

-- Update the weight_class column based on weight range
UPDATE dim_products
SET weight_class = CASE
    WHEN weight_in_kg < 2 THEN 'Light'
    WHEN weight_in_kg >= 2 AND weight_in_kg < 40 THEN 'Mid_Sized'
    WHEN weight_in_kg >= 40 AND weight_in_kg < 140 THEN 'Heavy'
    ELSE 'Truck_Required'
END;

--Rename the removed column
ALTER TABLE dim_products
RENAME COLUMN removed TO still_available;

-- Change data type of product_price and weight_in_kg to FLOAT
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT,
ALTER COLUMN weight_in_kg TYPE FLOAT;

-- Determine the maximum length for VARCHAR columns
SELECT MAX(LENGTH(ean)) AS max_EAN_length,
       MAX(LENGTH(product_code)) AS max_product_code_length,
       MAX(LENGTH(weight_class)) AS max_weight_class_length
FROM dim_products;
--ean: 17; product_code: 11; weight_class: 11

-- Change the values from "still_available" column to TRUE or FALSE
UPDATE dim_products
SET still_available = 
    CASE
        WHEN still_available = 'still_available' THEN TRUE
        WHEN still_available = 'removed' THEN FALSE
        ELSE NULL
    END;

-- Change data type of EAN to VARCHAR(?)
ALTER TABLE dim_products
	ALTER COLUMN ean TYPE VARCHAR(17),
	ALTER COLUMN product_code TYPE VARCHAR(11),
	ALTER COLUMN date_added TYPE DATE,
	ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
	ALTER COLUMN still_available TYPE BOOLEAN USING still_available::boolean,
	ALTER COLUMN weight_class TYPE VARCHAR(14);
	
--Find the max lenght for the columns
SELECT 
    MAX(LENGTH(month)) AS max_month_length,
    MAX(LENGTH(year)) AS max_year_length,
    MAX(LENGTH(day)) AS max_day_length,
    MAX(LENGTH(time_period)) AS max_time_period_length
FROM 
    dim_dates_time;
-- month: 2; year: 4; day: 2; time_period: 7
--Delete invalid values from date_uuid column
DELETE FROM dim_dates_time
WHERE (date_uuid !~* '^[a-f0-9]{8}-([a-f0-9]{4}-){3}[a-f0-9]{12}$' or date_uuid is NULL);

-- Change data types of columns
ALTER TABLE dim_dates_time
    ALTER COLUMN month TYPE VARCHAR(2), 
    ALTER COLUMN year TYPE VARCHAR(4),
    ALTER COLUMN day TYPE VARCHAR(2),
    ALTER COLUMN time_period TYPE VARCHAR(7),
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
	
--Find the max lenght for the columns	
SELECT 
    MAX(LENGTH(card_number)) AS max_card_number_length,
    MAX(LENGTH(expiry_date)) AS max_expiry_date_length
FROM dim_card_details;

-- Change data types of columns
ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(19), 
    ALTER COLUMN expiry_date TYPE VARCHAR(5),
    ALTER COLUMN date_payment_confirmed TYPE DATE;
	
-- Add primary key to dim_users
ALTER TABLE dim_users
    ADD PRIMARY KEY (user_uuid);

-- Add primary key to dim_store_details
ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code);

-- Add primary key to dim_products
ALTER TABLE dim_products
    ADD PRIMARY KEY (product_code);

-- Add primary key to dim_dates_time
ALTER TABLE dim_dates_time
    ADD PRIMARY KEY (date_uuid);

-- Add primary key to dim_card_details
ALTER TABLE dim_card_details
    ADD PRIMARY KEY (card_number);
	
	
-- Add foreign key constraints to orders_table referencing dim_tables
ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_users
FOREIGN KEY (user_uuid)
REFERENCES dim_users(user_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_products
FOREIGN KEY (product_code)
REFERENCES dim_products(product_code);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_dates_time
FOREIGN KEY (date_uuid)
REFERENCES dim_dates_time(date_uuid);

ALTER TABLE orders_table
ADD CONSTRAINT fk_orders_dim_card_details
FOREIGN KEY (card_number)
REFERENCES dim_card_details(card_number);
--------------------------------------------------------------------------------------------------------------------------
-- Milestone 4:

--Show where the company currently operates and how many stores on each country
SELECT country_code AS country, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY total_no_stores DESC;
+----------+-----------------+
| country  | total_no_stores |
+----------+-----------------+
| GB       |             266 |
| DE       |             141 |
| US       |              34 |
+----------+-----------------+

--Show which location they have the most stores
SELECT locality, COUNT(*) AS total_no_stores
FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC
LIMIT 7;
+-------------------+-----------------+
|     locality      | total_no_stores |
+-------------------+-----------------+
| Chapletown        |              14 |
| Belper            |              13 |
| Bushley           |              12 |
| Exeter            |              11 |
| High Wycombe      |              10 |
| Arbroath          |              10 |
| Rutherglen        |              10 |
+-------------------+-----------------+

-- Shows the month where the company produced the most sales
SELECT EXTRACT(MONTH FROM to_date(dt."month", 'MM')) AS month,
       SUM(o.product_quantity * dp.product_price) AS total_revenue
FROM orders_table o
JOIN dim_dates_time dt ON o.date_uuid = dt.date_uuid
JOIN dim_products dp ON o.product_code = dp.product_code
GROUP BY EXTRACT(MONTH FROM to_date(dt."month", 'MM'))
ORDER BY total_revenue DESC
LIMIT 6;
+-------------+-------+
| total_sales | month |
+-------------+-------+
|   670290.42 |     8 |
|   664907.40 |     1 |
|   654338.30 |    10 |
|   647346.85 |     5 |
|   643247.35 |     7 |
|   642447.38 |     3 |
+-------------+-------+

--Show how many sales are happening online and offline
SELECT CASE 
        WHEN sd.store_type = 'Web Portal' THEN 'Online'
        ELSE 'Offline'
    END AS store_type,
    COUNT(*) AS number_of_sales,
    SUM(o.product_quantity) AS product_quantity_count
FROM orders_table o
INNER JOIN dim_store_details sd ON o.store_code = sd.store_code
GROUP BY store_type;
+------------------+-------------------------+----------+
| numbers_of_sales | product_quantity_count  | location |
+------------------+-------------------------+----------+
|            26957 |                  107739 | Web      |
|            93166 |                  374047 | Offline  |
+------------------+-------------------------+----------+

-- Show the percentage of sales that comes trhough each type of store
SELECT dsd.store_type,
    ROUND(SUM(ot.product_quantity * ot.product_price)) AS total_sales,
    SUM(ot.product_quantity * ot.product_price) * 100.0 / total.total_sales AS percentage_total
FROM orders_table ot
JOIN dim_store_details dsd ON ot.store_code = dsd.store_code
JOIN (SELECT SUM(product_quantity * product_price) AS total_sales FROM orders_table) total
ON true
GROUP BY dsd.store_type, total.total_sales
ORDER BY total_sales DESC;
+-------------+-------------+---------------------+
| store_type  | total_sales | percentage_total(%) |
+-------------+-------------+---------------------+
| Local       |     3425266 |               44.54 |
| Web portal  |     1719107 |               22.35 |
| Super Store |     1219474 |               15.85 |
| Mall Kiosk  |      695696 |                9.04 |
| Outlet      |      629469 |                8.18 |
+-------------+-------------+---------------------+

-- Show which month in each year produced the most revenue for the company
SELECT
    MAX(total_sales) AS total_sales,
    EXTRACT(YEAR FROM to_date(sales_by_month."year", 'YYYY')) AS year,
    EXTRACT(MONTH FROM to_date(sales_by_month."month", 'MM')) AS month
FROM (SELECT
        ROUND(SUM(ot.product_quantity * ot.product_price)) AS total_sales,
        dt."year",
        dt."month"
    FROM orders_table ot
    JOIN dim_dates_time dt ON ot.date_uuid = dt.date_uuid
    GROUP BY dt."year", dt."month") AS sales_by_month
GROUP BY
    EXTRACT(YEAR FROM to_date(sales_by_month."year", 'YYYY')),
    EXTRACT(MONTH FROM to_date(sales_by_month."month", 'MM'))
ORDER BY total_sales DESC
LIMIT 10;
+-------------+------+-------+
| total_sales | year | month |
+-------------+------+-------+
|    27883.38 | 1994 |     3 |
|    27202.72 | 2019 |     1 |
|    27029.07 | 2009 |     8 |
|    26628.89 | 1997 |    11 |
|    26310.96 | 2018 |    12 |
|    26201.51 | 2017 |     9 |
|    26150.51 | 2019 |     8 |
|    25790.12 | 2010 |     5 |
|    25594.28 | 2000 |     1 |
|    25522.76 | 1996 |     8 |
+-------------+------+-------+

-- Show the headcount of staff around the world
SELECT SUM(sd.staff_numbers) AS total_staff_numbers, sd.country_code
FROM dim_store_details sd
GROUP BY sd.country_code
ORDER BY total_staff_numbers DESC;
+---------------------+--------------+
| total_staff_numbers | country_code |
+---------------------+--------------+
|               13307 | GB           |
|                6123 | DE           |
|                1384 | US           |
+---------------------+--------------+

-- Show which store type is selling the most in Germany
SELECT
    SUM(o.product_quantity * dp.product_price) AS total_sales,
    sd.store_type,
    sd.country_code
FROM orders_table o
JOIN dim_products dp ON o.product_code = dp.product_code
JOIN dim_store_details sd ON o.store_code = sd.store_code
WHERE sd.country_code = 'DE'
GROUP BY sd.store_type, sd.country_code
ORDER BY total_sales;
+--------------+-------------+--------------+
| total_sales  | store_type  | country_code |
+--------------+-------------+--------------+
|   197516.67  | Outlet      | DE           |
|   246502.79  | Mall Kiosk  | DE           |
|   383107.30  | Super Store | DE           |
|  1104791.73  | Local       | DE           |
+--------------+-------------+--------------+

-- Show the avarage of how quickly the company is making a sale throughout the year

WITH event_times AS (
    SELECT TO_TIMESTAMP(CONCAT(year, '-', month, '-', day, ' ', timestamp), 'YYYY-MM-DD HH24:MI:SS') AS event_time
    FROM dim_dates_time),
next_event_times AS (
    SELECT event_time, LEAD(event_time) OVER (ORDER BY event_time) AS next_event_time
    FROM event_times)
SELECT EXTRACT(YEAR FROM event_time) AS year,
    JSON_BUILD_OBJECT(
        'hours', ROUND(AVG(EXTRACT(HOUR FROM next_event_time - event_time))),
        'minutes', ROUND(AVG(EXTRACT(MINUTE FROM next_event_time - event_time))),
        'seconds', ROUND(AVG(EXTRACT(SECOND FROM next_event_time - event_time))),
		'milliseconds', ROUND(AVG(EXTRACT(MILLISECONDS FROM next_event_time - event_time)))) AS actual_time_taken
FROM next_event_times
GROUP BY EXTRACT(YEAR FROM event_time)
ORDER BY EXTRACT(YEAR FROM event_time);

 +------+-------------------------------------------------------+
 | year |                actual_time_taken                      |
 +------+-------------------------------------------------------+
 | 1993 | "hours": 2, "minutes": 26, "seconds": 29, "millise... |
 | 2002 | "hours": 2, "minutes": 13, "seconds": 50, "millise... | 
 | 2022 | "hours": 2, "minutes": 13, "seconds": 6,  "millise... |
 | 2008 | "hours": 2, "minutes": 13, "seconds": 2,  "millise... |
 | 2013 | "hours": 2, "minutes": 17, "seconds": 12, "millise... |
 +------+-------------------------------------------------------+