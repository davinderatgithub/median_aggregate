CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;

-- varchar, even number of records
CREATE TABLE varchar_data (value VARCHAR(255));

INSERT INTO varchar_data (value) VALUES ('date'), ('banana'), ('apple'), ('cherry');
SELECT * from varchar_data ORDER BY value;
SELECT median(value) AS median_value FROM varchar_data;


-- Window function test
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    product_id INT,
    amount INT
);

INSERT INTO sales (product_id, amount) VALUES
(1, 100),
(1, 200),
(1, 150),
(2, 300),
(2, 350),
(2, 400),
(3, 250),
(3, 300),
(3, 350);

select * from sales;

--median sales amount for each product
SELECT
    id,
    product_id,
    amount,
    median(amount) OVER (PARTITION BY product_id) AS median_amount
FROM
    sales;

--calculate running median
SELECT
    id,
    product_id,
    amount,
    median(amount) OVER (PARTITION BY product_id ORDER BY id) AS running_median_amount
FROM
    sales;


-- Test aggregate with all NULL values
-- Create a test table
CREATE TEMP TABLE test_median (
    id SERIAL PRIMARY KEY,
    value NUMERIC
);

-- Insert some test data, including NULLs
INSERT INTO test_median (value) VALUES
    (10),
    (20),
    (30),
    (NULL),
    (40),
    (NULL),
    (50);

-- Test aggregate with all NULL values
SELECT median(value) AS median_value FROM test_median WHERE value IS NULL;
