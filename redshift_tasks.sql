-- This queries were performed on Redshift query editor

-- loading data from a CSV 
COPY covid_stats
FROM 's3://ap-redshift-bucket/worldometer_data_cleaned.csv'
IAM_ROLE 'arn:aws:iam::257394483480:role/ap-redshift-s3-full-access'
CSV
IGNOREHEADER 1;


-- Loading csv with a delimiter
COPY covid_stats
FROM 's3://ap-redshift-bucket/demo_char_delimited.txt'
IAM_ROLE 'arn:aws:iam::257394483480:role/ap-redshift-s3-full-access'
DELIMITER '|';


-- Loading csv with a fixed
COPY covid_stats
FROM 's3://ap-redshift-bucket/demo_fixed_width.txt'
IAM_ROLE 'arn:aws:iam::257394483480:role/ap-redshift-s3-full-access'
FIXEDWIDTH '1,16,2';


--Get total number of countries and regions
SELECT COUNT(DISTINCT country_region) AS total_countries
FROM covid_stats;

--total number of cases, deaths, and recoveries
SELECT 
   SUM(total_cases) AS total_cases,
   SUM(total_deaths) AS total_deaths,
   SUM(total_recovered) AS total_recovered
FROM covid_stats;


-- --Analyze Per Capita Testing
SELECT country_region, tests_per_million
FROM covid_stats
ORDER BY tests_per_million DESC
LIMIT 10;





