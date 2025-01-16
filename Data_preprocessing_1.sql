-- Dropping Database if exist
DROP database IF exists layoffs;

-- Creating new Database 
CREATE DATABASE layoffs;

-- Data imported through the Data Import Wizard
-- 3491 rows imported

SELECT COUNT(*) FROM  layoffs.layoffs_raw;
-- Number of rows is 3491
-- All data imported. 

-- Creating a working Table
CREATE TABLE layoffs.layoffs_wrk
LIKE layoffs.layoffs_raw;

-- Populating the working table with data from layoffs_raw
INSERT layoffs_wrk
SELECT * FROM layoffs_raw;
-- 3491 rows copied to layoffs_wrk

SELECT * FROM layoffs.layoffs_wrk;

-- DATA FILTERING
-- No coulmn to drop we would be using all coulumns for analysis

-- DATA DEDUPLICATION
-- 1. Removing all duplicate data
-- Detecting row data that occured more than once
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY company, location,industry, total_laid_off,percentage_laid_off,`date`,stage,country,
    funds_raised) AS row_num
FROM layoffs.layoffs_wrk;

-- creating a cte table to filter row data more than one
WITH duplicate_cte AS
(
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY company, location,industry, total_laid_off,percentage_laid_off,`date`,stage,country,
    funds_raised) AS row_num
FROM layoffs.layoffs_wrk
)
SELECT * FROM duplicate_cte
WHERE row_num > 1 ;
-- 2 rows affected

-- Checking duolicate row data
SELECT*FROM layoffs_wrk
where company = 'cazoo';

-- Creating new table , adding row_number as new column
CREATE TABLE `layoffs_wrk_2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- Inserting value into new working table
INSERT INTO layoffs_wrk_2
SELECT *,
	ROW_NUMBER() OVER (
	PARTITION BY company, location,industry, total_laid_off,percentage_laid_off,`date`,stage,country,
    funds_raised) AS row_num
FROM layoffs.layoffs_wrk;
-- 3491 row data moved into new table

SELECT * FROM layoffs_wrk_2;

-- Deleting Duplicated Row from  layoffs_wrk_2
DELETE 
FROM layoffs_wrk_2
WHERE row_num > 1;
-- 2 rows affected

-- Total number of unduplicated row in the data
SELECT COUNT(*) FROM layoffs_wrk_2;
-- 3489 rows

-- 2. Data Standardization
-- Putting all data into a common format to facilitate comparison and analysis
SELECT * FROM layoffs_wrk_2;

-- Data Trimming
SELECT company , TRIM(company) from layoffs_wrk_2;
-- Update the trim Data

UPDATE layoffs_wrk_2
set company = TRIM(company);

-- Distinct Data 
SELECT DISTINCT(industry) FROM layoffs_wrk_2;

SELECT DISTINCT(country) FROM layoffs_wrk_2;

-- 3. Data Transformation - Modifiying Data to make it more suitable for analysis
-- Working on date coulmn
SELECT `date` FROM layoffs_wrk_2;

SELECT `DATE`, STR_TO_DATE(`date`, '%Y-%m-%d')
FROM layoffs_wrk_2;
-- Converting the date column from string to date
UPDATE layoffs_wrk_2
SET `date` = STR_TO_DATE(`date`, '%Y-%m-%d');

alter table layoffs_wrk_2
modify column `date` DATE;

-- 4. Data Imputation
-- Replacing Null Values with estimated values

-- Working on null values
SELECT * FROM layoffs_wrk_2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;
-- updating value for Appsmith Industry
UPDATE layoffs_wrk_2
SET industry = 'Software'
WHERE industry = '';
-- 1 row affected

SELECT *
FROM layoffs_wrk_2
WHERE total_laid_off = ''
AND percentage_laid_off = '';
-- 556 rows have both totail_laid_off and percentage laid off as empty cell

DELETE FROM layoffs_wrk_2
WHERE total_laid_off = ''
AND percentage_laid_off = '';
-- 556 rows affected

-- Numbers of row remaining for analysis
SELECT COUNT(*) FROM layoffs_wrk_2;
-- 2933 rows

SELECT * FROM layoffs_wrk_2
WHERE total_laid_off = '';
-- 629 rows with missing data

SELECT * FROM layoffs_wrk_2
WHERE percentage_laid_off = '';
-- 687 rows with missing data

SELECT *
FROM layoffs_wrk_2
WHERE total_laid_off != ''
AND percentage_laid_off != '';
-- 1617 rows with data for both total_laid_off and percentage_laid_off.

-- Creating new table for final anaylsis involving data for both total_laid_off and percentage_laid_off
CREATE TABLE `layoffs_wrk_3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` date DEFAULT NULL,
  `stage` text,
  `country` text,
  `funds_raised` double DEFAULT NULL,
  `row_num` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_wrk_3
SELECT *
FROM layoffs_wrk_2
WHERE total_laid_off != ''
AND percentage_laid_off != '';
-- 1617 rows

-- Drop row_num column
ALTER TABLE layoffs_wrk_3
DROP COLUMN row_num;

SELECT * FROM layoffs_wrk_3;

-- Exporting Working File to local machine
SELECT * 
FROM layoffs_wrk_3
INTO OUTFILE 'C:\Users\user\Desktop\DA\Portfolio\SQL\Data Preprocessing\Clean Data'
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n';




















