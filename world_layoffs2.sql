-- DATA CLEANING 

SELECT * FROM layoffs;

-- 1.REMOVE DUPLICATES 
-- 2.STANDARDIZE DATA
-- 3.NULL VALUES OR BLANK VALUES 
-- 4.REMOVE ANY COLUMNS AND ROWS

CREATE TABLE layoffs_staging 
LIKE layoffs;
-- to copy all data from raw original layoff table to the layoff_staging 

SELECT * FROM layoffs_staging; -- this will show columns 

INSERT layoffs_staging -- this help us insert values into table
SELECT * FROM layoffs;

SELECT *,   -- to check if we have duplicates by creating a row_number if its greater than 2 under cte that means we have duplicates
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, date ,stage,country,funds_raised_millions) AS row_num
 FROM layoffs_staging
;

WITH duplicates_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, date, stage,country,funds_raised_millions) AS row_num
 FROM layoffs_staging
)
SELECT * FROM duplicates_cte
WHERE row_num > 1;

-- To check one of the duplicates 
SELECT * FROM layoffs_staging
WHERE company = 'Casper';

-- one other safe way to remove duplicate is to create table and put them in stage 2 database because you cannnot update cte/copy clipboard 
-- and create statement

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * FROM layoffs_staging2;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off, date, stage,country,funds_raised_millions) AS row_num
 FROM layoffs_staging;
 
SELECT * FROM layoffs_staging2
WHERE row_num > 1;

DELETE FROM layoffs_staging2
WHERE row_num > 1;

SELECT * FROM layoffs_staging2

-- Standardizing data
-- to remove space at beginnning or end to be able to make changes go to  edit, preference,SQL editor and uncheck box 
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

-- to make crypto and crypto currency  group together since they are one and same thing to ensure consistency
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- to double check again if changes were made successfully 
SELECT DISTINCT industry
FROM layoffs_staging2;

-- we should look at each column , give time looking at data understanding it to be able to see where it need changes / sorted 
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%'

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- to change date column from text to date format to do time series 
SELECT `date`,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = str_to_date(`date`,'%m/%d/%Y')


SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
modify column `date` DATE;

SELECT *
FROM layoffs_staging2


-- working with null and blank values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = ' ';

SELECT * FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

SELECT * FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry   
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT * FROM layoffs_staging2

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging2

ALTER TABLE layoffs_staging2
DROP column row_num;

