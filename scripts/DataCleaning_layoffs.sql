-- Data Cleaning
-- 1. Remove the duplicate
-- 2. Standardize the Data
-- 3. Null values or blank values
-- 4. Remove any columns if necessary

SELECT *
FROM layoffs;

-- count the records
SELECT COUNT(*)
FROM layoffs;

-- create a copy of the raw data
CREATE TABLE layoffs_staging
LIKE layoffs;
INSERT INTO layoffs_staging
SELECT*FROM layoffs;

-- Remove the duplicate
WITH duplicate_cte AS(
	SELECT *,
	ROW_NUMBER() 
	OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num
	FROM layoffs_staging
)
SELECT*
FROM duplicate_cte
WHERE row_num > 1;

SELECT*
FROM layoffs_staging
WHERE company = 'Casper';

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

SELECT*
FROM layoffs_staging2;
INSERT INTO layoffs_staging2
SELECT *,
	ROW_NUMBER() 
	OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
    ) AS row_num -- row_num count the row start from 1 if there's duplicate that row will be 2, 3 , ... and that's what we want to delete
FROM layoffs_staging;

-- delete if we found it row_num >1
DELETE
FROM layoffs_staging2
WHERE row_num >1
;
SELECT*
FROM layoffs_staging2;

-- Standardizing Data : make the data standard no unnecessay space , . and more

-- Check company
SELECT company, TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company)
;
-- Check industry
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

-- Check location
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY 1
;
-- Check country
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1
;
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1
;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Check Date 
SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Null and Blank Values
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = '';

-- update a records using the data from the another one
SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE ( t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2	-- update the blank one to NULL first
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


