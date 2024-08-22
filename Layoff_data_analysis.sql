-- Data Cleaning

SELECT * 
FROM world_layoffs.layoffs ;

-- 1.Remove Duplicates
-- 2. Standartize the Data
-- 3.Null Values or Blank Values
-- 4.Remove any Column

-- Always Create staging (Try to copy the raw data to another table , so it doesn't affect the raw data)

CREATE TABLE layoffs_Staging
LIKE world_layoffs.layoffs;

SELECT * 
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM world_layoffs.layoffs;

SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off , percentage_laid_off, `date` , stage , country,funds_raised_millions) AS row_num
FROM layoffs_staging;


-- With CTE we give the query with the query-- to select the rows no >1 from the table
WITH duplicate_cte AS
(
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off , percentage_laid_off, `date` , stage ,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num >1;

select * 
from layoffs_staging
where company='Casper';

-- If we have a unique column the work is very easy
-- Cretating a new table with separte column of row no so we can delete that the row no above 2 from thst staging table
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2
WHERE row_num>1;

-- Inserting the elements into the staging2 table from the staging 1 table
INSERT INTO layoffs_staging2
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off , percentage_laid_off, `date` , stage ,country,funds_raised_millions) AS row_num
FROM layoffs_staging;

-- Deletion of row above row_num 2
DELETE 
FROM layoffs_staging2
WHERE row_num>1;


-- Checking whether it has deleted
SELECT * 
FROM layoffs_staging2
WHERE row_num>1;

-- ------> Removing Duplicaes Done------------

-- Standarding the data
-- If we have extra column , or space , we can standardize it to a proper format

-- Triming the Extra Space in the company Column
select company, TRIM(COMPANY)
FROM layoffs_staging2;


-- Updating the Trimed Company into layoff_staging2 table for standaizing it
UPDATE layoffs_staging2
SET Company=TRIM(COMPANY);

SELECT * 
FROM layoffs_staging2 ;


-- Simalar Industry into same name eg: Crypto , Crypto Currency
SELECT DISTINCT industry
FROM layoffs_staging2 
ORDER BY 1;

-- To overview the Crypto Column
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';


-- Updating the Crypto or Crypto Currency as Crypto
UPDATE layoffs_staging2
SET industry ='Crypto'
WHERE industry like 'Crypto%';


-- Check for all the other column in the similar way (United States  , United States. both are similar)

SELECT DISTINCT country
FROM layoffs_staging2 
ORDER BY 1;

-- Trailing Will trim at end
SELECT DISTINCT country , TRIM(TRAILING '.'FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country= TRIM(TRAILING '.'FROM country)
WHERE country like 'United States%';

-- Updating the date Column in a standard date Column
-- Error occurs sort it out 
SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y' )
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`, '%m/%d/%Y' );

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
;
-- NUll values and Blank Spaces remove in the column
-- BOth the total_laid_off and percentage_laid_off is null its an useless value
-- This can be used an 4 step to remove the columns
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND
 percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry ='';

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb';

-- If the same industry name for same company , the blank spce can be filled with the industry name(eg: Travel --> Airbnb it also has blank spnce , it can be filled with Travel )
-- Seting the Null to the blank space
UPDATE layoffs_staging2
SET industry=null
WHERE industry ='';

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
WHERE (t1.industry is null or t1.industry ='')
AND t2.industry is not null;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
  ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE (t1.industry is null or t1.industry ='')
AND t2.industry is not null;
SELECT * FROM layoffs_staging2;

-- 4.REmoving the rows or columns (Deleting)
-- Both the columns are null so it can be removed from the column

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND
 percentage_laid_off IS NULL
;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND
 percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_staging2;

-- Now we no need of Row_num Column anymore
-- So we are going to drop a column

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;