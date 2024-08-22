-- Exploratory Data analyze (Exploring the cleaned data for a better understanding of the data)\

-- 1.To over view the Cleaned data
SELECT *
FROM layoffs_staging2
;

-- Just a check which company has the highest layoff
SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

-- Just a check How many companies has the percentage layoff as 1 which is they have layoffed completly
SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off=1
ORDER BY total_laid_off DESC;

-- Just a check that how many total sum of layoff happned in the company to get some insights
SELECT company , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY COMPANY 
ORDER BY 2 DESC;

-- Checking the time period of layoff happended
SELECT MIN(`date`) , MAX(`date`)
FROM layoffs_staging2;

-- Checking which industry has the most layoffs similar like company we have checked earliar
SELECT industry , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

SELECT *
FROM layoffs_staging2;

-- Checking which country has the highest tata_layoffs
SELECT country , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

-- Checking which date has the most layoffs
SELECT YEAR(`date`) , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

-- Checking which stage  has the most  total_layoffs
SELECT stage , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 1 DESC;

-- Similarly we will be checking with percentage laid_off for all Country ,company, industry , date , Stage to get some insights
SELECT company , SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company5
ORDER BY 2 DESC;

-- Sum of percentage layoff doesn't give proper information
-- Sameway we can do as many date checking we can do

SELECT SUBSTRING(`date`,1,7) AS MONTH , SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY MONTH 
ORDER BY 2 ASC;

-- We are analysing with the date and the total laid off as rolling totatl according to the month

WITH Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `MONTH` , SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 2 ASC
)
SELECT `MONTH` , total_off,SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM Rolling_total;

-- Comaparing year , company , total laid off
SELECT company , YEAR (`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC;

-- Partitioning and giving rank based on which company has laid of more on the particular year with CTE
WITH Company_year(Company, Years ,Total_layoffs) AS 
(
SELECT company , YEAR (`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC
)
SELECT * , DENSE_RANK() OVER(PARTITION BY years ORDER BY Total_layoffs DESC ) AS Ranking
FROM Company_Year
WHERE years and Total_layoffs is not null
ORDER BY Ranking ASC;

-- Selecting the top 5 companies (per year)based the ranking (usage of two cte's)
WITH Company_year(Company, Years ,Total_layoffs) AS 
(
SELECT company , YEAR (`date`),SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company , YEAR(`date`)
ORDER BY 3 DESC
),Company_year_rank AS
(
SELECT * , DENSE_RANK() OVER(PARTITION BY years ORDER BY Total_layoffs DESC ) AS Ranking
FROM Company_Year
WHERE years and Total_layoffs is not null
)
SELECT *
FROM Company_year_rank
WHERE Ranking <=5;