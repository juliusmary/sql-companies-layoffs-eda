select * from layoffs;

-- 1, Remove Duplicates
-- 2, Standardize the data
-- 3, Null values or blank values
-- 4, Remove any Columns that are not necessary

Create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;
-- this create the columns but the actual datas will not inserted

insert layoffs_staging
select * from layoffs;

select *,
row_number () over ( Partition by company, industry, total_laid_off, 'date') as row_num
from layoffs_staging;

-- So 'row_num' does not exist when 'WHERE' runs.
-- 'date' = text, "date" = text, `date` = column, date = command

with duplicate_cte As 
( 
select *,
row_number () over (
Partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
from layoffs_staging
) 
select * 
from duplicate_cte 
where row_num > 1;

select * from layoffs_staging where company = 'elemy';

SHOW CREATE TABLE layoffs_staging;

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

insert into layoffs_staging2
select *,
row_number () over (
Partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
from layoffs_staging;

TRUNCATE TABLE layoffs_staging2;

select * from layoffs_staging2
where row_num > 1;

delete from layoffs_staging2
where row_num > 1;

SET SQL_SAFE_UPDATES = 0;

-- Standardizing data

SELECT company, trim(Company)
FROM layoffs_staging2;

Update layoffs_staging2
set company = trim(company);

SELECT distinct industry
FROM layoffs_staging2 order by 1;

SELECT *
FROM layoffs_staging2 where industry like 'crypto%';

Update layoffs_staging2 set industry = 'Crypto'
where industry like 'crypto%';

SELECT distinct Location
FROM layoffs_staging2 order by 1;

SELECT distinct *
FROM layoffs_staging2 order by 1;

SELECT distinct *
FROM layoffs_staging2 
where country like 'united states%'
order by 1;

SELECT distinct country, trim(trailing '.' from country)
FROM layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united states%';

-- To do Time series, the date column needs to be chnged from text to date

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set date = str_to_date(`date`, '%m/%d/%Y');

SET SQL_SAFE_UPDATES = 0;

alter table layoffs_staging2
modify column `date` date;

-- 3, Null values or blank values
-- using = NULL will not work, rather use IS NULL

Select * 
from layoffs_staging2 
where industry is Null or Industry = '';

select * 
from layoffs_staging2 
where company = 'airbnb';

select t1.industry, t2.industry from layoffs_staging2 t1 
Join layoffs_staging2 t2
    On t1.company = t2.company
    where (t1.industry is Null or t1.industry = '')
    and t2.industry is not null;
    
    SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
WHERE (t1.industry IS NULL OR TRIM(t1.industry) = '')
  AND t2.industry IS NOT NULL
  AND TRIM(t2.industry) <> '';

Update layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
    set t1.industry = t2.industry
WHERE (t1.industry IS NULL OR TRIM(t1.industry) = '')
and t2.industry is not Null;

SET SQL_SAFE_UPDATES = 0;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = company;

Update layoffs_staging2 
set industry = Null where Industry = '';

Select * 
from layoffs_staging2 
where total_laid_off is Null and percentage_laid_off is Null;

delete 
from layoffs_staging2 
where total_laid_off is Null and percentage_laid_off is Null;

Select * 
from layoffs_staging2 where company like 'Amazon';

Alter table layoffs_staging2 
drop column row_num;

-- Exploratory Data Analysis

select * from layoffs_staging2;

Select Max(total_laid_off), max(percentage_laid_off) 
from layoffs_staging2;

Select * from layoffs_staging2 
where percentage_laid_off = 1 
order by total_laid_off desc;

Select * from layoffs_staging2 
where percentage_laid_off = 1 
order by funds_raised_millions desc;

select company, sum(total_laid_off) 
from layoffs_staging2 
group by company 
order by 2 desc;

select company, sum(total_laid_off) as total from layoffs_staging2 group by company 
order by total desc;

SELECT company,
       SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY total_laid_off DESC, company asc;

Select min(`date`), max(`date`) 
from layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

select country, sum(total_laid_off) 
from layoffs_staging2 
group by country
order by 2 desc;

select `date`, sum(total_laid_off) 
from layoffs_staging2 
group by `date`
order by 1 desc;

select Year(`date`), sum(total_laid_off) 
from layoffs_staging2 
group by Year(`date`)
order by 2 desc;

select stage, sum(total_laid_off) 
from layoffs_staging2 
group by stage
order by 2 desc;

select Year(`date`), sum(total_laid_off) 
from layoffs_staging2 
group by Year(`date`)
order by 2 desc;

Select company, sum(percentage_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

Select company, avg(percentage_laid_off) 
from layoffs_staging2
group by company
order by 2 desc;

select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc;

-- rolling sum

With rolling_total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total
from layoffs_staging2
where substring(`date`,1,7) is not null
group by `month`
order by 1 asc
)
select `month`, total, sum(total) over(order by `month`) as rolling_total
from rolling_total;

SELECT VERSION();

Select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
order by 3 desc;

with company_year (company, years, total_laid_off) as
(
Select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
)
select *, 
dense_rank() over(partition by years order by total_laid_off desc) layoff_rank
from company_year;

with company_year (company, years, total_laid_off) as
(
Select company, year(`date`), sum(total_laid_off) 
from layoffs_staging2
group by company, year(`date`)
), company_year_rank as
(select *, 
dense_rank() over(partition by years order by total_laid_off desc) layoff_rank
from company_year
where years is not null
)
select * 
from company_year_rank 
where layoff_rank <= 5;