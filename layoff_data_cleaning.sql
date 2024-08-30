SELECT * FROM layoffs;

/*STEPS TO FOLLOW: - 
	1. REMOVING DUPLICATES
    2. STANDARDIZING THE DATA
    3. DEALING WITH THE NULL AND MISSING VALUES*/
    
-- Creating a copy of layoffs table
create table layoff_staging
like layoffs;

insert into layoff_staging
select * from layoffs; 

-- CHECKING FOR DUPLICATES
 with cte_duplicates as
 (
 select *,
 row_number() over(partition by company, location, industry, 
							   total_laid_off, percentage_laid_off, `date`, stage, country,
							   funds_raised_millions) as row_num 
from layoff_staging
) select * from cte_duplicates
where row_num > 1;


CREATE TABLE `layoff_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  row_num int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select * from layoff_staging2;

insert into layoff_staging2
select *,
 row_number() over(partition by company, location, industry, 
							   total_laid_off, percentage_laid_off, `date`, stage, country,
							   funds_raised_millions) as row_num 
from layoff_staging;

select * from layoff_staging2
where row_num > 1;

-- Deleting the rows having row_num > 1
delete from layoff_staging2
where row_num > 1;


-- STANDARDIZING THE DATA
-- Removing the white spaces from the company column
update layoff_staging2
set company = trim(company);

-- Dealing with the location column
select distinct location from layoff_staging2; 

-- Changing the FlorianÃ³polis to Florianópolis
update layoff_staging2
set location = 'Florianópolis'
where location like 'Florian%';

select location from layoff_staging2
where location like 'Florian%';

select distinct location, country from layoff_staging2
where location like 'Malm%';

update layoff_staging2
set location = 'Malmö'
where location like 'Malm%'; 
 
select distinct location, country from layoff_staging2
where location like '%sseldorf';

update layoff_staging2
set location = 'Düsseldorf'
where location like '%sseldorf';

-- Dealing with country column
select distinct country from layoff_staging2
order by 1;

update layoff_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- Dealing with date column
select `date` from layoff_staging2;

	-- Changing the datatype of date column
select `date`,
str_to_date(`date`,'%m/%d/%Y')
from layoff_staging2;

update layoff_staging2
set `date`= str_to_date(`date`,'%m/%d/%Y');

alter table layoff_staging2
modify column `date` date;

-- DEALING WITH THE NULL AND MISSING VALUES
select * from layoff_staging2
where total_laid_off is null and percentage_laid_off is null;

select distinct industry from layoff_staging2
order by 1;

select * from layoff_staging2
where industry is null or industry = '';

select * from layoff_staging2
where company = 'Airbnb';

-- coverting the missing values into null values
select * from  layoff_staging2
where industry = '';

update layoff_staging2
set industry = null
where industry = '';

select * from  layoff_staging2
where industry is null;

select t1.industry, t2.industry
from layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
where t1.industry is null and t2.industry is not null;

update layoff_staging2 t1
join layoff_staging2 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t2.industry is not null;

-- Dealing with the null values in total_laid_off and percentage_laid_off columns
select * from layoff_staging2
where total_laid_off is null and percentage_laid_off is null; 

/* Since we don't have any column through which we can calculate the null values of total_laid_off
and percentage_laid_off. Thus we have to remove them for now.*/
delete from layoff_staging2
where total_laid_off is null and percentage_laid_off is null; 

select * from layoff_staging2;

-- Removing the row_num column from the table
alter table layoff_staging2
drop column row_num; 