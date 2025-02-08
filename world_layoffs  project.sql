# world layoffs data cleaning and  Exploratory Data analysis project 

select * 

from layoffs;

-- 1. remove duplicates

-- 2. standardize the data 

-- 3. Null values or blank values 

-- 4. Remove any columns 





-- 1. remove duplicates----------------------------------------------
create table layoffs_staging 

like layoffs; 


select * 

from layoffs_staging;

insert layoffs_staging

select * 
from layoffs;

with duplicate_CTE as 
( select * ,
ROW_NUMBER() OVER( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num
from layoffs_staging
 )
 
select * 
from duplicate_CTE; 

; 

select * 

from layoffs_staging 

where company = 'Casper';

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


select * 
from layoffs_staging2;


insert into layoffs_staging2 

select *, 
ROW_NUMBER() OVER( partition by company,location,industry,total_laid_off,percentage_laid_off,`date`,
stage,country,funds_raised_millions) as row_num 
from layoffs_staging;


select * 
from layoffs_staging2

where row_num > 1;


delete from layoffs_staging2 
where row_num > 1;

select * 
from layoffs_staging2;



-- 2. standardize the data--------------------------------------------------



select company,trim(company)
from layoffs_staging2;


update layoffs_staging2 
set company = trim(company);

select distinct industry
from layoffs_staging2
order by industry asc ;


select *

from layoffs_staging2

where industry like  'Crypto%';



update layoffs_staging2

set industry =  'Crypto'

where industry like 'Crypto%';


select distinct country

from layoffs_staging2

order by 1; 



update layoffs_staging2 

set country = Trim(Trailing '.' from country)

where country like 'United States%';



select `date`,

str_to_date(`date`,'%m/%d/%Y') as parsed_date

from layoffs_staging2;



SELECT 
    `date`,
    
    STR_TO_DATE(REPLACE(`date`, '-', '/'), '%m/%d/%Y') AS parsed_date
FROM layoffs_staging2;


update layoffs_staging2

set `date` = STR_TO_DATE(REPLACE(`date`, '-', '/'), '%m/%d/%Y');


select distinct `date` 

from layoffs_staging2;


select * 

from layoffs_staging2;


ALTER TABLE layoffs_staging2
CHANGE `date` `event_date` date;

-- 3. Null values or blank values-------------------------------------------------------------

select * 

from world_layoffs.layoffs_staging2

where total_laid_off is Null 
and percentage_laid_off Is null ;




select * 

from layoffs_staging2 

where industry  Is null 
Or industry = '';


select * 

from layoffs_staging2

where company = 'Airbnb' or company like 'Bally%' or company = 'Carvana' or company = 'Juul';



select t1.industry as industry_t1, t2.industry as industry_t2

from layoffs_staging2 as t1
join layoffs_staging2 as t2 
	on  t1.company = t2.company and t1.location = t2.location 
where (t1.industry is null or t1.industry = '') and t2.industry is not null;

update layoffs_staging2 

set industry = NULL 

where industry = '';


select * 

from layoffs_staging2

where industry is null;


update layoffs_staging2 as t1 join layoffs_staging2 as t2 
on t1.company = t2.company and t1.location = t2.location 
set t1.industry = t2.industry 
where t1.industry is null and t2.industry is not null;

-- 4. Remove any columns-----------------------------------------------


delete from layoffs_staging2 
where total_laid_off is null and percentage_laid_off is null;


alter table layoffs_staging2 drop column row_num;

select * 
from layoffs_staging2;

-- -------------------------------------------------------------------------------------------------------------------------------------

-- Exploratory Data analysis ------------------------------------------------------------------------------------------

select * 

from world_layoffs.layoffs_staging2;


select max(total_laid_off) , max(percentage_laid_off)

from layoffs_staging2;

select count(percentage_laid_off) as cp
from layoffs_staging2
group by percentage_laid_off
having percentage_laid_off = 1;

select * 

from layoffs_staging2 
where percentage_laid_off = 1 
order by funds_raised_millions;

select company,sum(total_laid_off) as total_laid_off_for_company
from layoffs_staging2 
group by company
order by total_laid_off_for_company desc;


select max(event_date),min(event_date)
from layoffs_staging2;

select industry,sum(total_laid_off) as sum_total_laidoff
from layoffs_staging2
group by industry
order by sum_total_laidoff desc;

select country,sum(total_laid_off) as sum_total_laidoff
from layoffs_staging2
group by country
order by sum_total_laidoff desc;


select year(event_date) as `Year`,sum(total_laid_off) as sum_total_laidoff
from layoffs_staging2
group by `Year`
order by `Year` desc;


select stage,sum(total_laid_off) as sum_total_laidoff
from layoffs_staging2
group by stage
order by sum_total_laidoff desc;


select company , avg(percentage_laid_off) avg_percentage_laid_off

from layoffs_staging2
group by company
order by avg_percentage_laid_off desc ;



select country,count(country)  as companies_by_country
from layoffs_staging2
group by country
order by companies_by_country desc;


select substring(event_date,1,7) as `Month`,sum(total_laid_off) as sum_tota_laid_off
from layoffs_staging2
group by `Month`
having `month` is not null
order by `month` asc;

with Rolling_Total as 
(select substring(event_date,1,7) as `Month`,sum(total_laid_off) as sum_tota_laid_off
from layoffs_staging2
group by `Month`
having `Month` is not null
order by `Month` asc)

select `Month`,sum_tota_laid_off,sum(sum_tota_laid_off) over(order by `Month`) as Rolling_Total

from Rolling_Total
;


select company,country,Year(event_date) as yearly,sum(total_laid_off)
from layoffs_staging2 
group by company,country,yearly
order by company ;


with Company_Year (company,country,Years,total_laid_off) as(
select company,country,Year(event_date) as yearly,sum(total_laid_off)
from layoffs_staging2 
group by company,country,yearly
having yearly is not null
order by yearly)

select * from company_Year;

with Company_Year (company,country,industry,Years,total_laid_off) as(
select company,country,industry,Year(event_date) as yearly,sum(total_laid_off)
from layoffs_staging2 
group by company,country,industry,yearly
having yearly is not null
)
, Company_Year_Rank as
 ( select * , dense_rank() over(partition by Years order by total_laid_off desc) as Ranking
from company_Year
) 

select * from Company_Year_Rank
where Ranking <=5;
-- -------------------------------------------------------------------------------------------------------------------------------------











