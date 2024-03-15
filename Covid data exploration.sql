SELECT * FROM project.`covid deaths`;

CREATE DATABASE Covid;

use Covid;

CREATE TABLE covid_deaths(
iso_code varchar(20),	
continent varchar(100) DEFAULT NULL,	
location varchar(100),
dates date,
population int,
total_cases int DEFAULT NULL,
new_cases int DEFAULT NULL,
new_cases_smoothed FLOAT,
total_deaths int DEFAULT NULL,	
new_deaths int DEFAULT NULL,
new_deaths_smoothed FLOAT,
total_cases_per_million	FLOAT DEFAULT 0.00,
new_cases_per_million FLOAT DEFAULT 0.00,
new_cases_smoothed_per_million FLOAT DEFAULT 0.00,
total_deaths_per_million FLOAT DEFAULT 0.00,	
new_deaths_per_million FLOAT DEFAULT 0.00,
new_deaths_smoothed_per_million FLOAT DEFAULT 0.00,
reproduction_rate FLOAT DEFAULT 0.00,
icu_patients int DEFAULT NULL,
icu_patients_per_million FLOAT DEFAULT 0.00,	
hosp_patients int DEFAULT NULL,
hosp_patients_per_million FLOAT DEFAULT 0.00,
weekly_icu_admissions int DEFAULT NULL,
weekly_icu_admissions_per_million FLOAT DEFAULT 0.00,
weekly_hosp_admissions int DEFAULT NULL,
weekly_hosp_admissions_per_million FLOAT DEFAULT 0.00
);


Load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Covid_Deaths.csv'
into table covid_deaths
fields terminated by ','
lines terminated by '\n';

SELECT COUNT(*) from covid_deaths;

SET GLOBAL sql_mode = '';

CREATE TABLE covid_vaccination(
iso_code varchar(20),	
continent varchar(100),	
location varchar(100),
dates date,
total_tests int,
new_tests int,	
total_tests_per_thousand float ,	
new_tests_per_thousand float,
new_tests_smoothed int,	
new_tests_smoothed_per_thousand	float,
positive_rate float,
tests_per_case float,
tests_units varchar(100),
total_vaccinations int,	
people_vaccinated int,
people_fully_vaccinated	int,
total_boosters int,
new_vaccinations int,	
new_vaccinations_smoothed int,	
total_vaccinations_per_hundred float,
people_vaccinated_per_hundred float,	
people_fully_vaccinated_per_hundred float,	
total_boosters_per_hundred float,
new_vaccinations_smoothed_per_million int,
new_people_vaccinated_smoothed int,
new_people_vaccinated_smoothed_per_hundred float,	
stringency_index float,	
population_density float,
median_age float,	
aged_65_older float,	
aged_70_older float,	
gdp_per_capita float,	
extreme_poverty	float,
cardiovasc_death_rate float,	
diabetes_prevalence float,	
female_smokers float,
male_smokers float,	
handwashing_facilities float,	
hospital_beds_per_thousand float,
life_expectancy	float,
human_development_index float,	
excess_mortality_cumulative_absolute float,
excess_mortality_cumulative float,	
excess_mortality float,
excess_mortality_cumulative_per_million float
);

Load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Covid Vaccination.csv'
into table covid_vaccination
fields terminated by ','
lines terminated by '\n';

select * from covid_vaccination;

select * from covid_deaths
WHERE location = 'World';

select location, dates, total_cases, new_cases, total_deaths, population
from covid_deaths
order by 1,2;

#looking at total cases vs total deaths 
#shows likelihood of dying if you contract covid in your countrty
select location, dates, total_cases, total_deaths, (total_deaths/total_cases)*100 as Death_percentage
from covid_deaths
where location like '%states%'
order by 1,2;

#looking at total cases vs population
#shows what percentage of population got covid
select location, dates, total_cases, population,(total_cases/population)*100 as infected_percentage
from covid_deaths
where location like '%states%'
order by 1,2;

#Looking at countries with highest infection rate compared to population
select location, population,  Max(total_cases) as highestinfectioncount, Max((total_cases/population)*100) as infected_percentage
from covid_deaths
##where location like '%states%'
Group by Location, Population
order by 4 desc;

#Showing countruies with highest death count per population
select location,  max(total_deaths) as Totaldeathcount
from covid_deaths
##where location like '%states%'
where continent is not null
Group by Location
order by Totaldeathcount desc;

UPDATE covid_deaths
SET continent = NULL 
WHERE continent = '';

SET SQL_SAFE_UPDATES = 0;

#let's break it down by continents
select continent,  max(total_deaths) as Totaldeathcount
from covid_deaths
where continent is not null
Group by continent
order by Totaldeathcount desc;


# GLOBAL NUMBERS

select dates, SUM(new_cases)
from covid_deaths
where continent is not null
Group by dates
order by 1,2;

select dates, SUM(new_cases), SUM(new_deaths), SUM(new_deaths)/SUM(new_cases)*100 as deathpercentage
from covid_deaths
where continent is not null
Group by dates
order by 1,2;

select dates, SUM(new_cases), SUM(new_deaths), SUM(new_deaths)/SUM(new_cases)*100 as deathpercentage
from covid_deaths
where continent is not null
#Group by dates
order by 1,2;

#looking at total population vs vaccination
select dea.continent, dea.location, dea.dates, dea.population, vac.new_vaccinations from covid_deaths as dea
join covid_vaccination as vac
on dea.location = vac.location
and dea.dates = vac.dates
where dea.continent is not null
order by 2,3;

# use CTE

With PopvsVac (continent, location, dates, population, new_Vaccinations, Rollingpeoplevaccinated)
as 
(
select dea.continent, dea.location, dea.dates, dea.population, vac.new_vaccinations, Sum(vac.new_vaccinations) OVER(Partition by dea.location Order by dea.location,dea.dates) as Rollingpeoplevaccinated from covid_deaths as dea
join covid_vaccination as vac
on dea.location = vac.location
and dea.dates = vac.dates
where dea.continent is not null
#order by 2,3;
)
Select *, (Rollingpeoplevaccinated/population)*100
From PopvsVac;


Create Table PercentagePopulationVaccinated
(
Continent varchar(100),
Location varchar(100),
Dates datetime,
Population int,
New_vaccinations int,
RollingPeopleVaccinated float
);

Insert into PercentagePopulationVaccinated
select dea.continent, dea.location, dea.dates, dea.population, vac.new_vaccinations, Sum(vac.new_vaccinations) OVER(Partition by dea.location Order by dea.location,dea.dates) as Rollingpeoplevaccinated from covid_deaths as dea
join covid_vaccination as vac
on dea.location = vac.location
and dea.dates = vac.dates;
#where dea.continent is not null
#order by 2,3;

Select*,(Rollingpeoplevaccinated/population)*100
From PercentagePopulationVaccinated;

#Creating Views

Create View PercentPopulationVaccinated as
select dea.continent, dea.location, dea.dates, dea.population, vac.new_vaccinations, Sum(vac.new_vaccinations) OVER(Partition by dea.location Order by dea.location,dea.dates) as Rollingpeoplevaccinated from covid_deaths as dea
join covid_vaccination as vac
on dea.location = vac.location
and dea.dates = vac.dates
where dea.continent is not null
order by 2,3;
