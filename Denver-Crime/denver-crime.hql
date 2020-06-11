

-- Step 1: hive/denver-crime/ directory created in HDFS

hadoop fs -mkdir -p /PROG8450/hive/denver-crime/


-- Step 2: crime.csv  file loaded in HDFS

hadoop fs -copyFromLocal crime.csv /PROG8450/hive/denver-crime/

-- Step 3: Crime Table Created using Hive

CREATE TABLE IF NOT EXISTS crime (DISTRICT_ID int,FIRST_OCCURRENCE_DATE date, GEO_LAT float, GEO_LON float, GEO_X int, GEO_Y int,INCIDENT_ADDRESS string, INCIDENT_ID BIGINT, IS_CRIME int, IS_TRAFFIC int, NEIGHBORHOOD_ID string, OFFENSE_CATEGORY_ID string, OFFENSE_CATEGORY_NAME string, OFFENSE_CODE int,OFFENSE_CODE_EXTENSION int, OFFENSE_TYPE_ID string, OFFENSE_TYPE_NAME string,,REPORTED_DATE date) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/PROG8450/hive/denver-crime/';

-- Question-1 : List out all the crime category and count of crime for it for Denver city.

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count from crime 
where IS_CRIME=1 
group by OFFENSE_CATEGORY_NAME 
sort by Count desc;

-- Question-2 : List out top most five crime in Denver city.

select OFFENSE_CATEGORY_NAME, count(*) as Count from crime 
where IS_CRIME=1 
group by OFFENSE_CATEGORY_NAME 
SORT BY Count desc 
limit 5;

-- Question-3 : No. of Crime in Denver city per year.

select YEAR(FIRST_OCCURRENCE_DATE) AS Year, count(*) as Count from crime 
group by YEAR(FIRST_OCCURRENCE_DATE) 
SORT BY Year desc;

-- Question-4 : List out all crime Category  and its count per year for Denver City.

select OFFENSE_CATEGORY_NAME as Category,YEAR(FIRST_OCCURRENCE_DATE) AS Year, count(*) as Count from crime 
where IS_CRIME=1 
group by OFFENSE_CATEGORY_NAME,YEAR(FIRST_OCCURRENCE_DATE) 
SORT BY Year desc;

-- Question-5 : In which area of Denver city most crime happen from 2015  to 2020. (Dataset contains record from 2015 to 2020)

select INCIDENT_ADDRESS as Address, count(*) as Count from crime 
where INCIDENT_ADDRESS IS NOT NULL  
group by INCIDENT_ADDRESS 
sort by Count desc 
limit 10;

-- Question-6 : List out all the crime By Day of Week for denver city

-- For Saturday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 7 then 'Saturday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 7 
group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  
SORT BY Count desc;

-- For Friday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 6 then 'Friday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 6 group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  SORT BY Count desc;

-- For Thursday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 5 then 'Thursday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 5 
group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  
SORT BY Count desc;

-- For Wednesday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 4 then 'Wednesday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 4 
group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  
SORT BY Count desc;

-- For Tuesday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 3 then 'Tuesday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 3 
group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  
SORT BY Count desc;

-- For Monday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 2 then 'Monday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 2 
group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  
SORT BY Count desc;

-- For Sunday :

select OFFENSE_CATEGORY_NAME as Category, count(*) as Count,
case when pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 1 then 'Sunday' else '' end as WeekDay from crime 
where IS_CRIME=1 and pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1 = 1 
group by OFFENSE_CATEGORY_NAME,pmod(datediff(FIRST_OCCURRENCE_DATE,'1900-01-07'),7) + 1  
SORT BY Count desc;


-- Question-7  All the Crime By District ID for Denver city.

-- For Aggravated Assault Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Aggravated Assault" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Murder Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Murder" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Burglary Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Burglary" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Other Crimes Against Persons:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from ( select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Other Crimes Against Persons" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Theft from Motor Vehicle Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from ( select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count 
from crime where OFFENSE_CATEGORY_NAME="Theft from Motor Vehicle" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Drug & Alcohol:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Drug & Alcohol" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Auto Theft  Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Auto Theft" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Larceny Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Larceny" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For White Collar Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="White Collar Crime " and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Arson Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Arson" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Robbery Crime:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Robbery" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- For Sexual Assault:

select year, sum( if( DISTRICT_ID = 1, Count, 0 ) ) AS District_1, 
sum( if( DISTRICT_ID = 2, Count, 0 ) ) AS District_2, 
sum( if( DISTRICT_ID = 3, Count, 0 ) ) AS District_3, 
sum( if( DISTRICT_ID = 4, Count, 0 ) ) AS District_4, 
sum( if( DISTRICT_ID = 5, Count, 0 ) ) AS District_5, 
sum( if( DISTRICT_ID = 6, Count, 0 ) ) AS District_6, 
sum( if( DISTRICT_ID = 7, Count, 0 ) ) AS District_7 
from (select YEAR(FIRST_OCCURRENCE_DATE) AS Year,DISTRICT_ID, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME="Sexual Assault" and IS_CRIME=1 
group by YEAR(FIRST_OCCURRENCE_DATE), DISTRICT_ID 
sort by Count) t 
group by year;

-- Question-9 Violent Crime (Aggravated Assault, Murder, Sexsual Assault) By Month

select Category, sum( if( year = 2015, Count, 0 ) ) AS Year_2015, 
sum( if( year = 2016, Count, 0 ) ) AS Year_2016,
sum( if( year = 2017, Count, 0 ) ) AS Year_2017,
sum( if( year = 2018, Count, 0 ) ) AS Year_2018,
sum( if( year = 2019, Count, 0 ) ) AS Year_2019,
sum( if( year = 2020, Count, 0 ) ) AS Year_2020 
from ( Select OFFENSE_CATEGORY_NAME as Category, YEAR(FIRST_OCCURRENCE_DATE) as year, count(*) as Count from crime 
where OFFENSE_CATEGORY_NAME in ("Aggravated Assault","Murder","Sexsual Assault") 
group by OFFENSE_CATEGORY_NAME, YEAR(FIRST_OCCURRENCE_DATE) 
sort by Count) t 
group by Category;
