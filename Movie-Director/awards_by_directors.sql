
-- Create database
create database movie_awards;

-- Use Database
use movie_awards;

-- Cretae External table
create external table if not exists award_director (director_name String, ceremony String, year int, category String, outcome string, original_lang char(2))
row format DELIMITED
fields terminated by ','
STORED AS TEXTFILE
location '/user/k2lsuthar_gmail/dataset'
tblproperties ("skip.header.line.count"="1");

-- Analysis

-- Directors who were nominated and have won awards in the year 2011

select distinct(director_name) as Director from award_director where lower(outcome) in ('nominated','won') and year=2011;

-- Award categories available in the Berlin International Film Festival

select distinct(category) from award_director where lower(ceremony) = lower('Berlin International Film Festival');

-- Directors who won awards for making movies in French

select distinct(director_name) as Director from award_director where lower(outcome) = 'won' and original_lang = 'fr';

-- Directors who have won awards more than 10 times.

select director_name, count(director_name) from award_director 
where lower(outcome) = 'won' 
group by director_name 
having count(director_name) >10
order by 2 desc;
