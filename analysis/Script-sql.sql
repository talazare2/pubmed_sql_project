-- Работа с таблицей gen_table
CREATE INDEX pmid
ON gen_table(pmid);

-- Создадим ключи для таблицы, определим типы данных + автоматическая индексация
PRAGMA foreign_keys=off;
BEGIN TRANSACTION;

ALTER TABLE gen_table RENAME TO old_table;

CREATE TABLE new_gen_table ( 
	pmid TEXT NOT NULL,
	title TEXT,
	journ_issn VARCHAR(20),
	journ_title TEXT,
	journ_volume VARCHAR(40),
	journ_issue VARCHAR(40),
	year YEAR,
	month MONTH,
	day DAY,
	country VARCHAR(100));

INSERT INTO new_gen_table SELECT * FROM old_table;

COMMIT;

PRAGMA foreign_keys=on;

DROP TABLE old_table;

-- Общее число цитирований в БД
SELECT COUNT(pmid) AS total_number FROM new_gen_table ngt ;

-- Для удобства вывода в данных в виде pandas df для дальнейшего анализа, сохраним витрины данных

-- Для удобства работы с годами создадим индекс по year
CREATE INDEX year_id
ON new_gen_table(year);

-- удалим данные без даты
DELETE FROM new_gen_table 
WHERE year = '-';

-- Число цитирований по годам
CREATE VIEW IF NOT EXISTS citations_by_year AS
SELECT year,
	   COUNT(pmid) as cit_per_year	
FROM new_gen_table ngt 
GROUP BY year
ORDER BY year ASC;

-- Число цитирований по странам
CREATE VIEW IF NOT EXISTS citations_by_country AS
SELECT country,
	   COUNT(pmid) as cit_per_country	
FROM new_gen_table ngt 
GROUP BY country
ORDER BY cit_per_country DESC;

-- Как изменялось количество публикаций по годам в разных странах
-- для скорости исполнения используем подзапросы
CREATE VIEW IF NOT EXISTS country_year_citations AS
SELECT country,
   	year,
   	COUNT(pmid) as cit_country_year,  
   	DENSE_RANK() OVER(PARTITION BY year ORDER BY COUNT(pmid) DESC) AS rating
FROM new_gen_table ngt 
GROUP BY country, year
ORDER BY year, rating ASC

-- Тоже самое для среднего в десятилетнем интервале 
-- Число цитирований по интервалам
CREATE VIEW IF NOT EXISTS country_10year_citations AS
SELECT * FROM (
	SELECT country,
	  	   year,
	   	   citation_over_10_years,
		   ROW_NUMBER() OVER(PARTITION BY year ORDER BY citation_over_10_years DESC) AS rating
	FROM (
		SELECT country,
			   year,
		 	   SUM(cit_country_year) 
		 	   OVER(PARTITION BY country ORDER BY year groups 
		 	   BETWEEN 0 PRECEDING AND 9 FOLLOWING) AS citation_over_10_years
		FROM country_year_citations cyc)
	WHERE year%10 = 0)
WHERE rating <= 10
ORDER BY year DESC, citation_over_10_years DESC

-- Как изменялось часло публикаций по годам для топ-5 стран последнего десятилетия
CREATE VIEW IF NOT EXISTS top_5_countries_over_years AS
SELECT country,
	   year,
	   COUNT(pmid) as cit_country_year
FROM new_gen_table ngt 
WHERE country IN ('United States', 'England', 'Switzerland', 'Netherlands', 'Germany')
GROUP BY country, year

-- Работа с таблицей mesh

-- Для удобства работы с годами создадим индекс по descr
CREATE INDEX descr
ON new_mesh_table(descr);

-- Выделим дб для исследований на людях (индекс публикаций):
CREATE TABLE human_researches_pmid AS
SELECT pmid,
	   descr
FROM new_mesh_table nmt 
WHERE descr = 'Humans'

CREATE INDEX pmid
ON human_researches_pmid(pmid)

-- Выделим дб для исследований на людях (индекс публикаций + год издания):
CREATE TABLE human_researches_year AS
SELECT hrp.pmid,
	   ngt.year,
	   ngt.country
FROM new_gen_table ngt 
INNER JOIN human_researches_pmid hrp
ON hrp.pmid = ngt.pmid

-- Выделим дб для исследований на людях (Вся необходимая информация):
CREATE TABLE human_researches_mesh AS
SELECT nmt.pmid,
	   nmt.descr,
	   nmt.qualif,
	   nmt.major,
	   hry.year,
	   hry.country
FROM new_mesh_table nmt 
INNER JOIN human_researches_year hry 
ON nmt.pmid = hry.pmid

-- Исследования возрастных когорт
CREATE VIEW age_cogorts AS
SELECT descr,
	   year
FROM human_researches_mesh
WHERE descr IN ('Adult', 'Middle Aged', 'Aged', 'Adolescent', 'Child', 
				'Young Adult', 'Aged, 80 and over', 'Child, Preschool', 
				'Infant', 'Infant, Newborn')

--Предаставление распределения исследований для мужчин и женщин по годам
CREATE VIEW sex_cogorts AS
SELECT descr,
	   year
FROM human_researches_mesh
WHERE descr IN ('Female', 'Male')

--Представление квалификаторов
CREATE VIEW qualifcators AS
SELECT SUBSTR(qualif, 1, INSTR(qualif, ',') - 1) AS qual ,
	   count(pmid) as citation_qty
FROM human_researches_mesh hrm 
GROUP BY qual
ORDER BY citation_qty DESC;

--Создание таблицы по частоте испольхования дескрипторов
CREATE TABLE main_topic_db AS
SELECT descr,
	   count(pmid) as citation_qty,
	   SUBSTR(qualif, 1, INSTR(qualif, ',') - 1) AS qual
FROM human_researches_mesh hrm
WHERE major = 'Y'
GROUP BY descr
ORDER BY citation_qty DESC

--Представления по классификаторам
CREATE VIEW diagnost AS
SELECT descr,
	   citation_qty
FROM main_topic_db
WHERE qual = 'diagnosis'
ORDER BY citation_qty DESC

CREATE VIEW genetic AS
SELECT descr,
	   citation_qty
FROM main_topic_db
WHERE qual = 'genetics'
ORDER BY citation_qty DESC

CREATE VIEW drug_therapy AS
SELECT descr,
	   citation_qty
FROM main_topic_db
WHERE qual = 'drug therapy'
ORDER BY citation_qty DESC

-- Создаем таблицу по дескрипторам и годам
CREATE TABLE main_topic_year AS
SELECT descr,
	   year,
	   count(pmid) as citation_qty,
	   SUBSTR(qualif, 1, INSTR(qualif, ',') - 1) AS qual
FROM human_researches_mesh hrm
WHERE major = 'Y'
GROUP BY year, descr
ORDER BY year DESC

CREATE TABLE desease_10year_citations AS
SELECT * FROM (
	SELECT descr,
	  	   year,
	   	   citation_over_10_years,
		   ROW_NUMBER() OVER(PARTITION BY year ORDER BY citation_over_10_years DESC) AS rating
	FROM (
		SELECT descr,
			   year,
		 	   SUM(citation_qty) 
		 	   OVER(PARTITION BY descr ORDER BY year groups BETWEEN 0 PRECEDING AND 9 FOLLOWING) AS citation_over_10_years
		FROM main_topic_year mty)
	WHERE year%10 = 0)
WHERE rating <= 10
ORDER BY year DESC, citation_over_10_years DESC

CREATE VIEW top_3_desease_decade AS
SELECT * FROM desease_10year_citations dyc 
WHERE rating <= 3

CREATE TABLE drug_theraoy AS
SELECT pmid,
	   descr
FROM human_researches_mesh hrm 
WHERE year > 1990 AND descr IN ('Hypertension', 'Asthma', 'Parkinson Desease')

CREATE TABLE chem_desease AS
SELECT dt.pmid,
	   dt.descr,
	   ct.name_of_subst
FROM drug_theraoy dt
INNER JOIN chem_table ct 
ON dt.pmid = ct.pmid

CREATE VIEW chem_therapy AS
SELECT descr,
	   name_of_subst, 
	   count(name_of_subst) AS cnt
FROM chem_desease 
GROUP BY descr, name_of_subst
ORDER BY cnt DESC 


