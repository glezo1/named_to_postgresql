DROP TABLE IF EXISTS dns_log;
CREATE TABLE dns_log
(
	id						SERIAL			NOT		NULL,
	log_time				TIMESTAMP		NOT		NULL,
	client_ip_string		varchar(16)		NOT		NULL,
	client_ip_int			int8			NOT		NULL,
	client_question			varchar(255)	NOT		NULL,
	server_ip_string		varchar(16)		NOT		NULL,
	server_ip_int			int8			NOT		NULL,
	server_answer_ip_string	varchar(16)		DEFAULT	NULL,
	server_answer_ip_int	int8			DEFAULT	NULL,
	PRIMARY KEY(id),
	UNIQUE(log_time,client_ip_string,client_question)
);
-- ----------------------------------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------
-- IGNORE THIS IF YOU DON'T HAVE A GEOIP DATABASE AS COOL AS MINE--------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------
-- ----------------------------------------------------------------------------------------------------------------------
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

DROP SERVER IF EXISTS localhost_geoip CASCADE;
CREATE SERVER localhost_geoip FOREIGN DATA WRAPPER postgres_fdw OPTIONS 
(	host	'localhost'
	,dbname	'geoip'
	,port	'5432'
);

CREATE USER MAPPING FOR CURRENT_USER
SERVER localhost_geoip
OPTIONS (user 'postgres', password 'XXXXXXXXXXXXXXXXXXXX');

DROP SCHEMA IF EXISTS fdw_geoip;
CREATE SCHEMA fdw_geoip;

IMPORT FOREIGN SCHEMA geoip FROM SERVER localhost_geoip INTO fdw_geoip;

--importing fdw into a db-living table increases performance CRAZY FAST
DROP TABLE IF EXISTS geoip_board CASCADE;
CREATE TABLE geoip_board AS SELECT * FROM fdw_geoip.board;
CREATE INDEX i_geoip_board_range	ON geoip_board(network_begin_int,network_end_int);

DROP TABLE IF EXISTS public.dns_log_geoip;
CREATE TABLE public.dns_log_geoip
(
	id								INT				NOT		NULL,
	log_time						TIMESTAMP		NOT		NULL,
	client_ip_string				varchar(16)		NOT		NULL,
	client_ip_int					int8			NOT		NULL,
	client_ip_asnumber				FLOAT			DEFAULT	NULL,
	client_ip_asname				VARCHAR(512)	DEFAULT	NULL,
	client_ip_continent				VARCHAR(512)	DEFAULT	NULL,
	client_ip_country				VARCHAR(512)	DEFAULT	NULL,
	client_ip_country_subdivison_1	VARCHAR(512)	DEFAULT	NULL,
	client_ip_country_subdivison_2	VARCHAR(512)	DEFAULT	NULL,
	client_ip_city					VARCHAR(512)	DEFAULT	NULL,
	client_question					varchar(255)	NOT		NULL,
	server_ip_string				varchar(16)		NOT		NULL,
	server_ip_int					int8			NOT		NULL,
	server_answer_ip_string			varchar(16)		DEFAULT	NULL,
	server_answer_ip_int			int8			DEFAULT	NULL,
	PRIMARY KEY(id)
);

DROP FUNCTION IF EXISTS f_trigger_insert_dns_log() CASCADE;
CREATE OR REPLACE FUNCTION f_trigger_insert_dns_log()
RETURNS TRIGGER
LANGUAGE PLPGSQL
AS
$$
BEGIN
	INSERT INTO public.dns_log_geoip
	SELECT	A.id
			,A.log_time
			,A.client_ip_string
			,A.client_ip_int
			,B.asnumber
			,B.asname
			,B.continent_name
			,B.country_name
			,B.subdivision_1_name
			,B.subdivision_2_name
			,B.city_name
			,A.client_question
			,A.server_ip_string
			,A.server_ip_int
			,A.server_answer_ip_string
			,A.server_answer_ip_int
	FROM	public.dns_log					AS A
			LEFT  JOIN public.geoip_board	AS B	ON	A.client_ip_int	BETWEEN B.network_begin_int AND B.network_end_int
	WHERE	A.id	=	NEW.id;
	RETURN NEW;
END;
$$


CREATE TRIGGER t_insert_dns_log
AFTER INSERT
ON dns_log
FOR EACH ROW
EXECUTE PROCEDURE f_trigger_insert_dns_log();

DROP VIEW IF EXISTS public.v_dns_all_details;
CREATE OR REPLACE VIEW public.v_dns_all_details AS
WITH
count_of_separators AS
(
	SELECT		id
				,log_time
				,client_ip_string
				,client_ip_int
				,client_ip_asnumber
				,client_ip_asname
				,client_ip_continent
				,client_ip_country
				,client_ip_country_subdivison_1
				,client_ip_country_subdivison_2
				,client_ip_city
				,client_question
				,(CHAR_LENGTH(client_question) - CHAR_LENGTH(REPLACE(client_question, '.', ''))) / CHAR_LENGTH('.')		AS count_of_separators
				,server_ip_string
				,server_ip_int
				,server_answer_ip_string
				,server_answer_ip_int
	FROM		public.dns_log_geoip
)
,tld_and_domain AS
(
	SELECT	id
			,log_time
			,client_ip_string
			,client_ip_int
			,client_ip_asnumber
			,client_ip_asname
			,client_ip_continent
			,client_ip_country
			,client_ip_country_subdivison_1
			,client_ip_country_subdivison_2
			,client_ip_city
			,client_question
			,count_of_separators
			,CASE	WHEN count_of_separators=0	THEN	client_question
					ELSE								SPLIT_PART(client_question,'.',count_of_separators+1)
			END AS client_question_tld
			,CASE	WHEN count_of_separators=0	THEN	NULL
					ELSE								SPLIT_PART(client_question,'.',count_of_separators)
			END AS client_question_domain
			,server_ip_string
			,server_ip_int
			,server_answer_ip_string
			,server_answer_ip_int
	FROM	count_of_separators
)
SELECT	id
		,log_time
		,client_ip_string
		,client_ip_int
		,client_ip_asnumber
		,client_ip_asname
		,client_ip_continent
		,client_ip_country
		,client_ip_country_subdivison_1
		,client_ip_country_subdivison_2
		,client_ip_city
		,client_question
		,client_question_tld
		,client_question_domain
		,TRIM(TRAILING '.' FROM SUBSTRING(client_question FROM 1 FOR CHAR_LENGTH(client_question)-(CHAR_LENGTH(client_question_domain)+1+CHAR_LENGTH(client_question_tld))))	AS client_question_subdomains
		,server_ip_string
		,server_ip_int
		,server_answer_ip_string
		,server_answer_ip_int
FROM	tld_and_domain
;

