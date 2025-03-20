# Data-Architecture

## Local ETL
	local etl like system using pandas & postgres
## Instructions

Create schema view on postrges db
```sql

create view  my_tbls_schema as  	
with tmp as (SELECT     *
 FROM
    information_schema.columns where table_schema='insert_my_db_name_here'
	 order by table_name,ordinal_position)
	 SELECT     table_name, STRING_AGG(column_name, ',') A
 FROM
   tmp where table_schema='public'
	group by 1  
```