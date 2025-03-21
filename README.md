# Data-Architecture

## Local ETL
	local etl like system using pandas & postgres
## Instructions



Create schema view on postrges db for identifying table
```sql

create view  my_tbls_schema as  	
with tmp as (SELECT     *
 FROM
    information_schema.columns where table_schema='insert_my_db_name_here'
	 order by table_name,ordinal_position)
	 SELECT     table_name, STRING_AGG(column_name, ',')  cols
 FROM
   tmp where table_schema='insert_my_db_name_here'
	group by 1  
```

Set up you .env file like this
```
db_name="yourdbname"
db_user="yourdbuser"
db_password="password"
db_host="localhost"
db_port="5432"
db="yourdb"
base_dir="base dir"
```