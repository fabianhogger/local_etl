import pandas as pd
from dotenv import load_dotenv
import os 
import psycopg2
from sqlalchemy import create_engine


class Transfomation():
	def __init__(self):
		load_dotenv()
		dbname=os.getenv("db_name")
		user=os.getenv("db_user")
		password=os.getenv("db_password")
		host=os.getenv("db_host")
		port=os.getenv("db_port")
		engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
		self.engine=engine

	def load_from_db(self,table_name):
		with self.engine.connect() as conn:
			df=pd.read_sql_table(table_name=table_name,con=conn)
			return df

	def transform(self,df):
		df['anni'].apply(lambda x:int(x))
		return df

	def write_to_db(self,df,table_name):
		 df.to_sql(table_name, self.engine,if_exists='append',index=False)
	
	def close(self):
		self.engine.dispose()

if __name__ == "__main__":
	Transformation_obj=Transfomation()
	df=Transformation_obj.load_from_db("project_table")
	tranformed=Transformation_obj.transform(df)
	Transformation_obj.write_to_db(df,"project_table")
	Transformation_obj.close()