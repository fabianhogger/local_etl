import time
import os
import pandas as pd
import psycopg2
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import uuid
from datetime import datetime
from psycopg2.extras import execute_values
import logging
from dotenv import load_dotenv

logging.basicConfig(filename='log/myapp.log',level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Classe di gestione degli eventi
class FileHandler(FileSystemEventHandler):
    def __init__(self):
        try:
            # Configurazione della connessione al database
            self.conn = psycopg2.connect(
                dbname=os.getenv("db_name"),
                user=os.getenv("db_user"),
                password=os.getenv("db_password"),
                host=os.getenv("db_host"),
                port=os.getenv("db_port")
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Errore di connessione al database: {e}")
            self.conn = None
            self.cursor = None
        #get tbl metadata present in db
        self.cursor.execute(f"""SELECT  table_name, STRING_AGG(column_name, ',') cols  FROM  information_schema.columns where table_schema='public'    group by 1 """)
        metadata=self.cursor.fetchall()
        self.schema=dict(metadata)
        
        self.conn.commit()
    def close_connection(self):
        if self.conn:
            self.cursor.close()
            self.conn.close()
            logging.info("Database connection closed")

    def on_created(self, event):
        # Verifica se il file creato è un CSV
        if event.is_directory:
            return
        if event.src_path.endswith(".csv"):
            print(f"Nuovo file trovato: {event.src_path}")
            self.load_csv_to_db(event.src_path)

    # Funzione per caricare i dati nel database
    def load_csv_to_db(self, csv_file_path):
        if self.conn is None or self.cursor is None:
            logging.error("Database connection not available.")
            return
        
        try:
            df = pd.read_csv(csv_file_path, sep=",", skiprows=1, lineterminator="\n")
            #technical fieds
            df.insert(0,"insert_in_raw_timestamp",datetime.now())  
            df.insert(1,"row_number",range(0,len(df)))  
            table_name ,columns=self.get_table(csv_file_path)
            if table_name is None:
                logging.error(f"Skipping {csv_file_path}: Table not found.")
                return
            insert_query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES %s
            """
            batch_size = 5000  # Number of rows per batch
 
            for i in range(0, len(df), batch_size):
                batch_df = df.iloc[i : i + batch_size]
                try:
                    execute_values(self.cursor, insert_query, batch_df.values)
                    self.conn.commit()
                    logging.info(f"Inserted batch {i // batch_size + 1} into {table_name}")
                except psycopg2.Error as e:
                    self.conn.rollback()
                    logging.error(f"Database insert error: {e}")
        except Exception as e:
            logging.error(f"Error processing {csv_file_path}: {e}")

    def get_table(self,csv_file_path):
        try:
            dir_name=csv_file_path.replace(os.getenv("base_dir")+"/","").split('/')[-2]
            columns = self.schema.get(dir_name)

            if columns is None:
                raise KeyError(f"Table '{dir_name}' not found in schema metadata.")
                return dir_name, None
        except KeyError as e:
            logging.error(f"Error: {e}")
            return None, None
        return dir_name,columns

# Funzione per avviare il monitoraggio
def start_directory_monitor(directory_path):
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, directory_path, recursive=True)
    observer.start()
    print(f"Inizia a monitorare la directory: {directory_path}")
    
    try:
        while True:
            time.sleep(1)  # Dormi per evitare l'uso eccessivo della CPU
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    load_dotenv()
    # Percorso della directory da monitorare
    directory_to_monitor = os.getenv("base_dir")
    # Avvia il monitoraggio della directory
    start_directory_monitor(directory_to_monitor)