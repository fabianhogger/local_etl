import time
import os
import pandas as pd
import psycopg2
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import uuid
from datetime import datetime
from psycopg2.extras import execute_values

# Classe di gestione degli eventi
class FileHandler(FileSystemEventHandler):
    def __init__(self):
        try:
            # Configurazione della connessione al database
            self.conn = psycopg2.connect(
                dbname="postgres",
                user="postgres",
                password="123456789",
                host="localhost",
                port="5432"
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            print(f"Errore di connessione al database: {e}")
            self.conn = None
            self.cursor = None

    def on_created(self, event):
        # Verifica se il file creato è un CSV
        if event.is_directory:
            return
        if event.src_path.endswith(".txt"):
            print(f"Nuovo file trovato: {event.src_path}")
            self.load_csv_to_db(event.src_path)

    # Funzione per caricare i dati nel database
    def load_csv_to_db(self, csv_file_path):
        if self.conn is None or self.cursor is None:
            print("Connessione al database non disponibile.")
            return
        
        try:
            df = pd.read_csv(csv_file_path, sep=",", skiprows=1)
            table_name ,columns=get_table(csv_file_path)
            insert_query = f"""
                INSERT INTO {table_name} ({','.join(columns)})
                VALUES %s
            """
            batch_size = 10000  # Number of rows per batch
            num_batches = len(df) // batch_size + 1

            for i in range(num_batches):
                batch_df = df.iloc[i * batch_size:(i + 1) * batch_size]
                execute_values(self.    cursor, insert_query, batch_df.values)
                self.conn.commit()            

            print(f"Dati da {csv_file_path} inseriti correttamente nella tabella.")
        except Exception as e:
            self.conn.rollback()  # Se c'è un errore, rollback della transazione
            print(f"Errore durante l'inserimento da {csv_file_path}: {e}")
def get_table(csv_file_path):
    table_name="project_table"
    columns=["insert_in_raw_timestamp", "row_number", "nome", "cognome", "anni"]
    return table_name,columns

# Funzione per avviare il monitoraggio
def start_directory_monitor(directory_path):
    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, directory_path, recursive=False)
    observer.start()
    print(f"Inizia a monitorare la directory: {directory_path}")
    
    try:
        while True:
            time.sleep(1)  # Dormi per evitare l'uso eccessivo della CPU
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    # Percorso della directory da monitorare
    directory_to_monitor = r"/home/fabian/Documents/data_eng/local_etl/landing"
    # Avvia il monitoraggio della directory
    start_directory_monitor(directory_to_monitor)