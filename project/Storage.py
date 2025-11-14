import sqlite3
import pandas as pd
from datetime import datetime
import os

SQLITE_DB = os.path.join('project', 'events.sqlite')
PARQUET_DIR = os.path.join('output', 'data_lake', 'co2_events')
SQLITE_TABLE = 'co2_readings'

def store_batch(batch_data: list):
    if not batch_data:
        print("   -> Lote de datos vacío, omitiendo Almacenamiento.")
        return
        
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] ALMACENAMIENTO: Iniciando guardado de {len(batch_data)} registros.")

    store_sqlite(batch_data)
    store_parquet(batch_data)

def store_sqlite(data: list):
    conn = sqlite3.connect(SQLITE_DB)
    cursor = conn.cursor()
    
    stored_at = datetime.now().isoformat()
    
    records_to_insert = [
        (record['ts'], record['aula'], record['co2_ppm'], stored_at)
        for record in data
    ]
    
    try:
        cursor.executemany(f"""
            INSERT OR IGNORE INTO {SQLITE_TABLE} 
            (ts, aula, co2_ppm, stored_at) 
            VALUES (?, ?, ?, ?)
        """, records_to_insert)
        conn.commit()
        
        rows_inserted = cursor.rowcount
        print(f"   -> SQLite: Insertados/Actualizados {rows_inserted} registros (Idempotencia aplicada).")

    except Exception as e:
        print(f" Error al escribir en SQLite: {e}")
    finally:
        conn.close()

def store_parquet(data: list):
    
    if not os.path.exists(PARQUET_DIR):
        os.makedirs(PARQUET_DIR, exist_ok=True)

    df = pd.DataFrame(data)
    
    df['ts_dt'] = pd.to_datetime(df['ts'])
    
    df['year'] = df['ts_dt'].dt.strftime('%Y')
    df['month'] = df['ts_dt'].dt.strftime('%m')
    df['day'] = df['ts_dt'].dt.strftime('%d')
    df['hour'] = df['ts_dt'].dt.strftime('%H')
    
    try:
        df.to_parquet(
            PARQUET_DIR, 
            index=False, 
            partition_cols=['year', 'month', 'day', 'hour'],
            engine='pyarrow'
        )
        print(f"   -> Parquet: Guardado lote en Data Lake particionado (Ruta base: {PARQUET_DIR}).")
        
    except Exception as e:
        print(f" Error al escribir en Parquet: {e}")

