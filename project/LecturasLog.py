# LecturasLog.py
# Genera lecturas de CO2 en formato NDJSON y las guarda en la carpeta 'Data'.

import json
import time
import random
from datetime import datetime, timedelta
import os

# --- CONFIGURACIÓN ---
OUTPUT_DIR = 'Data'
LOG_FILE_NAME = 'lecturas.log'
LOG_FILE_PATH = os.path.join(OUTPUT_DIR, LOG_FILE_NAME)
NUM_RECORDS = 4500
AULA_ID = 'aula_101'
CO2_BASE_PPM = 600

def generate_co2_record(timestamp, aula_id, base_ppm):
    """
    Genera un único registro de lectura de CO2 en formato diccionario (ts, aula, co2_ppm).
    Simula acumulación y picos aleatorios de CO2.
    """
    
    # Simulación de nivel basado en el tiempo base y ruido
    noise = random.randint(-50, 50)
    co2_level = int(base_ppm + noise)
    
    # Simula una ocupación ligera
    co2_level += random.randint(0, 100)
    
    co2_level = max(400, co2_level) # Asegurar mínimo 400 ppm
    
    # Simular un pico de CO2 muy alto para que la limpieza tenga algo que hacer
    if random.random() < 0.005:
        co2_level = random.randint(5500, 7000)

    record = {
        "ts": timestamp.isoformat(),
        "aula": aula_id,
        "co2_ppm": co2_level
    }
    return record

def get_last_timestamp(file_path):
    """ Busca la marca de tiempo del último registro en un archivo NDJSON para continuar la simulación. """
    last_line = None
    try:
        # Abrir en modo 'r' para buscar la última línea
        with open(file_path, 'r') as f:
            for line in f:
                last_line = line
        if last_line:
            # Parsear la última línea como JSON
            record = json.loads(last_line.strip())
            return datetime.fromisoformat(record['ts'])
    except Exception:
        return None

def generate_log_file(overwrite=True):
    """
    Genera el archivo lecturas.log con más de 4000 datos de CO2 en formato NDJSON,
    continuando el tiempo si se está añadiendo (overwrite=False).
    """
    
    # 1. Crear la carpeta 'Data' si no existe
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"    -> Creada la carpeta de salida: '{OUTPUT_DIR}'")
        
    mode = 'w' if overwrite else 'a'
    
    # 2. Determinar el tiempo de inicio de la simulación
    last_ts = None
    if not overwrite and os.path.exists(LOG_FILE_PATH):
        last_ts = get_last_timestamp(LOG_FILE_PATH)

    if last_ts:
        # Si se encontró un último registro, la simulación continúa 15 segundos después.
        start_time = last_ts
        print(f"    -> MODO APPEND: Continuando simulación desde: {start_time.isoformat()}")
    else:
        # Si es la primera ejecución o se sobrescribe, iniciar un nuevo historial
        start_time = datetime.now() - timedelta(hours=NUM_RECORDS * 15 / 3600)
        print("    -> MODO OVERWRITE/NUEVO: Iniciando nuevo historial de simulación.")

    current_time = start_time

    print(f"--- 🏭 Iniciando Generador de Insumos: {LOG_FILE_PATH} ---")
    
    try:
        with open(LOG_FILE_PATH, mode) as f:
            for i in range(NUM_RECORDS):
                # Generar una lectura cada 15 segundos
                current_time += timedelta(seconds=15)
                
                record = generate_co2_record(current_time, AULA_ID, CO2_BASE_PPM)
                
                # Convertir a NDJSON (JSON + Salto de línea)
                json_line = json.dumps(record)
                f.write(json_line + '\n')
                
                # Mostrar progreso de vez en cuando
                if i % 500 == 0 and i > 0:
                    print(f"    -> Generados {i} / {NUM_RECORDS} registros...")

        print(f" Generador completado. {NUM_RECORDS} registros escritos en '{LOG_FILE_PATH}'.")
        print(f"    -> Formato: NDJSON (ts, aula, co2_ppm)")

    except Exception as e:
        print(f" Error al generar el archivo de log: {e}")

# NOTA: La función 'generate_log_file' NO debe ser llamada aquí.
# Será llamada por tu script principal (main.py).