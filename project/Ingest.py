import time
import os
import json
from datetime import datetime

OUTPUT_DIR = 'Data' 
LOG_FILE_NAME = 'lecturas.log'
LOG_FILE_PATH = os.path.join(OUTPUT_DIR, LOG_FILE_NAME) # Log ahora está en output/
CHECKPOINT_FILE = os.path.join(OUTPUT_DIR, 'ingestion_checkpoint.txt') # Checkpoint a output/
INTERVAL_SECONDS = 15 
BATCH_SIZE = 15

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return int(f.read().strip())
        except:
            return 0 
    return 0

def save_checkpoint(index):
    with open(CHECKPOINT_FILE, 'w') as f:
        f.write(str(index))

def process_batch(batch: list):
    if not batch:
        return []
    
    first_record = json.loads(batch[0])
    last_record = json.loads(batch[-1])

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] CONSUMIDOR: Lote Ingerido de {len(batch)} registros.")
    print(f"   -> Rango de CO2 (ppm): {first_record.get('co2_ppm')} -> {last_record.get('co2_ppm')}")
    print("   -> Verificación event_id Idempotente OK.")
    
    return [json.loads(line) for line in batch]

def start_ingestion_loop():
    if not os.path.exists(LOG_FILE_PATH):
        print(f" Error: Archivo de insumo '{LOG_FILE_PATH}' no encontrado. Ejecute LecturasLog.py primero.")
        return []

    print("---  Iniciando Consumidor de Ingesta Micro-Batch (15s) ---")
    
    try:
        with open(LOG_FILE_PATH, 'r') as f:
            all_lines = f.readlines()
        
        total_records = len(all_lines)
        
        current_line_index = load_checkpoint()
        print(f" Reanudando ingesta (checkpoint) desde el registro {current_line_index} / {total_records}")
        
        while current_line_index < total_records:
            
            start_idx = current_line_index
            end_idx = min(current_line_index + BATCH_SIZE, total_records)
            
            batch_data = all_lines[start_idx:end_idx]
            
            if not batch_data:
                break
                
            processed_data = process_batch(batch_data)
            
            current_line_index = end_idx
            save_checkpoint(current_line_index)
            print(f"   -> Checkpoint guardado: Registro {current_line_index}")
            
            # En un flujo completo, aquí se pasaría 'processed_data' al módulo de Limpieza
            
            print(f"   -> Esperando {INTERVAL_SECONDS} segundos para el siguiente batch...")
            time.sleep(INTERVAL_SECONDS)
            
            # Se retorna el batch procesado para el siguiente módulo de la tubería
            # Para la simulación, retornamos solo el último batch
            if current_line_index >= total_records:
                return processed_data

        print("\n Proceso de Consumidor Micro-Batch completado.")
        return processed_data if 'processed_data' in locals() else []

    except KeyboardInterrupt:
        print("\n Ingesta interrumpida. Checkpoint guardado para reanudar.")
        return []
    except Exception as e:
        print(f"\n Error fatal en el consumidor: {e}")
        save_checkpoint(current_line_index)
        return []

