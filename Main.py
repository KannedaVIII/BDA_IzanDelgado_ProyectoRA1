import project.LecturasLog as LecturasLog
import project.Ingest as Ingest
import project.Cleaning as Cleaning
import project.Storage as Storage
import project.report as report
import time
import os
import shutil

SIMULATION_CYCLES = 5 
CYCLE_INTERVAL = Ingest.INTERVAL_SECONDS 
OUTPUT_DIR = 'output'

def setup():
    print("--- SETUP: Inicializando Ambiente ---")
    
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
        print(f"    -> Creada la carpeta de salida: '{OUTPUT_DIR}'")
    
    # Se mantienen los archivos de CHECKPOINT y QUARANTINE para permitir la ingesta incremental.
    
    # Mantenemos esta limpieza para el Data Lake Parquet
    # Si deseas mantener los datos Parquet anteriores, COMENTA las siguientes 2 líneas.
    if os.path.exists(Storage.PARQUET_DIR):
        shutil.rmtree(Storage.PARQUET_DIR)

    # **IMPORTANTE: overwrite=False para añadir datos y continuar el log**
    LecturasLog.generate_log_file(overwrite=False)

def main():
    setup()
    
    print("\n---  INICIANDO TUBERÍA DE PROCESAMIENTO (INSTANTÁNEO) ---")
    
    # Leer el log completo (con los datos nuevos añadidos)
    with open(Ingest.LOG_FILE_PATH, 'r') as f:
        all_lines = f.readlines()
        
    total_records = len(all_lines)
    
    print(f"\n[ORQUESTADOR] Simulando ingesta por lotes hasta completar {total_records} registros.")
    
    while True:
        # Cargar el índice del último registro procesado.
        current_index = Ingest.load_checkpoint()
        
        if current_index >= total_records:
            print("\n Todos los registros han sido procesados por la Ingesta.")
            break
            
        start_idx = current_index
        end_idx = min(current_index + Ingest.BATCH_SIZE, total_records)
        
        batch_lines = all_lines[start_idx:end_idx]
        if not batch_lines: break
        
        current_batch_data = Ingest.process_batch(batch_lines)
        
        valid_data = Cleaning.clean_data(current_batch_data)
        
        Storage.store_batch(valid_data)
        
        # Guardar el nuevo punto de control
        Ingest.save_checkpoint(end_idx)
        print(f"    -> [ORQUESTADOR] Checkpoint avanzado a registro {end_idx}.")
        
        
    print("\n--- GENERANDO REPORTE FINAL ---")
    report.generate_report()
    
    print("\n TUBERÍA DE DATOS COMPLETA.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nPrograma terminado por el usuario.")