# Cleaning.py

import json
from datetime import datetime
import os

LOWER_BOUND = 300
UPPER_BOUND = 5000
QUARANTINE_FILE = os.path.join('output', 'quarantine.log')

def clean_data(batch_data: list):
    """
    Recibe un lote de datos y aplica la lógica de limpieza:
    Outliers a cuarentena (co2_ppm < 300 O co2_ppm > 5000).
    Retorna los datos válidos y una lista de datos de cuarentena.
    """
    
    valid_data = []
    quarantine_data = []
    
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] LIMPIEZA: Iniciando proceso de outliers.")

    for record in batch_data:
        co2_ppm = record.get("co2_ppm", 0)
        
        # Lógica de detección de Outlier
        is_outlier = co2_ppm < LOWER_BOUND or co2_ppm > UPPER_BOUND
        
        if is_outlier:
            # Si es un outlier, lo marcamos y lo enviamos a cuarentena
            record['reason'] = f'Outlier: {co2_ppm} ppm fuera del rango [{LOWER_BOUND}, {UPPER_BOUND}]'
            quarantine_data.append(record)
        else:
            # Si es válido, pasa al siguiente módulo (Almacenamiento)
            valid_data.append(record)

    # Almacenar los outliers en un archivo de cuarentena (simulación de 'cuarentena')
    if quarantine_data:
        save_quarantine(quarantine_data)
        
    print(f"   -> Lote de {len(batch_data)} registros procesados.")
    print(f"   -> Válidos para Almacenamiento: {len(valid_data)}")
    print(f"   -> Registros en Cuarentena: {len(quarantine_data)}")
    
    # Retornamos solo los datos limpios y válidos
    return valid_data

def save_quarantine(data: list):
    """
    Escribe los datos de cuarentena en un archivo NDJSON separado.
    """
    try:
        # Usamos 'a' (append) para añadir al final del archivo
        with open(QUARANTINE_FILE, 'a') as f:
            for record in data:
                f.write(json.dumps(record) + '\n')
        print(f"   ->  {len(data)} outliers escritos en '{QUARANTINE_FILE}' para revisión.")
    except Exception as e:
        print(f" Error al escribir el archivo de cuarentena: {e}")

# Si se ejecuta directamente (no debería pasar si se llama desde Main)
