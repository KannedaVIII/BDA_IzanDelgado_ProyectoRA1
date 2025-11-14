import sqlite3
import pandas as pd
from datetime import datetime
import os

# --- CONFIGURACIÓN DE RUTAS ---
SQLITE_DB = os.path.join('project', 'events.sqlite') # Lee la BD desde 'project'
# El reporte se guardará en la carpeta 'output'
REPORT_FILE = os.path.join('output', 'co2_reporte.md') 
# ------------------------------

SQLITE_TABLE = 'co2_readings'
ALERT_THRESHOLD_PPM = 1500
ALERT_WINDOW_MIN = 5

def generate_report():
    if not os.path.exists(SQLITE_DB):
        print(f" Error: Base de datos '{SQLITE_DB}' no encontrada. Ejecute la ingesta y almacenamiento primero.")
        return

    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] REPORTE: Generando informe analítico.")

    conn = sqlite3.connect(SQLITE_DB)
    df = pd.read_sql_query(f"SELECT ts, aula, co2_ppm FROM {SQLITE_TABLE} ORDER BY ts", conn)
    conn.close()
    
    if df.empty:
        print("   -> No hay datos válidos en la base de datos para generar el reporte.")
        return

    df['ts'] = pd.to_datetime(df['ts'])
    df = df.set_index('ts')

    report_ppm_minute = generate_ppm_minute_table(df)
    report_alerts = generate_alerts_table(df)
    checkpoint_explanation = get_checkpoint_explanation()
    
    report_markdown = f"""
# Reporte de Calidad del Aire (CO2)

## 1. Tabla "PPM por Minuto" (Media)

| Minuto | Aula | Media CO2 (ppm) | Total Lecturas |
|:---|:---|:---:|:---|
{report_ppm_minute}

## 2. Tabla de Alertas (Media 5 min > {ALERT_THRESHOLD_PPM} ppm)

| Ventana de 5 min | Aula | Media CO2 (ppm) | Estado |
|:---|:---|:---:|:---|
{report_alerts}

## 3. Explicación de Reanudación con Checkpoint

### **¿Qué es un Checkpoint?**
Un checkpoint es un **marcador de progreso** guardado en un archivo (`ingestion_checkpoint.txt`) que indica el último registro de `lecturas.log` que el consumidor (`Ingest.py`) procesó con **éxito** antes de escribir en el almacenamiento.

### **¿Cómo permite la Reanudación?**
1.  **Fallo/Interrupción:** Si la ingesta se detiene (por un error o manual).
2.  **Reanudación:** Al reiniciar `Ingest.py`, primero **lee** el valor del checkpoint.
3.  **Posicionamiento:** El script empieza a leer `lecturas.log` **exactamente desde esa línea**, garantizando que:
    * No se pierda **ningún dato** generado posteriormente.
    * No se reprocesen datos ya almacenados (trabajo conjunto con la **idempotencia**).

---
"""
    
    # ----------------------------------------------------
    # LÓGICA DE ESCRITURA EN ARCHIVO
    # ----------------------------------------------------
    try:
        with open(REPORT_FILE, 'w', encoding='utf-8') as f:
            f.write(report_markdown)
        print(f"   ->  Reporte Markdown generado en: {REPORT_FILE}")
    except Exception as e:
        print(f" Error al escribir el archivo de reporte: {e}")
    # ----------------------------------------------------

def generate_ppm_minute_table(df):
    df_minute = df.groupby(['aula', pd.Grouper(freq='min')]).agg(
        co2_mean=('co2_ppm', 'mean'),
        count=('co2_ppm', 'count')
    ).reset_index()
    
    markdown_rows = []
    for _, row in df_minute.iterrows():
        minute_str = row['ts'].strftime('%Y-%m-%d %H:%M')
        markdown_rows.append(f"| {minute_str} | {row['aula']} | {int(row['co2_mean'])} | {row['count']} |")
        
    return "\n".join(markdown_rows)

def generate_alerts_table(df):
    df_5min = df['co2_ppm'].rolling(f'{ALERT_WINDOW_MIN}min', closed='left').mean().resample(f'{ALERT_WINDOW_MIN}min').mean().dropna().reset_index()
    
    markdown_rows = []
    
    for _, row in df_5min.iterrows():
        mean_ppm = row['co2_ppm']
        status = "🟢 NORMAL"
        
        if mean_ppm > ALERT_THRESHOLD_PPM:
            status = " ALERTA (VENTILACIÓN URGENTE)"
            
        window_str = row['ts'].strftime('%Y-%m-%d %H:%M')
        
        markdown_rows.append(f"| {window_str} | aula_101 | {int(mean_ppm)} | {status} |")

    if not markdown_rows:
        return "| N/A | N/A | N/A | No se detectaron alertas |"
        
    return "\n".join(markdown_rows)

def get_checkpoint_explanation():
    # Nota: El archivo checkpoint.txt está en la carpeta 'output'
    CHECKPOINT_FILE = os.path.join('output', 'ingestion_checkpoint.txt') 
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            last_checkpoint = f.read().strip()
            
        return f"""
El último valor de checkpoint registrado es: **{last_checkpoint}**. 
Esto significa que el sistema puede reanudar la lectura del log desde el registro número **{last_checkpoint}** si es necesario.
"""
    else:
        return "El archivo de checkpoint (`ingestion_checkpoint.txt`) no ha sido creado o el proceso de ingesta se ha completado/reiniciado. No hay un punto de reanudación activo."