Instalación y Ejecución del Pipeline de CO₂ Este tutorial describe los pasos necesarios para instalar, configurar y ejecutar el proyecto de pipeline de datos en Python que simula la ingesta, limpieza, almacenamiento y reporte de lecturas de CO₂.

1. Requisitos del Sistema
Asegúrate de tener instalado lo siguiente:

Python: Versión 3.8 o superior.

Git: Para clonar el repositorio.

2. Configuración del Ambiente
Sigue estos pasos para obtener el código y preparar el entorno de Python:

A. Clonar el Repositorio Abre tu terminal o símbolo del sistema y ejecuta:

Bash

git clone [URL_DE_TU_REPOSITORIO] cd [nombre-del-repositorio] B. Crear un Entorno Virtual (Recomendado) Es una buena práctica aislar las dependencias del proyecto:

Bash

Crea el entorno virtual (usando venv)
python -m venv venv

Activa el entorno virtual
En Windows:
.\venv\Scripts\activate

En macOS/Linux:
source venv/bin/activate C. Instalar Dependencias Tu pipeline utiliza bibliotecas para el manejo de datos, bases de datos y formatos Parquet (requiere pandas y pyarrow).

Bash

Usando pip, instala las bibliotecas necesarias:
pip install pandas pyarrow

3. Estructura y Ejecución
La arquitectura del proyecto está diseñada para ser ejecutada por el script Main.py, el cual orquesta todos los módulos (LecturasLog, Ingest, Cleaning, Storage, Report).

A. Estructura de Directorios El pipeline generará automáticamente la siguiente estructura de directorios y archivos:

. ├── project/ │ ├── Cleaning.py │ ├── Ingest.py │ ├── LecturasLog.py │ ├── Storage.py │ └── report.py ├── output/ # Creada automáticamente al inicio │ ├── data_lake/ # Data Lake (archivos .parquet particionados) │ ├── quarantine.log # Outliers de CO2 │ └── co2_reporte.md # Reporte final generado ├── Data/ # Creada automáticamente al inicio │ ├── lecturas.log # El archivo de insumo (4500 registros) │ └── ingestion_checkpoint.txt └── Main.py # Orquestador B. Ejecución del Pipeline Asegúrate de que tu entorno virtual esté activado y ejecuta el script principal:

Bash

python Main.py El script:

Generará el archivo de insumo (lecturas.log).

Simulará la ingesta por lotes (15 registros por lote).

Aplicará la limpieza y enviará los outliers a cuarentena.

Almacenará los datos limpios en SQLite (con idempotencia) y en Parquet particionado.

Al finalizar, generará el reporte final (output/co2_reporte.md).

4. Limpieza
Para restablecer el ambiente y ejecutar el pipeline desde cero, puedes eliminar manualmente las carpetas output/ y Data/. El script Main.py también realiza una limpieza automática (setup()) cada vez que se ejecuta.
