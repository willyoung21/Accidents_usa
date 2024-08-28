# Proyecto de Análisis de Accidentes en EE.UU.

Este proyecto tiene como objetivo analizar datos de accidentes de tráfico en Estados Unidos. A través de la extracción, limpieza, análisis y visualización de los datos, se busca identificar patrones y tendencias que puedan ser útiles para mejorar la seguridad vial.

## Tabla de Contenidos

- [Descripción del Proyecto](#descripción-del-proyecto)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Instalación](#instalación)
- [Uso](#uso)
  - [1. Extracción de Datos](#1-extracción-de-datos)
  - [2. Limpieza y Transformación de Datos](#2-limpieza-y-transformación-de-datos)
  - [3. Visualización de Datos](#3-visualización-de-datos)
- [Documentación](#documentación)
- [Contribuciones](#contribuciones)
- [Licencia](#licencia)

## Descripción del Proyecto

El análisis de accidentes de tráfico es crucial para entender las causas subyacentes y desarrollar estrategias para reducir su frecuencia y gravedad. Este proyecto utiliza un conjunto de datos de accidentes en Estados Unidos, aplicando técnicas de limpieza y transformación para preparar los datos para análisis más profundos y visualizaciones informativas.

### Objetivos

- **Extracción de Datos**: Obtención de los datos necesarios desde fuentes abiertas.
- **Limpieza de Datos**: Proceso de depuración y estructuración de los datos para asegurar su calidad.
- **Análisis de Datos**: Identificación de patrones y tendencias en los datos de accidentes.
- **Visualización de Datos**: Creación de gráficos y mapas para representar visualmente los hallazgos del análisis.

## Estructura del Proyecto

El proyecto está organizado en las siguientes carpetas y archivos:

- **data/**: Archivos de datos brutos y limpios.
  - `us_accidents_raw.csv`: Datos originales de accidentes.
  - `us_accidents_cleaned.csv`: Datos de accidentes después de la limpieza.

- **Docs/**: Documentación del proyecto.
  - `Graphics.pdf`: Documento con gráficos generados a partir de los datos.
  - `presentation.pdf`: Presentación del proyecto.
  - `report.md`: Informe detallado del proyecto.

- **Notebooks/**: Notebooks de Jupyter para análisis interactivo.
  - `etl_cleaning.ipynb`: Proceso de limpieza y transformación de los datos.
  - `visualizations.ipynb`: Creación de visualizaciones a partir de los datos procesados.

- **src/**: Scripts de Python para automatizar tareas.
  - `data_cleaning.py`: Script para la limpieza de los datos.
  - `db_conexion.py`: Módulo para gestionar la conexión a la base de datos.
  - `extract.py`: Script para la extracción de datos.
  - `visualization.py`: Herramientas para la creación de visualizaciones.

- **.gitignore**: Archivos y carpetas que no se deben incluir en el control de versiones.
- **requirements.txt**: Lista de dependencias de Python necesarias para ejecutar el proyecto.

## Instalación

### Requisitos Previos

- Python 3.7 o superior
- pip (gestor de paquetes de Python)

### Configuración del Entorno

1. Clona este repositorio:

   ```
   git clone https://github.com/tu_usuario/Accidents_usa.git
   cd Accidents_usa
   ```

2. Crea un entorno virtual y actívalo:

