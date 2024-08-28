# Accident Analysis Project in the USA

This project aims to analyze traffic accident data in the United States. Through the extraction, cleaning, analysis and visualization of data, the aim is to identify patterns and trends that can be useful to improve road safety.

## Table of Contents

- [Project Description](#project-description)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Use](#use)
  - [1. Data Extraction](#1-data-extraction)
  - [2. Data Cleaning and Transformation](#2-data-cleaning-and-transformation)
  - [3. Data Visualization](#3-data-visualization)
- [Documentation](#documentation)
- [Contributions](#contributions)
- [License](#license)

## Project Description

The analysis of traffic accidents is crucial to understand the underlying causes and develop strategies to reduce their frequency and severity. This project uses a dataset of accidents in the United States, applying cleaning and transformation techniques to prepare the data for deeper analysis and informative visualizations.

### Goals

- **Data Extraction**: Obtaining the necessary data from open sources.
- **Data Cleaning**: Data cleaning and structuring process to ensure its quality.
- **Data Analysis**: Identification of patterns and trends in accident data.
- **Data Visualization**: Creation of graphs and maps to visually represent the findings of the analysis.

## Project Structure

The project is organized in the following folders and files:

- **data/**: Raw and clean data files.
  - `us_accidents_raw.csv`: Original accident data.
  - `us_accidents_cleaned.csv`: Accident data after cleaning.

- **Docs/**: Project documentation.
  - `Graphics.pdf`: Document with graphics generated from the data.
  - `presentation.pdf`: Presentation of the project.
  - `report.md`: Detailed project report.

- **Notebooks/**: Jupyter notebooks for interactive analysis.
  - `etl_cleaning.ipynb`: Data cleaning and transformation process.
  - `visualizations.ipynb`: Creation of visualizations from the processed data.

- **src/**: Python scripts to automate tasks.
  - `data_cleaning.py`: Script for data cleaning.
  - `db_conexion.py`: Module to manage the connection to the database.
  - `extract.py`: Script for data extraction.
  - `visualization.py`: Tools for creating visualizations.

- **.gitignore**: Files and folders that should not be included in version control.
- **requirements.txt**: List of Python dependencies required to run the project.
- **README.md**: This file.

## Instalation

### Prerequisites

- Python 3.7 or higher
- pip (Python package manager)

### Environment Configuration

1. Clone this repository:

   ```
   git clone https://github.com/tu_usuario/Accidents_usa.git
   cd Accidents_usa
   ```

2. Create a virtual environment and activate it:
    ```
    python -m venv env
    source env/bin/activate # On Windows: env\Scripts\activate
    ```

3. Install the dependencies:
    ```
    pip install -r requirements.txt
    ```

## Use
1. Data Extraction
The extract.py script allows you to extract the necessary data from the specified sources. You can run it with the following command:

```
python src/extract.py
```

2. Data Cleaning and Transformation
To clean and prepare data for analysis, run the data_cleaning.py script or use the etl_cleaning.ipynb notebook:

```
python src/data_cleaning.py
```

Or open the notebook in Jupyter:

```
jupyter notebook Notebooks/etl_cleaning.ipynb
```

3. Data Visualization
To generate visualizations, you can use the visualization.py script or the visualizations.ipynb notebook:

```
python src/visualization.py
```

Or open the notebook in Jupyter:

```
jupyter notebook Notebooks/visualizations.ipynb
```

## Documentation
Detailed project documentation, including graphs and the final report, is located in the Docs/ folder.

- **Graphics.pdf**: Contains graphics that illustrate the main findings.
- **presentation.pdf**: Presentation of the project.
- **report.md**: Complete report in markdown format.


## Contributions
Contributions are welcome. If you would like to contribute, follow these steps:

Fork the project.
Create a new branch (git checkout -b feature/new-feature).
Make your changes and commit (git commit -am 'Add new functionality').
Upload your changes (git push origin feature/new-feature).
Open a Pull Request.

## License
This project is licensed under the MIT License. See the LICENSE file for more information.

## Contactanos
- **Names** : Juan Manuel Lopez Rodriguez / William Alejandro Botelo Florez
- **Email** : [juan_m.lopez_r@uao.edu.co](mailto:tu-email@ejemplo.com)
              [william.botero@uao.edu.co](mailto:tu-email@ejemplo.com)
- **Project Link**: [https://github.com/willyoung21/Accidents_usa](https://github.com/tu-usuario/WorkShop_01)