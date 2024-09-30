import pandas as pd

file_path = 'data/API_data.csv' 
data = pd.read_csv(file_path)

pd.set_option('display.max_columns', None)

data['crash_date'] = pd.to_datetime(data['crash_date'])

data_filtered = data[data['crash_date'].dt.year >= 2021]

data_filtered['crash_date'] = data_filtered['crash_date'].dt.date
# Borrar columnas
data_filtered = data_filtered.drop(['vehicle_type_code_5','contributing_factor_vehicle_5',
                                    'vehicle_type_code_4','contributing_factor_vehicle_4',
                                    'vehicle_type_code_3','contributing_factor_vehicle_3',
                                    'cross_street_name'], axis=1)

print("\nDatos filtrados y ordenados:")
data_filtered = data_filtered.dropna()
print(data_filtered.isnull().sum())
print(data_filtered.duplicated())
data_filtered
data_filtered.to_csv('data/API_data.csv', index=False, encoding='utf-8')
print("Archivo limpiado correctamente")