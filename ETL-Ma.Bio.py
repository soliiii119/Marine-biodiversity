import pandas as pd
import numpy as np

# -------- EXTRACCIÓN --------
def extract(csv_path):
    try:
        df = pd.read_csv(csv_path)
        print(f" Extracción exitosa: {len(df)} registros encontrados.")
        return df
    except Exception as e:
        print(f" Error en la extracción: {e}")
        return pd.DataFrame()

# -------- TRANSFORMACIÓN --------
def transform(df):
    try:      
        # Verificar otras columnas necesarias
        required_columns = ['temperature_c', 'salinity_psu', 'species', 'station_id']
        for col in required_columns:
            if col not in df.columns:
                print(f" Error: No se encontró la columna '{col}' en el DataFrame.")
                return pd.DataFrame()
            # Convertir a numérico si es necesario
            if col in ['temperature_c', 'salinity_psu']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
            
        # Eliminar filas donde ph esté fuera del rango [5, 9]
        df_filtered = df[(df['ph'] >= 5) & (df['ph'] <= 9)]
        
        if df_filtered.empty:
            print(" Advertencia: No hay datos con pH entre 5 y 9.")
            return pd.DataFrame()       
    
         # Eliminar filas con datos nulos en columnas críticas
        df_filtered = df_filtered.dropna(subset=['temperature_c', 'salinity_psu', 'ph'])
       
        if df_filtered.empty:
            print(" Advertencia: Todos los datos tienen valores nulos en columnas críticas.")
            return pd.DataFrame()
         
        print(f" Transformación exitosa: {len(df_filtered)} grupos generados.")
        return df_filtered
    except Exception as e:
        print(f" Error en la transformación: {e}")
        import traceback
        traceback.print_exc()
        return pd.DataFrame()

# -------- CARGA --------
def load(df, output_path):
    try:
        if df.empty:
            print(" Error: No hay datos para cargar.")
            return False
        df.to_csv(output_path, index=False)
        print(f" Carga exitosa: Archivo guardado en {output_path}.")
        return True
    except Exception as e:
        print(f" Error en la carga: {e}")
        return False

# -------- ETL --------
def run_etl(input_csv, output_csv):
    print("Iniciando proceso ETL...")
    data = extract(input_csv)
    if data.empty:
        print("No se encontraron datos para procesar.")
        return False
    transformed_data = transform(data)
    if transformed_data.empty:
        print("No se generaron datos transformados.")
        return False
    success = load(transformed_data, output_csv)
    if success:
        print("Proceso ETL completado con éxito.")
        return True
    else:
        print("Proceso ETL falló en la fase de carga.")
        return False

if __name__ == "__main__":
    input_csv = "marine_biodiversity_data.csv"
    output_csv = "marine_biodiversity_summary.csv"
    run_etl(input_csv, output_csv)