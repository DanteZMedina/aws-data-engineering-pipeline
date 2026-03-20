import os
import requests

# Path de la ruta para la información nueva
DATA_PATH = "/home/ubuntu/nyc_project/data/raw"

os.makedirs(DATA_PATH, exist_ok=True)


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

#Parámetros
years = range(2021, 2027)
months = [f"{i:02d}" for i in range(1, 13)]
types = ["yellow", "green", "fhv"]

# Generar los nombres de los archivos a consumir despues y los descarga y guarda en el path de /data/raw
# yellow_tripdata_2021-01.parquet
for data_type in types:
    for year in years:
        for month in months:

            file_name = f"{data_type}_tripdata_{year}-{month}.parquet"
            url = BASE_URL + file_name
            file_path = os.path.join(DATA_PATH, file_name)

            print(f"\n📥 Descargando: {file_name}")

            try:
                response = requests.get(url)

                if response.status_code == 200:
                    with open(file_path, "wb") as f:
                        f.write(response.content)
                    print(f"✔ Descargado: {file_name}")
                else:
                    print(f"❌ No existe: {file_name}")

            except Exception as e:
                print(f"⚠️ Error con {file_name}: {e}")

