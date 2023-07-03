import geopandas as gpd 
import pandas as pd 

import matplotlib.pyplot as plt 
import os

# Define rutas
FILE_PATH = os.getcwd()
DATA_PATH = os.path.join(FILE_PATH, "datos")
KDE_DATA_PATH = os.path.join(FILE_PATH, "datos","kde")

densidad_unidades = 25
KDE_FILE_PATH = os.path.join(KDE_DATA_PATH,f"kde_{densidad_unidades}.geojson")
COL_GEO_FILE_PATH = os.path.join(DATA_PATH, "colonias_geometria.geojson")
COL_FILE_PATH = os.path.join(DATA_PATH, "coloniascdmx.csv")

# Carga datos
kde_buffer = gpd.read_file(KDE_FILE_PATH)
colonias_geo = gpd.read_file(COL_GEO_FILE_PATH)
colonias = pd.read_csv(COL_FILE_PATH)

# Obten las colonias dentro de la densidad definida
colonias__prioriza_nombres = colonias_geo[colonias_geo["geometry"].within(kde_buffer["geometry"].values[0])]["nombre"].to_list()
