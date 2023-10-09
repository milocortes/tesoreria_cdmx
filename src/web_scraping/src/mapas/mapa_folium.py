### Junta todos los datos de hoteles/aribnb/booking y denue 


"""
Created on Mon Jul  3 11:54:44 2023

@author: fernandasobrino
"""
import pandas as pd 
from pathlib import Path 
import glob
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import folium
import numpy as np
import re
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.neighbors import BallTree, KDTree
import jellyfish
from concurrent.futures import ThreadPoolExecutor
import itertools

path_aribnb = Path('/Users/fernandasobrino/Documents/Tesorereria/colonia_1615')
path_booking = Path('/Users/fernandasobrino/Documents/Tesorereria/booking_cdmx')
path_hoteles = Path('/Users/fernandasobrino/Documents/Tesorereria/hotelespuntocom')
path = Path('/Users/fernandasobrino/Documents/Tesorereria')





# CDMX
cdmx = gpd.read_file(path/'colonias_geometria.geojson')
bounds = cdmx.total_bounds



## LIMPIAR AIRBNB 
archivos_airbnb = path_aribnb.glob('*.csv')
airbnb_dta = pd.concat([pd.read_csv(f) for f in archivos_airbnb], ignore_index=True)
airbnb_dta = airbnb_dta[airbnb_dta.name.notnull()]

airbnb_dta['property_id'] = [re.findall(r'/rooms/(?:plus/)?(\d+)', url)[0] for url in 
                               airbnb_dta.url] 
airbnb_dta.drop_duplicates(subset = ['property_id','lat','lng'], inplace = True) 
tipo_aribnb = {'departamento':['apartment','loft'],
               'casa':['condo','place','home', 'guesthouse','vacation',
                       'cabin','hut', 'dome','chalet','townhouse','casa',
                       'villa', 'tiny', 'earthen', 'cottage','ranch','campsite',
                       'treehouse','nature', 'barn', 'houseboat', 'boat',
                       'yurt','farm','holiday','castle'],
               'cuarto':['room','guest','shared', 'bungalow', 'tower'],
               'hotel':['hotel', 'boutique', 'bed','hostel',
                        'aparthotel','resort','pension'],
               'otros':['shipping', 'camper/rv', 'tent','train', 'island',
                        'cave','nan']}
def LimpiarAirbnb(tipo):
    tipo = str(tipo).lower().split()[0]
    real_type = ''
    for element in tipo_aribnb:
        if tipo in tipo_aribnb[element]:
            real_type = element
    return real_type
airbnb_dta['tipo'] = airbnb_dta['name'].apply(LimpiarAirbnb)    

def LimpiarCuartos(beds):
    bed = np.nan
    if 'bed' in str(beds):
        bed = beds.split()[0]
    return bed
airbnb_dta['camas'] = airbnb_dta['rooms'].apply(LimpiarCuartos)        
airbnb_dta['camas'] = ['1' if (pd.isna(cama) and tipo in ['cuarto','hotel']) else cama
                         for cama, tipo in zip(airbnb_dta.camas, airbnb_dta.tipo)]  
airbnb_dta['bedrooms'] = [1 if bed is np.nan or bed == 0 else bed for bed in 
                            airbnb_dta.bedrooms]
airbnb_dta['bedrooms'] = [1 if bed is np.nan else bed for bed in 
                            airbnb_dta.bedrooms]
airbnb_dta.bedrooms.fillna(1,inplace = True)
monedas = {'HKD':0.12769587, 'TWD':0.032159858, 'MXN':0.058635363,'COP':0.00024147869,
           'zł':0.24501856, 'SEK':0.092341206,'₺': 0.038461893,'₽': 0.011018397,
           'NOK': 0.093966986, 'CHF': 1.1146735, 'R$':0.20657099, 'S/': 0.20193284023668637, 
           '¥': 0.0069218008, '₩': 0.00077226686, '₪': 0.2699522 , '€': 1.0882822,
           'Rp': 0.000066303195, '₡': 0.0018358301, 'CLP': 0.0012543981, 
           'Ft': 0.0028539312, '฿': 0.028571305, 'Kč':0.045656116,
           'UYU': 0.026653931, 'MAD': 0.10247023, '￥':0.12008113283582089,
           '₱': 0.017996175, 'lei': 0.040488343, 'DKK': 0.14573806,
           'JMD': 0.0064633212, 'Rs': 0.012184037}


def limpiar_precio(pp):
    pp = pp.replace(',','')
    pattern = r"(\d+,*\d*)" 
    price = int(re.findall(pattern, pp)[0])
    for moneda in monedas:
        if moneda in pp:
            price  = price*monedas[moneda]
    return price

airbnb_dta['precio_dls'] = airbnb_dta['price'].apply(limpiar_precio)
airbnb_dta['precio_mx'] = np.round(airbnb_dta['precio_dls']/0.058635363,2)
airbnb_dta = airbnb_dta[['name', 'tipo', 'camas','bedrooms', 'precio_mx', 'lat', 'lng']]
airbnb_dta['descripcion'] = ['Tipo: ' + str(tipo) + '\n' + 'Camas: ' + str(camas) + 
                               '\n' + 'Cuartos: ' + str(bedrooms) + '\n' + 
                               'Precio x noche: ' + str(precio_mx) for tipo, camas, bedrooms, precio_mx
                               in zip(airbnb_dta.tipo, airbnb_dta.camas, airbnb_dta.bedrooms,airbnb_dta.precio_mx)]
# quitar cosas fuera de la cdmx: 
airbnb_dta['no_drop'] = [1 if (bounds[1] <= lat <= bounds[3]) and 
                              (bounds[0] <= lng <= bounds[2]) else 0 
                              for lat,lng in zip(airbnb_dta['lat'],
                                                 airbnb_dta['lng'])]  
airbnb_dta = airbnb_dta[airbnb_dta.no_drop == 1]  
airbnb_dta['geometry'] = airbnb_dta.apply(lambda x: Point((x.lng, x.lat)), axis = 1)
airbnb_crs = {'init': 'epsg:4326'}
airbnb_geo = gpd.GeoDataFrame(airbnb_dta, 
                                crs = airbnb_crs, 
                                geometry = airbnb_dta.geometry)
airbnb_inside = gpd.sjoin(airbnb_geo, cdmx, how = 'inner', op = 'within')
airbnb_inside.reset_index(inplace = True)




## BOOKING
files_booking = path_booking.glob('*.csv')
booking_dta = pd.concat([pd.read_csv(f) for f in files_booking], ignore_index=True)
booking_dta = booking_dta[booking_dta.name.notnull()]
booking_dta.drop_duplicates(subset = ['name','lat','lng'], inplace = True)
booking_dta.drop_duplicates(subset = ['lat','lng'], inplace = True)
tipo_booking = {'casa':['home', 'villa', 'house'],
                'departamento':['apartment', 'studio', 'loft', 'apartamento'],
                'hostal': ['bathroom', 'dormitory', 'toilet', 'bath', 'family',
                           'bunk', 'budget', 'toilet']}

def LimpiarBooking(tipo):
    tipo = str(tipo).lower()
    real_type = 'hotel'
    for element in tipo_booking:
        for posible_type in tipo_booking[element]:
            if posible_type in tipo:
                real_type = element
    return real_type
booking_dta['tipo'] = booking_dta['tipo'].apply(LimpiarBooking)   
def CuartosBooking(descripcion):
    match = re.search(r'(\d+)\s*bedroom', descripcion)
    if match:
        return int(match.group(1))
    else:
        return 1
booking_dta['bedrooms'] = booking_dta['descripcion_1'].apply(CuartosBooking)    


booking_dta['precio_dls'] = booking_dta['precio'].apply(limpiar_precio)/2
booking_dta['precio_mx'] = np.round(booking_dta['precio_dls']/0.058635363,2)


# quitar cosas fuera de la cdmx: 
booking_dta['no_drop'] = [1 if (bounds[1] <= lat <= bounds[3]) and 
                              (bounds[0] <= lng <= bounds[2]) else 0 
                              for lat,lng in zip(booking_dta['lat'],
                                                 booking_dta['lng'])]  
booking_dta = booking_dta[booking_dta.no_drop == 1]  
booking_dta['geometry'] = booking_dta.apply(lambda x: Point((x.lng, x.lat)), axis = 1)
booking_crs = {'init': 'epsg:4326'}
booking_geo = gpd.GeoDataFrame(booking_dta, 
                                crs = booking_crs, 
                                geometry = booking_dta.geometry)
booking_inside = gpd.sjoin(booking_geo, cdmx, how = 'inner', op = 'within')
booking_inside.reset_index(inplace = True)

booking_inside = booking_inside[['name', 'tipo', 'taxes', 'lat', 'lng', 'bedrooms',
                                 'precio_mx', 'geometry', 'index_right', 'id',
                                 'nombre', 'entidad', 'cve_alc', 'alcaldia', 'cve_col', 'secc_com',
                                 'secc_par']]







## HOTELES.COM
archivos_hoteles = path_hoteles.glob('*.csv')
hoteles_dta = pd.concat([pd.read_csv(f) for f in archivos_hoteles], ignore_index=True)
hoteles_dta.drop_duplicates(subset = ['nombre'], inplace = True)
def number_rooms(text):
    match = re.search(r'(\d+)\s+rooms', text)
    if match: 
        rooms = int(match.group(1))
    else:
        rooms = np.nan
    return rooms
hoteles_dta['rooms'] = hoteles_dta['descripcion'].apply(lambda x: number_rooms(x))
hoteles_dta = hoteles_dta[['latitude', 'longitude', 'nombre', 'address', 'rooms']]
hoteles_dta.rename(columns = {'nombre':'name', 'latitude':'lat', 'longitude':'lng'}, 
                   inplace = True)
hoteles_dta['no_drop'] = [1 if (bounds[1] <= lat <= bounds[3]) and 
                              (bounds[0] <= lng <= bounds[2]) else 0 
                              for lat,lng in zip(hoteles_dta['lat'],
                                                 hoteles_dta['lng'])]  
hoteles_dta = hoteles_dta[hoteles_dta.no_drop == 1] 
hoteles_dta['name_lower'] = hoteles_dta['name'].str.lower().str.strip()
booking_inside['name_lower'] = booking_inside['name'].str.lower().str.strip()


## MATCH DE ESTO CON BOOKING.COM 
match_booking_hoteles = pd.merge(booking_inside, hoteles_dta, how = 'outer', 
                                 on = ['name_lower'])
no_rooms = match_booking_hoteles[match_booking_hoteles.rooms.isna()]
with_rooms = match_booking_hoteles[match_booking_hoteles.rooms.notnull()]
rooms_matches = with_rooms[with_rooms.precio_mx.notnull()]
missing_rooms = with_rooms[with_rooms.precio_mx.isna()]


missing_rooms = missing_rooms[['name_lower', 'lat_y',
'lng_y', 'name_y', 'address', 'rooms']]
no_rooms = no_rooms[['name_x', 'tipo', 'taxes', 'lat_x', 'lng_x', 'bedrooms', 'precio_mx',
       'geometry', 'index_right', 'id', 'nombre', 'entidad', 'cve_alc',
       'alcaldia', 'cve_col', 'secc_com', 'secc_par', 'name_lower']]


def compare_names(nombre, possible):
    name = nombre.strip().lower()
    distances = []
    for nombre in possible['name_lower']:
        distances.append(jellyfish.levenshtein_distance(name, nombre))
    minimo = pd.Series(distances).idxmin()
    best_match_name = possible.iloc[minimo].name_lower
    best_match_distance = distances[minimo]
    return best_match_name, best_match_distance

def compute_best_matches(missing, possible):
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(compare_names, missing.name_lower, itertools.repeat(possible)))
    return results


def find_matches(missing, possible_matches):
    results = compute_best_matches(missing, possible_matches)
    missing['name_match'], missing['best_match_distance'] = zip(*results)
    missing['other_dist'] = [jellyfish.jaro_distance(x,y) for x,y in 
                                   zip(missing.name_lower, missing.name_match)]
    missing_matched = missing[(missing.best_match_distance <= 2) |
                                    (missing.other_dist >= 0.802)]
    missing_matched = pd.merge(missing_matched, possible_matches, how = 'left', 
                               left_on = 'name_match', right_on = 'name_lower')
    missing_rooms = missing[~((missing.best_match_distance <= 2) |
                                         (missing.other_dist >= 0.802))]
    return missing_matched, missing_rooms
    
    
missing_matched, missing_rooms =  find_matches(missing_rooms, no_rooms)  



missing_matched = missing_matched[['name_x', 'tipo', 'taxes', 'lat_x',
'lng_x', 'bedrooms', 'precio_mx', 'geometry', 'index_right', 'id',
'nombre', 'entidad', 'cve_alc', 'alcaldia', 'cve_col', 'secc_com',
'secc_par', 'name_lower_x', 'lat_y', 'lng_y', 'name_y', 'address', 'rooms']]
missing_matched.rename(columns = {'name_lower_x':'name_lower'}, inplace = True)


rooms_matches = pd.concat([rooms_matches, missing_matched])
rooms_matches.drop_duplicates(subset = ['name_x'], inplace = True)

missing_drop = missing_matched[['name_x', 'tipo']]
missing_drop['drop_row'] = 1
no_rooms = pd.merge(no_rooms, missing_drop, how = 'left', on = ['name_x', 'tipo'])
no_rooms = no_rooms[no_rooms.drop_row != 1]



## intenar encontrar las que faltan: 
missing_matched, missing_rooms =  find_matches(missing_rooms, no_rooms) 
missing_matched = missing_matched[['name_x', 'tipo', 'taxes', 'lat_x',
'lng_x', 'bedrooms', 'precio_mx', 'geometry', 'index_right', 'id',
'nombre', 'entidad', 'cve_alc', 'alcaldia', 'cve_col', 'secc_com',
'secc_par', 'name_lower_x', 'lat_y', 'lng_y', 'name_y', 'address', 'rooms']]
missing_matched.rename(columns = {'name_lower_x':'name_lower'}, inplace = True)    

rooms_matches = pd.concat([rooms_matches, missing_matched])
rooms_matches.drop_duplicates(subset = ['name_x'], inplace = True)

missing_drop = missing_matched[['name_x', 'tipo']]
missing_drop['drop_row'] = 1
no_rooms = pd.merge(no_rooms, missing_drop, how = 'left', on = ['name_x', 'tipo'])
no_rooms = no_rooms[no_rooms.drop_row_y != 1]


## JUNTAR TODO ESTO OTRA VEZ en una sola base: 
rooms_matches.rename(columns = {'name_x':'name', 'lat_x':'lat', 'lng_x':'lng'},
                     inplace = True)    
rooms_matches = rooms_matches[['name', 'tipo', 'taxes', 'lat', 'lng', 'bedrooms', 'precio_mx',
       'geometry', 'index_right', 'id', 'nombre', 'entidad', 'cve_alc',
       'alcaldia', 'cve_col', 'secc_com', 'secc_par', 'rooms']]    
no_rooms.rename(columns = {'name_x':'name', 'lat_x':'lat', 'lng_x':'lng'}, inplace = True)
no_rooms = no_rooms[['name', 'tipo', 'taxes', 'lat', 'lng', 'bedrooms', 'precio_mx',
       'geometry', 'index_right', 'id', 'nombre', 'entidad', 'cve_alc',
       'alcaldia', 'cve_col', 'secc_com', 'secc_par']]
missing_rooms.rename(columns = {'name_y':'name', 'lat_y':'lat', 'lng_y':'lng'}, inplace = True)
missing_rooms = missing_rooms[['name','lat','lng', 'rooms']]
# hacer esto un geopanda 
missing_rooms['geometry'] = missing_rooms.apply(lambda x: Point((x.lng, x.lat)), axis = 1)
missing_crs = {'init': 'epsg:4326'}
missing_geo = gpd.GeoDataFrame(missing_rooms, 
                                crs = missing_crs, 
                                geometry = missing_rooms.geometry)
missing_inside = gpd.sjoin(missing_geo, cdmx, how = 'inner', op = 'within')
missing_inside.reset_index(inplace = True)

all_hotels = pd.concat([rooms_matches, no_rooms, missing_inside])



## DENUE: 
denue_1 = pd.read_csv(path/'Deneu/denue_00_72_1_csv/conjunto_de_datos/denue_inegi_72_1.csv',encoding='latin-1')
denue_2 = pd.read_csv(path/'Deneu/denue_00_72_2_csv/conjunto_de_datos/denue_inegi_72_2.csv', encoding='latin-1')

denue = pd.concat([denue_1, denue_2])
denue['mini_scian'] = denue.codigo_act.astype('string').str[:3]
## 7016: hoteles sin otros servicios integrados
## 5182: hoteles con otros servicios integrados
## 2289: pensiones y casas de huespedes
## 2032: moteles
## 1161: cabañas, villas y similares
## 335: departamenntos y casas amueblados con servicios de hoteleria
## 114: campamento y albergues recreativos 
denue = denue[denue.mini_scian == '721']
denue = denue[denue.cve_ent == 9]
denue = denue[['nom_estab', 'codigo_act', 'per_ocu', 'latitud', 'longitud', 
                'fecha_alta','nombre_act']]

denue['no_drop'] = [1 if (bounds[1] <= lat <= bounds[3]) and 
                              (bounds[0] <= lng <= bounds[2]) else 0 
                              for lat,lng in zip(denue['latitud'],
                                                  denue['longitud'])] 
denue = denue[denue.no_drop == 1]  
denue['geometry'] = denue.apply(lambda x: Point((x.longitud, x.latitud)), axis = 1)
denue_crs = {'init': 'epsg:4326'}
denue_geo = gpd.GeoDataFrame(denue, 
                              crs = denue_crs, 
                              geometry = denue.geometry)
#denue_inside = gpd.sjoin(denue_geo, small_cdmx, how = 'inner', op = 'within')
denue_inside = gpd.sjoin(denue_geo, cdmx, how = 'inner', op = 'within')
denue_inside.reset_index(inplace = True)

denue_inside.rename(columns = {'latitud':'lat', 'longitud':'lng'}, inplace = True)






#### INTERSECCION ENTRE DENUE Y BOOKING.COM 


## Juntar denue con booking.com 
### Sacar los 10 closests neighbors usando distancia euclideana 
all_hotels = all_hotels[all_hotels.lat.notnull()]
all_hotels['name_lower'] = all_hotels['name'].str.lower().str.strip()
denue_inside['name_lower'] = denue_inside['nom_estab'].str.lower().str.strip()


matched_denue, missing_denue =  find_matches(denue_inside, all_hotels)  


# limpiar esto: 
matched_denue.drop_duplicates(subset = ['nom_estab'], inplace = True)
matched_denue.rename(columns = {'lat_y':'lat', 'lng_y':'lng',
                                'geometry_y':'geometry','nombre_y':'nombre',
                                'entidad_y':'entidad','index_right_y':'index_right',
                                'id_y':'id','cve_alc_y':'cve_alc',
                                'alcaldia_y':'alcaldia', 'cve_col_y':'cve_col',
                                'secc_com_y':'secc_com', 'secc_par':'secc_par'}, inplace = True)

matched_denue = matched_denue[[ 'name', 'tipo', 'taxes', 'lat', 'lng',
'bedrooms', 'precio_mx', 'geometry', 'index_right', 'id', 'nombre',
'entidad', 'cve_alc', 'alcaldia', 'cve_col', 'secc_com', 'secc_par_y',
'rooms', 'codigo_act', 'per_ocu', 'fecha_alta', 'nombre_act']]

all_hotels = pd.merge(all_hotels, matched_denue, how = 'left',
                      on = ['name', 'tipo', 'taxes', 'lat', 'lng', 'bedrooms', 'precio_mx',
                             'geometry', 'index_right', 'id', 'nombre', 'entidad', 'cve_alc',
                             'alcaldia', 'cve_col', 'secc_com', 'rooms'])

missing_denue = missing_denue[['index', 'nom_estab', 'codigo_act', 'per_ocu', 'lat', 'lng',
       'fecha_alta', 'nombre_act', 'geometry', 'index_right', 'id',
       'nombre', 'entidad', 'cve_alc', 'alcaldia', 'cve_col', 'secc_com',
       'secc_par']]





## Mapa Feo: 
# # dejar cosas solo en la cdmx (airbnb es tonto y nos esta regresando 
# # propiedades en otras partes del pais :( )
# fig, ax = plt.subplots()
# cdmx.plot(ax = ax, color='white', edgecolor = 'black')
# airbnb_inside.plot(ax = ax, color='red', markersize=5)
# denue_inside.plot(ax = ax, color='blue', markersize=5)    
# booking_inside.plot(ax = ax, color = 'green', markersize=5)


## Mapa interactivo:
    
x_map_center = cdmx['geometry'].centroid.x.mean()
y_map_center = cdmx['geometry'].centroid.y.mean()    
    
m = folium.Map(location=[y_map_center, x_map_center], zoom_start = 13)
folium.GeoJson(cdmx, style_function=lambda feature: {
        'fillColor': '#ffaf00',
        'color': 'orange',
        'weight': 1}).add_to(m)
locations_airbnb = list(zip(airbnb_inside.lat, airbnb_inside.lng))

for i in range(len(locations_airbnb)):
    folium.CircleMarker(location=locations_airbnb[i],radius=2,color="#3186cc",
                        fill_color="#3186cc", 
                        popup="Airbnb: " + "\n" + 
                        "Tipo: " + airbnb_inside.tipo[i] + "\n" + 
                        "Habitaciones: " + str(airbnb_inside.bedrooms[i]) + "\n" + 
                        "Precio: " + str(airbnb_inside.precio_mx[i])).add_to(m)
    
locations_hotels = list(zip(all_hotels.lat, all_hotels.lng))
for i in range(len(locations_hotels)):
    popup_content = ["Hoteles:"]
    popup_content.append("Nombre: " + str(all_hotels.name[i]))
    if not pd.isna(all_hotels.precio_mx[i]):
        popup_content.append("precio: " + str(all_hotels.precio_mx[i]))
    if not pd.isna(all_hotels.rooms[i]):
        popup_content.append("cuartos: " + str(all_hotels.rooms[i]))
    if not pd.isna(all_hotels.nombre_act[i]):
        popup_content.append("SCIAN: " + str(all_hotels.nombre_act[i]))
    if not pd.isna(all_hotels.per_ocu[i]):
        popup_content.append("personal ocupado: " + str(all_hotels.per_ocu[i]))
    popup_string = "\n".join(popup_content)
    folium.CircleMarker(
        location=locations_hotels[i],
        radius=2,
        color="#f21707",
        fill_color="#f21707",
        popup=popup_string
    ).add_to(m)
    
#missing_denue.reset_index(inplace = True)    
    
denue_no_hotels = list(zip(missing_denue.lat, missing_denue.lng))    
for i in range(len(denue_no_hotels)):
    folium.CircleMarker(location=denue_no_hotels[i],
                        radius=2,
                        color="#00AC3E",
                        fill_color="#00AC3E",
                        popup="DENUE: " + "\n" + 
                        "Nombre: " + missing_denue.nom_estab[i] + "\n" + 
                        "SCIAN: " + missing_denue.nombre_act[i] + "\n" + 
                        " personal ocupado : "  + missing_denue.per_ocu[i]).add_to(m)     
    
m.save(path/"prueba2.html")






# ## Graficas de esta madre: 
# colonias_inside['tipo'].value_counts().plot(kind='bar',
#                                     figsize = (14,8),
#                                     title = "Tipos de Alojamiento")  

# sns.histplot(data = colonias_inside[colonias_inside.precio_mx <= 5000], 
#              x = "precio_mx", 
#              kde = True).set(title='Precios de Airbnb <= 5000')

# sns.histplot(data = colonias_inside[colonias_inside.bedrooms <= 6], 
#              x = "bedrooms", 
#              kde = True).set(title='Cuartos <= 6')


# por_alcaldias = colonias_inside.groupby('alcaldia').size().reset_index()
# por_alcaldias.rename(columns = {0:'numero_de_airbnbs'}, inplace = True)
# sns.barplot(data = por_alcaldias, x = 'numero_de_airbnbs', 
#             y = 'alcaldia',
#             order = por_alcaldias.sort_values('numero_de_airbnbs',ascending = False).alcaldia,
#             orient='h')


# denue_inside['per_ocu'].value_counts().plot(kind='bar',
#                                     figsize = (14,8),
#                                     title = "Personas Ocupadas") 
# denue_inside['codigo_act'].value_counts().plot(kind='bar',
#                                     figsize = (14,8),
#                                     title = "Codigo de Actividad") 


# por_alcaldias_d = denue_inside.groupby('alcaldia').size().reset_index()
# por_alcaldias_d.rename(columns = {0:'numero_de_establecimientos'}, inplace = True)
# sns.barplot(data = por_alcaldias_d, x = 'numero_de_establecimientos', 
#             y = 'alcaldia',
#             order = por_alcaldias_d.sort_values('numero_de_establecimientos',ascending = False).alcaldia,
#             orient='h')





# colonias_inside['Fuente'] = 'Airbnb'
# denue_inside['Fuente'] = 'Denue'

# colonias_inside.rename(columns = {'lat':'latitud',
#                                   'lng': 'longitud',
#                                   'nombre': 'colonia'}, inplace = True)
# colonias_inside = colonias_inside[['tipo', 'bedrooms', 'precio_mx', 'latitud', 
#                                    'longitud', 'descripcion', 'geometry',
#                                    'colonia', 'alcaldia','Fuente']]
# denue_inside.rename(columns = {'nombre':'colonia'}, inplace = True)
# denue_inside = denue_inside[['nom_estab', 'codigo_act','per_ocu',
#                              'latitud', 'longitud','geometry',
#                              'colonia','alcaldia','Fuente']]

# todos = pd.concat([colonias_inside, denue_inside])


# por_alcaldia_todo = todos.groupby(['Fuente','alcaldia']).size().reset_index()
# por_alcaldia_todo.rename(columns = {0:'total'}, inplace = True)

# sns.barplot(data = por_alcaldia_todo, x = "total", 
#             y = "alcaldia", hue="Fuente",
#             order = por_alcaldia_todo.sort_values('total',ascending = False).alcaldia)


# todos.to_csv(path/'denue_airbnb_centro.csv')


# airbnb_inside['pagina'] = 'Airbnb'
# booking_inside['pagina'] = 'Booking'

# all_dta = pd.concat([airbnb_inside, booking_inside])


# all_dta = all_dta[['pagina','name', 'tipo', 'bedrooms', 'precio_mx', 'lat', 'lng',
#                     'nombre', 'entidad', 'cve_alc', 'alcaldia', 'cve_col']]


# all_dta.to_csv(path/'datos_booking_airbnb.csv')










