import pandas as pd
import numpy as np 
import matplotlib.pyplot as plt
import os 

# Define directorios
#dir_path = os.path.dirname(os.path.realpath(__file__))
dir_path = os.getcwd()

sources_path = os.path.abspath(os.path.join(dir_path,"..","datos" )) 
save_path = os.path.abspath(os.path.join(dir_path,"..","output" )) 

"""

FUENTES DE INFORMACIÓN

"""


### Encuesta Mensual de Servicios (EMS)
### url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=EMS_EMS_ENTIDAD_IPS_34_ff3a8990-c479-4763-afa9-855ab9421ad0


### Encuesta Mensual de la Industria Manufacturera (EMIM)
### url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=EMIM_EMIM_ENTIDAD_34_79ed5bb2-b394-45dc-a81f-7e79c8b88ab3

### Encuesta Nacional de Empresas Constructoras (ENEC)
### Personal Ocupado ---> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_1_3efb4198-9d3c-49d6-be9e-3e6483e7405a
### Horas Trabajadas ---> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_2_25dd8a47-8c80-4a06-90de-354d340aeeb4
### Remuneraciones -----> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_3_40586f36-2507-4987-934e-c2cc850d616e
### Gastos en la ejecución de obras y servicios ---> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_7_7b598678-7290-43d8-8ba3-d1661b306142
### Ingresos por prestación de servicios ---> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_8_a75b1b00-f9ff-4a97-93bf-0db1c91ab922
### Valor de producción generado por las empresas en la entidad ---> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_10_142188a3-55a6-46c6-865d-a7452402eedf
### Valor de producción generado por las empresas de la entidad ---> url : https://www.inegi.org.mx/app/tabulados/interactivos/?pxq=ENEC_ENEC_ENTIDAD_9_d5eaca78-53de-41c3-a767-03e06b2b50c8

### Encuesta Mensual sobre Empresas Comerciales (EMEC). Base 2013
### url : https://www.inegi.org.mx/contenidos/programas/emec/2013/datosabiertos/emec_mensual_entidad_federativa_csv.zip


"""

PREPROCESAMIENTO

"""


def preprocesa_datos(data_set_name : str, excel_col_index : tuple, time_period : tuple, names_index_row : tuple) -> pd.DataFrame:
    # Define path del archivo
    inegi_data_set_file_path = os.path.join(sources_path, data_set_name)

    # Construye multi-indice de filas
    inegi_data_index = pd.read_excel(inegi_data_set_file_path, skiprows = 4, usecols = excel_col_index).dropna()
    index_row = pd.MultiIndex.from_arrays(np.transpose(inegi_data_index.to_numpy()), names = names_index_row)


    # Construye multi-indice de columnas
    tuples = [(y, f"{m:02d}") for y in range(*time_period) for m in range(1,13)]
    index_col = pd.MultiIndex.from_tuples(tuples, names=["anio", "mes"])

    # Carga datos 
    inegi_data_set = pd.read_excel(inegi_data_set_file_path, skiprows=4).dropna()

    inegi_data_set = pd.DataFrame(inegi_data_set[inegi_data_set.columns[4:]].to_numpy(), index = index_row, columns=index_col).reset_index()
    inegi_data_set = pd.melt(inegi_data_set, id_vars = [ (i,'') for i in names_index_row] )
    inegi_data_set = inegi_data_set.rename(columns = {(i,'') : i for i in names_index_row})

    inegi_data_set["time"] = pd.to_datetime(inegi_data_set["anio"].astype(str) + inegi_data_set["mes"], format='%Y%m')

    return inegi_data_set.drop(columns = ["anio","mes"])


### Encuesta Mensual de la Industria Manufacturera (EMIM)
emim = preprocesa_datos("EMIM_34.xlsx", ("A,D"), (2013, 2024), ("variable", "sector"))
emim = emim.query("value!='-' and value!='ND'")
emim["value"] = emim["value"].astype(float)

### Encuesta Mensual de Servicios (EMS)
ems = preprocesa_datos("EMS_34.xlsx", ("A,C,D"), (2013, 2024), ("variable", "sector","indicador"))
ems = ems.query("indicador == 'Estimación' ").reset_index(drop = True)[["variable", "sector", "time", "value"]]
ems = ems.query("value!='-' and value!='ND'")
ems["value"] = ems["value"].astype(float)


### Encuesta Nacional de Empresas Constructoras (ENEC)
enec_files = ["ENEC_3410.xlsx", "ENEC_342.xlsx", "ENEC_344.xlsx", "ENEC_349.xlsx", "ENEC_3411.xlsx", "ENEC_343.xlsx", "ENEC_348.xlsx"]

enec = []

for enec_file in enec_files:
    enec.append(preprocesa_datos(enec_file, ("B,D"), (2006, 2024), ("variable", "sector")))

enec = pd.concat(enec, ignore_index = True)

enec = enec.query("value!='-' and value!='ND' and value !='I'")
enec["value"] = enec["value"].astype(float)



### Encuesta Mensual sobre Empresas Comerciales (EMEC). Base 2013
emec_file = os.path.join(sources_path, "emec_mensual_entidad_federativa_csv", "conjunto_de_datos", "tr_emec_entidad_federativa_indice_2008_2023.csv")
emec = pd.read_csv(emec_file)
emec = emec.query("ENTIDAD =='Ciudad de México'")
emec = emec.rename( columns = {"K100W" : "Mercancías compradas para su reventa", 
                               "M000W" : "Ingresos totales por suministro de bienes y servicios",
                               "REMUNERACION_MEDIA" : "Remuneraciones medias",
                               "H000W_I000W" : "Personal ocupado total",
                               "J000W" : "Remuneraciones totales",
                               "CODIGO_ACTIVIDAD" : "sector",
                               "ANIO" : "anio",
                               "MES" : "mes"}).drop(columns = ["ENTIDAD", "ESTATUS"])
emec = pd.melt(emec, id_vars = ["sector","anio","mes"])           
emec["mes"] = emec["mes"].apply(lambda x : f"{x:02d}")                    


emec = emec.query("value!='-' and value!='ND' and value !='I'")
emec["value"] = emec["value"].astype(float)

emec["sector"] = emec["sector"].replace({43 : "43 Comercio al por mayor", 46 : "46 Comercio al por menor"})
emec["time"] = pd.to_datetime(emec["anio"].astype(str) + emec["mes"], format='%Y%m')
emec = emec.drop(columns = ["anio","mes"])
emec
encuestas_inegi = pd.concat([emim, ems, enec, emec], ignore_index = True)


#### Hacemos algunas visualizaciones
## EMIM 

emim_personal_ocupado = emim.query(f"variable=='-Personal ocupado total (Número de personas)' and sector != '31-33 Industrias manufactureras'")\
                    .pivot(index = ["variable","time"], columns = "sector", values = "value").reset_index().drop(columns = "variable").set_index("time")
emim_personal_ocupado.plot.area(title = '-Personal ocupado total (Número de personas)')
plt.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))
plt.show()

# Por subsector
for subsector in emim.sector.unique()[1:]:
    emim_valor_produccion = emim.query(f"variable=='-Personal ocupado total (Número de personas)' and sector != '31-33 Industrias manufactureras' and sector =='{subsector}'")\
                    .pivot(index = ["variable","time"], columns = "sector", values = "value").reset_index().drop(columns = "variable").set_index("time")

    emim_valor_produccion.plot()
    plt.show()

# Todas las variables para el sector
for variable in emim.variable.unique():
    emim_variable = emim.query(f"variable=='{variable}' and sector == '31-33 Industrias manufactureras'")\
                    .pivot(index = ["sector","time"], columns = "variable", values = "value").reset_index().drop(columns = "sector").set_index("time")

    title_name = "\n(".join(variable.split(" ("))
    emim_variable.plot(title = f'{title_name} 31-33 Industrias manufactureras', legend=None)
    plt.figsize=(14, 10)
    file_name_save = variable.lower().strip().replace(" ","_").replace("(","").replace(")","").replace("-","")+"_scian_31-33.png"    
    plt.savefig(os.path.join(save_path, file_name_save))
    plt.clf()


## EMS
# Todas las variables para el sector
for variable in ems.variable.unique():
    ems_variable = ems.query(f"variable=='{variable}'")\
                    .pivot(index = ["sector","time"], columns = "variable", values = "value").reset_index().drop(columns = "sector").set_index("time")

    title_name = "\n(".join(variable.split(" ("))

    ems_variable.plot(title = f'{title_name}\n72 Servicios de alojamiento temporal y de preparación de alimentos y bebidas',  legend=None)
    plt.figsize=(14, 10)
    
    file_name_save = variable.lower().strip().replace(" ","_").replace("(","").replace(")","").replace("-","")+"_scian_72.png"    
    #plt.savefig(os.path.join(save_path, file_name_save))
    #plt.clf()
    plt.show()
## ENEC
# Todas las variables para el sector
variables_funcionan = []

for variable in enec.variable.unique():
    try:
        enec_variable = enec.query(f"variable=='{variable}'")\
                        .pivot(index = ["sector","time"], columns = "variable", values = "value").reset_index().drop(columns = "sector").set_index("time")

        title_name = "\n(".join(variable.split(" ("))

        enec_variable.plot(title = f'{title_name} 23 Construcción',  legend=None)

        plt.figsize=(14, 10)

        file_name_save = variable.lower().strip().replace(" ","_").replace("(","").replace(")","").replace("-","")+"_scian_23.png"
        
        plt.savefig(os.path.join(save_path, file_name_save))
        plt.clf()

        variables_funcionan.append(variable)
    except:
        print("no jaló")

### EMEC
for sector in emec.sector.unique():
    for variable in emec.variable.unique():
        emec_variable = emec.query(f"variable=='{variable}' and sector == '{sector}'")\
                        .pivot(index = ["sector","time"], columns = "variable", values = "value").reset_index().drop(columns = "sector").set_index("time")

        
        emec_variable.plot(title = f'{variable}\n{sector}',  legend=None)
        plt.figsize=(14, 10)
        
        file_name_save = variable.lower().strip().replace(" ","_").replace("(","").replace(")","").replace("-","")+"_scian_" + sector[:2] + ".png"
        
        plt.savefig(os.path.join(save_path, file_name_save))
        plt.clf()


"""

CENSO ECONÓMICO 2019

URL : https://www.inegi.org.mx/contenidos/programas/ce/2019/Datosabiertos/ce2019_cdmx_csv.zip

"""

censo_file = os.path.join(sources_path, "ce2019_cdmx_csv", "conjunto_de_datos", "ce2019_cdmx.csv")
censo = pd.read_csv(censo_file)


censo_file_dict = os.path.join(sources_path, "ce2019_cdmx_csv", "diccionario_de_datos", "diccionario_de_datos_ce2019.csv")
censo_dict_datos = pd.read_csv(censo_file_dict)

censo["MUNICIPIO"] = censo["MUNICIPIO"].replace( {" ":"TOTAL"})


cw_encuestas = {
                "EMIM" : ( ('311','312','313','314','315','316','321','322','323','325','326','327','331','332','333','334','335','336','337','339'), 
                          {
                          "-Personal ocupado total (Número de personas)" : "H001A",
                          "-Remuneraciones totales dependientes de la razón social (Miles de pesos corrientes)" : "J000A",
                          "-Valor de producción de los productos elaborados (Miles de pesos corrientes)" : "O010A"
                          }),
                "ENEC" : ( ('23'), {
                          "--Personal ocupado total (Número de personas)" : "H001A",
                          "--Personal dependiente de la razón social (Número de personas)" : "H000A",
                          "--Remuneraciones totales (Miles de pesos corrientes)" : "J000A", 
                          "--Valor de producción total generado por las empresas de la entidad (Miles de pesos corrientes)" : "O010A"
                         }),
                "EMS" : ( ('72'), {
                          "Ingresos totales por suministro de bienes y servicios (Índice Base 2013 = 100)" : "M000A",
                          "Personal ocupado total (Índice Base 2013 = 100)" : "H001A"
                        }),
                "EMEC" : ( ('43', '46'),{
                          "Personal ocupado total" : "H001A",
                          "Remuneraciones totales" : "J000A",
                          "Ingresos totales por suministro de bienes y servicios" : "M000A"
                })
}

## Obtenemos ponderadores municipio-subsector para la EMIM
manufactura_long = pd.melt(censo[censo["CODIGO"].isin(cw_encuestas["EMIM"][0])].query("ENTIDAD == 9 and MUNICIPIO!='TOTAL'")\
            .dropna()[["MUNICIPIO", "CODIGO"]+list(cw_encuestas["EMIM"][1].values())]\
            .groupby(["MUNICIPIO","CODIGO"]).sum().reset_index(), id_vars = ["MUNICIPIO", "CODIGO"])

manufactura_long["ponderador"] =  manufactura_long.groupby(["CODIGO","variable"])["value"].transform(lambda x: x/x.sum())
manufactura_long = manufactura_long.rename(columns  = {"CODIGO" : "scian"}).drop(columns = "value")
manufactura_long["variable"] = manufactura_long["variable"].replace({j : i for i,j in cw_encuestas["EMIM"][1].items()})
manufactura_long["key"] = manufactura_long["scian"] + manufactura_long["variable"]

emim["scian"] = emim.sector.apply(lambda x : x[:3])
emim["key"] = emim["scian"] + emim["variable"] 

acumula_municipio_emim = []
for i in emim.time.unique():
    mes_muni_emim = manufactura_long.merge(right=emim.query(f"time=='{i}'")[["key","value","time"]], how = "left", on = "key")
    acumula_municipio_emim.append(mes_muni_emim)

municipio_emim = pd.concat(acumula_municipio_emim, ignore_index = True)
municipio_emim["value"] = municipio_emim["ponderador"]*municipio_emim["value"]

## Obtenemos ponderadores municipio-sector para la EMS
servicios_long = pd.melt(censo.query(f"CODIGO =='{cw_encuestas['EMS'][0]}' and ENTIDAD == 9 and MUNICIPIO!='TOTAL'")\
            .dropna()[["MUNICIPIO", "CODIGO"]+list(cw_encuestas["EMS"][1].values())]\
            .groupby(["MUNICIPIO","CODIGO"]).sum().reset_index(), id_vars = ["MUNICIPIO", "CODIGO"])

servicios_long["ponderador"] =  servicios_long.groupby(["CODIGO","variable"])["value"].transform(lambda x: x/x.sum())
servicios_long = servicios_long.rename(columns  = {"CODIGO" : "scian"}).drop(columns = "value")
servicios_long["variable"] = servicios_long["variable"].replace({j : i for i,j in cw_encuestas["EMS"][1].items()})
servicios_long["key"] = servicios_long["scian"] + " -" +servicios_long["variable"]

total_72_2019_anual = pd.melt(censo.query(f"CODIGO =='{cw_encuestas['EMS'][0]}' and ENTIDAD == 9 and MUNICIPIO=='TOTAL'")\
            .dropna()[["MUNICIPIO", "CODIGO"]+list(cw_encuestas["EMS"][1].values())]\
            .groupby(["MUNICIPIO","CODIGO"]).sum().reset_index(), id_vars = ["MUNICIPIO", "CODIGO"])

total_72_2019_anual["variable"] = total_72_2019_anual["variable"].replace({j : i for i,j in cw_encuestas["EMS"][1].items()})
total_72_2019_anual = total_72_2019_anual.rename(columns = {"CODIGO":"scian"})
total_72_2019_anual["key"] = total_72_2019_anual["scian"] + " -" + total_72_2019_anual["variable"]

ems["scian"] = ems.sector.apply(lambda x : x[:3])
ems["key"] = ems["scian"] + "-" + ems["variable"] 

ems["value_abs"] = ems["key"].replace({i:j for i,j in zip(total_72_2019_anual["key"], total_72_2019_anual["value"])})
ems["value_est"] = ems["value"]/100 * ems["value_abs"]


acumula_municipio_ems = []
for i in ems.time.unique():
    mes_muni_ems = servicios_long.merge(right=ems.query(f"time=='{i}'")[["key","value_est","time"]], how = "left", on = "key")
    acumula_municipio_ems.append(mes_muni_ems)

municipio_ems = pd.concat(acumula_municipio_ems, ignore_index = True)
municipio_ems["value_est"] = municipio_ems["ponderador"]*municipio_ems["value_est"]


##### VISUALIZACIONES DATOS MUNICIPALES

muni_cdmx_inegi_claves = pd.read_html("https://cuentame.inegi.org.mx/monografias/informacion/df/territorio/div_municipal.aspx?tema=me&e=09")[1]
muni_cdmx_inegi_claves = muni_cdmx_inegi_claves.iloc[:-1].rename(columns = {"Clave de la demarcaciÃ³n territorial" : "MUNICIPIO",
                                                            "Demarcación terrirorial" : "nombre"})
muni_cdmx_inegi_claves["MUNICIPIO"] = muni_cdmx_inegi_claves["MUNICIPIO"].astype(int)

heat_map_df = municipio_emim.query("time=='2023-02-01' and variable =='-Personal ocupado total (Número de personas)'")[["MUNICIPIO","scian","value"]].reset_index(drop=True)
heat_map_df["MUNICIPIO"] = heat_map_df["MUNICIPIO"].astype(int)

heat_map_df["MUNICIPIO"] = heat_map_df["MUNICIPIO"].replace({int(i):j for i,j in zip(muni_cdmx_inegi_claves["MUNICIPIO"],muni_cdmx_inegi_claves["nombre"])})

heat_map_matrix = heat_map_df.pivot(index ="scian", columns = "MUNICIPIO", values = "value")


import seaborn as sns
from matplotlib.colors import LogNorm, Normalize

sns.heatmap(heat_map_matrix, norm=LogNorm())

plt.title("Personal ocupado total (Número de personas)\n Enero 2023")
plt.show()

####

municipio_ems["MUNICIPIO"] = municipio_ems["MUNICIPIO"].astype(int)

municipio_ems["MUNICIPIO"] = municipio_ems["MUNICIPIO"].replace({int(i):j for i,j in zip(muni_cdmx_inegi_claves["MUNICIPIO"],muni_cdmx_inegi_claves["nombre"])})
municipio_ems.query("MUNICIPIO =='Cuauhtémoc' and variable == 'Ingresos totales por suministro de bienes y servicios (Índice Base 2013 = 100)'")[["time","value_est"]].set_index("time").plot(title = "Ingresos totales por suministro de bienes y servicios\n72 Servicios de alojamiento temporal y de preparación de alimentos y bebidas\nAlcaldía Cuauhtémoc (millones de pesos)",  legend=None)
plt.show()

municipio_ems.query("MUNICIPIO =='Cuauhtémoc' and variable == 'Personal ocupado total (Índice Base 2013 = 100)'")[["time","value_est"]].set_index("time").plot(title = "Personal ocupado total\n72 Servicios de alojamiento temporal y de preparación de alimentos y bebidas\nAlcaldía Cuauhtémoc",  legend=None)
plt.show()



### ESTIMACIÓN RECAUDACIÓN OPTIMA
"""
## Metodología

Se toma como base los datos de Total de remuneraciones (J000A) y Gastos por indemnización o liquidación del personal (J600A) 
del Censo Económico de 2019 (ambas cifras en millones de pesos). Los datos se actualizan utilizando las variaciones de remuneraciones totales de 
las siguientes encuestas : 
                cv_encuestas = {
                "EMIM" : ( ('311','312','313','314','315','316','321','322','323','325','326','327','331','332','333','334','335','336','337','339'), 
                          {
                          "-Remuneraciones totales dependientes de la razón social (Miles de pesos corrientes)" : "J000A",
                          }),
                "ENEC" : ( ('23'), {
                          "--Remuneraciones totales (Miles de pesos corrientes)" : "J000A", 
                         }),
                "EMS" : ( ('72'), {
                          "Ingresos totales por suministro de bienes y servicios (Índice Base 2013 = 100)" : "M000A",
                        }),
                "EMEC" : ( ('43', '46'),{
                          "Remuneraciones totales" : "J000A"}
                          }

Para los sectores que no tenemos las variaciones de las encuestas, utilizamos el Índice Global de Remuneraciones de los Sectores Económicos (IGRESE). Base 2013:

{'11' : "IGRESE",
 '21' : "IGRESE",
 '22' : "IGRESE",
 '23' : "ENEC",
 '43' : "EMEC",
 '46' : "EMEC",
 '51' : "IGRESE",
 '52' : "IGRESE",
 '53' : "IGRESE",
 '54' : "IGRESE",
 '55' : "IGRESE",
 '56' : "IGRESE",
 '61' : "IGRESE",
 '62' : "IGRESE",
 '71' : "IGRESE",
 '72' : "EMS",
 '81' : "IGRESE"
 }

Para la manufactura, tenemos la siguiente correspondencia

{'311' : "EMIM",
 '312' : "EMIM",
 '313' : "EMIM",
 '314' : "EMIM",
 '315' : "EMIM",
 '316' : "EMIM",
 '321' : "EMIM",
 '322' : "EMIM",
 '323' : "EMIM",
 '324' : "IGRESE",
 '325' : "EMIM",
 '326' : "EMIM",
 '327' : "EMIM",
 '331' : "EMIM",
 '332' : "EMIM",
 '333' : "EMIM",
 '334' : "EMIM",
 '335' : "EMIM",
 '336' : "EMIM",
 '337' : "EMIM",
 '339' : "EMIM"}

"""

##### Construimos indices base 2019

### Índice Global de Remuneraciones de los Sectores Económicos (IGRESE). Base 2013
igrese_file = os.path.join(sources_path, "IGRESE", "igrese_mensual_csv","conjunto_de_datos", "tr_igrese_2008_2023.csv")
igrese = pd.read_csv(igrese_file)

## Realizamos cambio de año base a junio de 2019
mes_to_number = {j:f"{i+1:02d}" for i,j in enumerate(igrese.iloc[:12]["MES"])}
igrese["MES"] = igrese["MES"].replace(mes_to_number)
igrese["time"] = pd.to_datetime(igrese.ANIO.apply(lambda x : str(x)) + "-" + igrese.MES + "-01") 
igrese = igrese.set_index("time")

igrese_val_base = igrese.loc["2019-06-01"]["IGRESE"]

igrese["IGRESE"] = (igrese["IGRESE"]/igrese_val_base)*100

igrese_indice_base_2019 = igrese["IGRESE"].copy()

### Encuesta Mensual de la Industria Manufacturera (EMIM)
emim_remu = emim.query("variable == '-Remuneraciones totales dependientes de la razón social (Miles de pesos corrientes)' and sector != '31-33 Industrias manufactureras'").reset_index(drop = True)
emim_remu["scian"] = emim_remu.sector.apply(lambda x : x[:3])
emim_remu = emim_remu[["time","scian","value"]]
emim_remu["time"] = pd.to_datetime(emim_remu["time"])

emim_remu_pivot = emim_remu.pivot(index='time', columns='scian', values='value')

for c in emim_remu_pivot.columns:
    val_base_emim = emim_remu_pivot.loc["2019-06-01"][c]
    emim_remu_pivot[c] =  (emim_remu_pivot[c]/val_base_emim)*100

### Encuesta Nacional de Empresas Constructoras (ENEC)
enec_remuneraciones = enec.query("variable == '-Remuneraciones totales (Miles de pesos corrientes)'").reset_index(drop = True)
enec_remuneraciones["time"] = pd.to_datetime(enec_remuneraciones["time"])

enec_remuneraciones["scian"] = enec_remuneraciones["sector"].apply(lambda x: x[:3].strip())

enec_remu_pivot = enec_remuneraciones.pivot(index='time', columns='scian', values='value')

for c in enec_remu_pivot.columns:
    val_base_enec = enec_remu_pivot.loc["2019-06-01"][c]
    enec_remu_pivot[c] =  (enec_remu_pivot[c]/val_base_enec)*100

### Encuesta Mensual de Servicios (EMS)
ems_remuneraciones = ems.query("variable == 'Ingresos totales por suministro de bienes y servicios (Índice Base 2013 = 100)'").reset_index(drop = True)
ems_remuneraciones["time"] = pd.to_datetime(ems_remuneraciones["time"])

ems_remuneraciones["scian"] = ems_remuneraciones["sector"].apply(lambda x: x[:3].strip())

ems_remu_pivot = ems_remuneraciones.pivot(index='time', columns='scian', values='value')

for c in ems_remu_pivot.columns:
    val_base_ems = ems_remu_pivot.loc["2019-06-01"][c]
    ems_remu_pivot[c] =  (ems_remu_pivot[c]/val_base_ems)*100


### Encuesta Mensual sobre Empresas Comerciales (EMEC). Base 2013
emec_remuneraciones = emec.query("variable == 'Remuneraciones totales'").reset_index(drop = True)
emec_remuneraciones["time"] = pd.to_datetime(emec_remuneraciones["time"])

emec_remuneraciones["scian"] = emec_remuneraciones["sector"].apply(lambda x: x[:3].strip())

emec_remu_pivot = emec_remuneraciones.pivot(index='time', columns='scian', values='value')

for c in emec_remu_pivot.columns:
    val_base_emec = emec_remu_pivot.loc["2019-06-01"][c]
    emec_remu_pivot[c] =  (emec_remu_pivot[c]/val_base_emec)*100

### Agrupamos los indices

indices_base_2019 = {
    "IGRESE" : igrese_indice_base_2019["2013":"2022"].copy(),
    "EMIM" : emim_remu_pivot["2013":"2022"].copy(),
    "ENEC" : enec_remu_pivot["2013":"2022"].copy(),
    "EMS" : ems_remu_pivot["2013":"2022"].copy(),
    "EMEC" : emec_remu_pivot["2013":"2022"].copy()
}


cw_sectores = {'11' : "IGRESE",
 '21' : "IGRESE",
 '22' : "IGRESE",
 '23' : "ENEC",
 '43' : "EMEC",
 '46' : "EMEC",
 '51' : "IGRESE",
 '52' : "IGRESE",
 '53' : "IGRESE",
 '54' : "IGRESE",
 '55' : "IGRESE",
 '56' : "IGRESE",
 '61' : "IGRESE",
 '62' : "IGRESE",
 '71' : "IGRESE",
 '72' : "EMS",
 '81' : "IGRESE",
 '311' : "EMIM",
 '312' : "EMIM",
 '313' : "EMIM",
 '314' : "EMIM",
 '315' : "EMIM",
 '316' : "EMIM",
 '321' : "EMIM",
 '322' : "EMIM",
 '323' : "EMIM",
 '324' : "IGRESE",
 '325' : "EMIM",
 '326' : "EMIM",
 '327' : "EMIM",
 '331' : "EMIM",
 '332' : "EMIM",
 '333' : "EMIM",
 '334' : "EMIM",
 '335' : "EMIM",
 '336' : "EMIM",
 '337' : "EMIM",
 '339' : "EMIM"}



from functools import reduce

acumula_tasas_promedio_anuales = []


for sector, encuesta in cw_sectores.items():
    for anio in range(2013, 2023):
        anio = str(anio)

        if encuesta == "IGRESE":
            tasa_promedio_anual = (reduce(lambda x, y: x*y, [(1+i) for i in indices_base_2019[encuesta].loc[anio].pct_change().dropna()])**(1/12)-1)*100
        else:
            tasa_promedio_anual = (reduce(lambda x, y: x*y, [(1+i) for i in indices_base_2019[encuesta].loc[anio][sector].pct_change().dropna()])**(1/12)-1)*100

        acumula_tasas_promedio_anuales.append(
            (anio, sector, tasa_promedio_anual)
        )

tasas_promedio_anuales = pd.DataFrame(acumula_tasas_promedio_anuales, columns = ["anio","scian","tasa"])
tasas_promedio_anuales_short = tasas_promedio_anuales.pivot(index = "anio", columns = "scian", values = "tasa") 

def tasa_futuro(valor_inicial, tasas):
    for tasa in tasas:
        valor_inicial = valor_inicial * (1+(tasa/100))
        yield valor_inicial

def tasa_pasado(valor_inicial, tasas):
    for tasa in tasas:
        valor_inicial = valor_inicial * (1+(tasa/100))**-1
        yield valor_inicial


def build_anual_serie(valor, datos_tasas, sector):
    val_futuro = [i for i in tasa_futuro(valor, datos_tasas["2020":][sector])]
    val_pasado = [i for i in tasa_pasado(valor, reversed(datos_tasas[:"2018"][sector]))]

    val_pasado.reverse()

    return val_pasado + [valor] + val_futuro


censo_totales = censo.query("MUNICIPIO =='TOTAL'").reset_index(drop = True)

censo_totales_remu = censo_totales[censo_totales.CODIGO.isin(list(cw_sectores))][["CODIGO","J000A","J600A"]]
censo_totales_remu["remu_total"] = censo_totales_remu["J000A"] + censo_totales_remu["J600A"]
#censo_totales_remu["remu_total"] = censo_totales_remu["J000A"] 
censo_totales_remu = censo_totales_remu.drop(columns= ["J000A", "J600A"])
censo_totales_remu = censo_totales_remu.groupby("CODIGO").sum()*1_000_000

print(f"2019 {int((censo_totales_remu*0.03).sum()/1_000_000)} millones de pesos")

dic_proyecciones = {"time" : range(2013, 2023)}

for sector in censo_totales_remu.index:
    
    dic_proyecciones[sector] = build_anual_serie(censo_totales_remu.loc[sector].remu_total, tasas_promedio_anuales_short, sector)


df_proyecciones = pd.DataFrame(dic_proyecciones)
df_proyecciones.set_index("time", inplace = True)


(df_proyecciones.sum(1)*0.03/1_000_000_000).plot(title = "Ingresos potenciales por el Impuesto sobre Nóminas\n(Miles de Millones de Pesos)")
plt.show()

ingresos_reales_nomina = pd.DataFrame({"time" : range(2013, 2022), "Observados" : [14_021, 18_197, 20_248, 21_678, 22_707, 24_817, 25_439, 26_279, 28_268]})
ingresos_reales_nomina.set_index("time", inplace = True)

ingresos_estimados_nomina = (df_proyecciones.sum(1)*0.03/1_000_000).rename("Simulado")

compara_ingresos = pd.concat([ingresos_reales_nomina, ingresos_estimados_nomina], axis = 1)
compara_ingresos = compara_ingresos/1000

compara_ingresos.plot.bar(title = "Ingresos potenciales por el Impuesto sobre Nóminas\n(Miles de Millones de Pesos)", rot = 0)
plt.show()

##### Shares

sectores_no_manufactura = list(set(df_proyecciones.columns) - set([i for i in df_proyecciones.columns if len(i)==3]))
sectores_no_manufactura.sort() 
sectores_manufactura = [i for i in df_proyecciones.columns if len(i)==3]

df_solo_sectores = pd.concat([df_proyecciones[sectores_no_manufactura], df_proyecciones[sectores_manufactura].sum(axis = 1).rename("311-339")], axis = 1)

df_shares_solo_sectores = pd.DataFrame( np.matrix(df_solo_sectores.to_numpy())/np.matrix(df_solo_sectores.to_numpy()).sum(axis = 1), columns = df_solo_sectores.columns) 
df_shares_solo_sectores.plot.area()
plt.show()