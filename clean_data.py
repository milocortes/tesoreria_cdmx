#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 11 10:53:18 2023

@author: fernandasobrino
"""

import boto3
import pandas as pd
import seaborn as sns
import numpy as np 
import matplotlib.pyplot as plt
from sklearn.preprocessing import OneHotEncoder
from sklearn.cluster import DBSCAN


s3 = boto3.client('s3') 

obj = s3.get_object(Bucket= "proyectomate01", Key="laplace.csv") 

initial_df = pd.read_csv(obj['Body'])
# Base original 2'751,882 y 46 columnas 

# Tirar las variables que nos dijeron no usaramos 
drop_columns = ['X'+str(i) for i in range(1, 32) if i != 3]
initial_df = initial_df.drop(drop_columns, axis=1)


# 0. Tirar los duplicados en las 46 variables
duplicated_df = initial_df[initial_df.duplicated(keep=False)]
base = initial_df.drop_duplicates()


## 1. Arreglar las fechas de lo que estan pagando 
base['V2_new'] = pd.to_datetime(base['V2'], format='%Y-%m-%d', errors='coerce')
base['V6_new'] = base['V6'].astype(str).apply(lambda x: x[:-2] + '-' + x[4:])
base['V6_new'] = pd.to_datetime(base['V6_new'], format='%Y-%m', errors='coerce')
base = base[base.V6_new.notnull()]
base['pago_year'] = base['V6_new'].dt.year



## 2. Arreglar los missings
## V11 - C si se cancelo / 0 otherwise 
base['V11'].fillna(0, inplace=True)

## ESTO FUE LO QUE ME EXPLICO ISRAEL: ES UN POCO UN CAOS Y POR ESTO ESTE CODIGO ES FEO 
## sirve 

## V7: antes de 2018 el numero de empleados de la primera declaración, 
## despues del 2018, la suma: 
base_before_2018 = base[base.pago_year < 2018]
base_after_2018 = base[base.pago_year >= 2018]    

## Arreglar todo para antes de 2018 (aca dijeron que si el numero de empleados no es el 
## mismo que los sumemos y si es el mismo no hay que sumarlo)
base_before_2018 = base_before_2018.sort_values(['V1','V6_new'])
base_before_2018['V7'] = base_before_2018.groupby(['V1','V6_new','V9','V10','V11','V12',
                                                 'V13','V14','V15'])['V7'].ffill()
duplicados_before = base_before_2018.duplicated(subset = ['V1','V6_new',
                                                          'V9','V10','V11','V12',
                                                          'V13','V14','V15'], keep = False)
db_df = base_before_2018[duplicados_before]
ndb_df = base_before_2018[~duplicados_before]

def aggregate_if_different(group):
    return group.nunique() == group.size
grouped_db = db_df.groupby(['V1', 'V6_new',
                            'V9','V10','V11','V12',
                            'V13','V14','V15'])
mask = grouped_db['V7'].transform(aggregate_if_different)
db_df.loc[mask, 'V7'] = grouped_db['V7'].transform('sum')  
base_before = pd.concat([db_df, ndb_df])
# agregar las variables V3, V4 y V7 y sacar el rango de los pagos cuando hay 
grouped_sums = base_before.groupby(['V1', 'V6_new',
                                    'V9','V10','V11','V12',
                                    'V13','V14','V15'])[['V3', 'V4', 'V8']].sum().reset_index()
grouped_range = base_before.groupby(['V1','V6_new', 
                                     'V9','V10','V11','V12',
                                     'V13','V14','V15']).agg(fecha_primer_pago = ('V2_new','min'),
                                                         fecha_ultimo_pago = ('V2_new','max')).reset_index()
base_before = pd.merge(base_before[['V1', 'V6_new','V9','V5','V11','V7','V12',
                                    'V14','V15','V10','V13','pago_year']],
                       grouped_sums, on = ['V1','V6_new',
                                           'V9','V10','V11','V12',
                                           'V13','V14','V15'], how = 'left')
base_before = pd.merge(base_before, grouped_range, on = ['V1','V6_new',
                                                         'V9','V10','V11','V12',
                                                         'V13','V14','V15'], how = 'left')
base_before = base_before.drop_duplicates()

## Arreglar todo para despues de 2018 (aca solo hay que sumarlas si estan repetidos
## o eso dijeron) 
base_after_2018 = base_after_2018.sort_values(['V1','V6_new'])



duplicados_after = base_after_2018.duplicated(subset = ['V1','V6_new',
                                                          'V9','V10','V11','V12',
                                                          'V13','V14','V15'], keep = False)
da_df = base_after_2018[duplicados_after]
da_df['V7'].fillna(0, inplace = True)


def sum_or_nan(x):
    return x.sum() if not x.isna().any() else np.nan


grouped_sums = da_df.groupby(['V1','V6_new',
                                    'V9','V10','V11','V12',
                                    'V13','V14','V15'])[['V3', 'V4','V7','V8']].sum().reset_index()
grouped_range = da_df.groupby(['V1','V6_new', 
                                     'V9','V10','V11','V12',
                                     'V13','V14','V15']).agg(fecha_primer_pago = ('V2_new','min'),
                                                         fecha_ultimo_pago = ('V2_new','max')).reset_index()

base_after_duplicates = pd.merge(da_df[['V1', 'V6_new','V9','V5','V11','V12',
                                    'V14','V15','V10','V13','pago_year']],
                       grouped_sums, on = ['V1','V6_new',
                                           'V9','V10','V11','V12',
                                           'V13','V14','V15'], how = 'left')

base_after_duplicates = pd.merge(base_after_duplicates, grouped_range, on = ['V1','V6_new',
                                                         'V9','V10','V11','V12',
                                                         'V13','V14','V15'], how = 'left')
base_after_duplicates = base_after_duplicates.drop_duplicates()


nda_df = base_after_2018[~duplicados_after]
nda_df['fecha_primer_pago'] = nda_df['fecha_ultimo_pago'] = nda_df['V2_new']
nda_df = nda_df[['V1','V6_new', 'V9','V5','V11','V12','V14',
                 'V15','V10','V13','pago_year','V3','V4',
                 'V7','V8', 'fecha_primer_pago', 'fecha_ultimo_pago']]


base_after = pd.concat([base_after_duplicates, nda_df])

# Ojo: la base clean sigue teniendo missing values en el numero de empleados
# segui las instrucciones de Isarael pero estas no quitaron todos 
# hay   19,261 

base_clean = pd.concat([base_before, base_after])









