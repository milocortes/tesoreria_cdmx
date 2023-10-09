"""
Created on Mon Aug 14 10:44:03 2023

@author: fernandasobrino
"""

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
import re
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import time 
from tqdm import tqdm 


## Ojo: este codigo usa selenium (no jala con solo requests porque es javascript)
## Entonces hay que literal abrir y cerrar en este caso firefox cada vez :(
## Hay que cambiar el IP despues de extraer no se 10/15 listings en el for 
# de los listings adentro del process_search_pages



def extract_listings(url):
    ## Esto nos va a dar en promedio 500 listings por alcaldia 
    driver = webdriver.Firefox()
    driver.get(url)
    time.sleep(5)
    i = 0 
    while i < 8:
        try:
            # cada que apretamos el boton salen 50 mas 
            more_button = driver.find_element(By.CSS_SELECTOR,'button[data-stid="show-more-results"]')
            more_button.click()
            time.sleep(10)
            i += 1
        except: 
            break
    page_source = driver.page_source        
    soup = BeautifulSoup(page_source, features='html.parser')
    listings = soup.find_all("a", {"data-stid": "open-hotel-information"})
    driver.close()
    return listings
    
    

def extract_features(url):
    features = {}
    driver = webdriver.Firefox()
    driver.get(url)
    source_detailed = driver.page_source        
    soup_detailed = BeautifulSoup(source_detailed, features='html.parser') 
    geo_div = soup_detailed.find("div", {"itemprop": "geo"})   
    features['latitude'] = geo_div.find("meta", {"itemprop": "latitude"})["content"]
    features['longitude'] = geo_div.find("meta", {"itemprop": "longitude"})["content"]     
    features['nombre'] = soup_detailed.find("h1", {"class": "uitk-heading uitk-heading-3"}).text
    features['address'] = soup_detailed.find("div", {"data-stid": "content-hotel-address"}).text
    amenities_button = driver.find_element(By.CSS_SELECTOR,'a[href="#Amenities"]')    
    amenities_button.click()   
    amenities_button.click()
    amenities_button.click()
    source_amenities = driver.page_source        
    soup_amenities = BeautifulSoup(source_amenities, features='html.parser')  
    descripcion_lista = soup_amenities.find_all("div",{"class":"uitk-text uitk-type-300 uitk-text-default-theme"})
    descripcion = ''
    for element in descripcion_lista:
        descripcion = descripcion + ' ' + element.text + ' '
    features['descripcion'] = descripcion
    driver.close()
    return features




class Parser:
    def __init__(self, link, out_file):
        self.link = link
        self.out_file = out_file
        
    def process_search_pages(self):
        features_list = []
        features = {}
        listings = extract_listings(self.link)
        
        for listing in tqdm(listings): 
            try:
                features['url'] = listing['href']
                detailed_url = 'https://www.hoteles.com' + features['url']
                extra_features = extract_features(detailed_url)
                features_all = {**features, **extra_features}
                features_list.append(features_all)
            except:
                pass
        self.features_list = features_list
    
    def save(self):
        pd.DataFrame(self.features_list).to_csv(self.out_file)
        
    def parse(self):
        self.process_search_pages()
        self.save()




def create_link(alcaldia,datein, dateout):
    url = 'https://www.hoteles.com/Hotel-Search?destination={alcaldia}'\
        '&d1={datein}&startDate={datein}&endDate={dateout}&adults=2&rooms=1'\
            .format(datein = str(datein),\
                    dateout = str(dateout),
                    alcaldia = alcaldia)
    file = 'hoteles_' + alcaldia.replace(' ', '_')
    return url, file 
    


alcaldias = ['Alvaro Obregon', 'Azcapotzalco', 'Benito Juarez', 'Coyoacan',
             'Cuajimalpa' ,'Cuauhtemoc', 'Gustavo A. Madero', 'Iztacalco',
             'Iztapalapa', 'Magdalena Contreras', 'Miguel Hidalgo', 
             'Milpa Alta', 'Tlahuac', 'Tlalpan', 'Venustiano Carranza', 
             'Xochimilco']


if __name__ == "__main__":
    datein = '2023-09-01'
    dateout = '2023-09-02'
    for alcaldia in alcaldias:
        link, file = create_link(alcaldia, datein, dateout)
        new_parser = Parser(link, file + '.csv')
        t0 = time.time()
        new_parser.parse()
        print(time.time() - t0)
        