#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun  7 20:24:05 2023

@author: fernandasobrino
"""

import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
from pathlib import Path
from tqdm import tqdm

from requests_ip_rotator import ApiGateway

path = Path('/home/milo/Documents/egap/BID/tesoreria/scraping')

RULES_SEARCH_PAGE = {
    'url':{'tag':'a','get': 'href'},
    'name':{'tag':'div','class': 't1jojoys dir dir-ltr'},
    'name_alt':{'tag':'div', 'class':'fb4nyux s1cjsi4j dir dir-ltr'},
    'rooms':{'tag':'span', 'class':'dir dir-ltr'},
    'price':{'tag':'div', 'class':'pquyp1l dir dir-ltr'}}


def extract_listings(page_url, attempts=10):
    listings_max = 0
    listings_out = [BeautifulSoup('', features='html.parser')]
    for idx in range(attempts):

        try:
            #answer = session.get(page_url, timeout=5)
            answer = session.get(page_url, timeout=60)
            content = answer.content
            soup = BeautifulSoup(content, features='html.parser')
            listings = soup.findAll("div", {"class": "cy5jw6o dir dir-ltr"})
            bandera = False
            
        except:
            listings = [BeautifulSoup('', features='html.parser')]

        if len(listings) == 20:
            listings_out = listings
            break

        if len(listings) >= listings_max:
            listings_max = len(listings)
            listings_out = listings

    return listings_out
        


def extract_element_data(soup, params):
    if 'class' in params:
        elements_found = soup.find_all(params['tag'], params['class'])
    else:
        elements_found = soup.find_all(params['tag'])
    if 'get' in params:
        element_texts = [el.get(params['get']) for el in elements_found]
    else:
        element_texts = [el.get_text() for el in elements_found]
    tag_order = params.get('order', 0)
    if tag_order == -1:
        output = '**__**'.join(element_texts)
    else:
        output = element_texts[tag_order]
    
    return output



def extract_listing_features(soup, rules):
    features_dict = {}
    for feature in rules:
        try:
            features_dict[feature] = extract_element_data(soup, rules[feature])
        except:
            features_dict[feature] = 'empty'
    return features_dict


def scrape_detail_page(detailed_url):
    soup_detail = requests.get(detailed_url, timeout = 5)
    p_lat = re.compile(r'"lat":([-0-9.]+),')
    p_lng = re.compile(r'"lng":([-0-9.]+),')
    ### AGREGUE ESTA LINEEA DE bedroom_Pattern
    bedroom_pattern = r'(\d+)\s+bedroom'
    extra_features = {}
    extra_features['lat'] = p_lat.findall(soup_detail.text)[0]
    extra_features['lng'] = p_lng.findall(soup_detail.text)[0] 
    ## AGREGUE ESTE TRY PARA EXTRAER LOS CUARTOS Y OJO LE CAMBIE EL NOMBRE 
    # A EXTRA FEATURES PERO SE LO PUEDES DEJAR COMO ESTABA NO PASA NADA 
    try: 
        extra_features['bedrooms'] = re.findall(bedroom_pattern, soup_detail.text.lower())[0]
    except: 
        extra_features['bedrooms'] = 'NaN'
    return extra_features





class Parser:
    def __init__(self, link, out_file):
        self.link = link
        self.out_file = out_file

    
    def build_urls(self, listings_per_page = 18, pages_per_location = 15):
        """Builds links for all search pages for a given location"""
        url_list = []
        for i in range(pages_per_location):
            offset = listings_per_page * i
            url_pagination = self.link + f'&items_offset={offset}'
            url_list.append(url_pagination)
            self.url_list = url_list

        print("PASÓ build_urls")

    def process_search_pages(self):
        """Extract features from all search pages"""
        features_list = []
        for page in tqdm(self.url_list):
            listings = extract_listings(page)
            for listing in listings:
                features = extract_listing_features(listing, RULES_SEARCH_PAGE)
                features['sp_url'] = page
                detailed_url = 'https://www.airbnb.com' + features['url']

                bandera = True
                cuenta = 0
                while bandera and cuenta<10:
                    try:
                        location_feature = scrape_detail_page(detailed_url)
                        features_all = {**features, **location_feature}
                        features_list.append(features_all)
                        bandera = False
                    except:
                        print("FALLÓ")
                        print("REINTENTAMOS")
                        cuenta +=1
                        #pass
        self.base_features_list = features_list
        

        
    def save(self):
        pd.DataFrame(self.base_features_list).to_csv(self.out_file)

            
        
    def parse(self):
        self.build_urls()
        self.process_search_pages()
        self.save()






def create_link(colonia):
    col = colonia.split('(')[0].strip().lower()
    col = col.replace(' ', '-')
    http_1 = 'https://www.airbnb.com/s/'
    http_2 = '--Mexico-City/homes?tab_id=home_tab&refinement_paths%5B%5D=%2Fhomes&flexible_trip_lengths%5B%5D=one_week&monthly_start_date=2023-07-01&monthly_length=3&price_filter_input_type=0&price_filter_num_nights=5&channel=EXPLORE&date_picker_type=calendar&source=structured_search_input_header&search_type=search_query'
    link = http_1 + col + http_2 
    return link, col
    



if __name__ == "__main__":
    # Create gateway object and initialise in AWS
    gateway = ApiGateway("https://www.airbnb.com")
    gateway.start()

    with open("colonias_priorizadas.txt","r") as file:
        colonias_priorizadas = [i.replace("\n","") for i in file.readlines()]
    file.close()

    # Assign gateway to session
    session = requests.Session()
    session.mount("https://www.airbnb.com", gateway)

    colonias = pd.read_csv(path/'coloniascdmx.csv')

    colonias_resto = set(colonias.nombre) - set(colonias_priorizadas)

    #for colonia in colonias.nombre[:1]:
    for id_col,colonia in enumerate(colonias_priorizadas[:1]):
        print(f"#{id_col}-{len(colonias_priorizadas)} {colonia}")
        LINK, file = create_link(colonia)
        new_parser = Parser(LINK, file + '.csv')
        t0 = time.time()
        new_parser.parse()
        print(time.time() - t0)
    

    # Delete gateways
    gateway.shutdown()