#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  1 19:31:54 2023

@author: fernandasobrino
"""

import requests
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
from pathlib import Path

from requests_ip_rotator import ApiGateway


#session = requests.Session()

REQUEST_HEADER = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.75 Safari/537.36"}


RULES_SEARCH_PAGE = {
    'url': {'tag':'a', 'search':{'data-testid':'title-link'}},
    'name': {'tag':'div', 'search':{'data-testid':'title'}},
    'precio':{'tag':'span', 'search':{'data-testid': 'price-and-discounted-price'}},
    'noches':{'tag':'div', 'search':{'data-testid': 'price-for-x-nights'}},
    'tipo':{'tag':'span', 'search':{'class': 'df597226dd'}},
    'descripcion_1':{'tag':'div', 'search':{'data-testid':'property-card-unit-configuration'}},
    'descripcion_2':{'tag':'div', 'search':{'class':'cb5b4b68a4'}},
    'taxes':{'tag':'div', 'search':{'data-testid':'taxes-and-charges'}}                
    } 



def extract_listings(page_url, attempts = 10):
    listings_max = 0
    listings_out = [BeautifulSoup('', features = 'html.parser')]
    for idx in range(attempts):
        try:
            answer = session.get(page_url, headers=REQUEST_HEADER, timeout = 5)
            content = answer.content
            soup = BeautifulSoup(content, features='html.parser')
            listings = soup.find_all("div", {"data-testid": "property-card"})
        except:
            listings = [BeautifulSoup('', features='html.parser')]

        if len(listings) == 25:
            listings_out = listings
            break

        if len(listings) >= listings_max:
            listings_max = len(listings)
            listings_out = listings
    return listings_out


def extract_listing_features(soup, rules):
    features_dict = {}
    for feature in rules:
        try:
            features_dict[feature] = extract_element_data(soup, rules[feature])
        except:
            features_dict[feature] = 'empty'
    return features_dict


def extract_element_data(soup, params):
    if 'a' in params.values():
        elements_found = soup.find(params['tag'], params['search'])['href']
    else:
        elements_found = soup.find(params['tag'], params['search']).text
    return elements_found
        

    
def scrape_detail_page(detailed_url):
    location_features = {}
    answer_detailed = requests.get(detailed_url, timeout=5)
    content_deatiled = answer_detailed.content
    soup_deatiled = BeautifulSoup(content_deatiled, features='html.parser') 
    coordinates = soup_deatiled.find("a", {"id":"hotel_sidebar_static_map"})["data-atlas-latlng"]
    coord = coordinates.split(',')
    location_features['lat'] = coord[0]
    location_features['lng'] = coord[1]
    return location_features
    
    


class Parser:
    def __init__(self, link, out_file):
        self.link = link
        self.out_file = out_file

    
    def build_urls(self, listings_per_page = 25, pages_per_location = 35):
        url_list = []
        for i in range(pages_per_location):
            offset = listings_per_page * i
            url_pagination = self.link + f'&offset={offset}'
            url_list.append(url_pagination)
            self.url_list = url_list
            
    def process_search_pages(self):
        features_list = []
        MAX_OFFSET = 100
        TOTAL_LENGTH = len(self.url_list)

        for ID_PAGE, page in enumerate(self.url_list):
            OFFSET_PROGRESS_URL = MAX_OFFSET//TOTAL_LENGTH
            PORCENTAJE_PROGRESO_URL = round(ID_PAGE/TOTAL_LENGTH,2)*100
            
            print('\rURL Page progreso|{}{}|{}% ({}/{})\n'.format('='*ID_PAGE*OFFSET_PROGRESS_URL,
                                    " "*((TOTAL_LENGTH*OFFSET_PROGRESS_URL)-(ID_PAGE*OFFSET_PROGRESS_URL)-OFFSET_PROGRESS_URL), 
                                    PORCENTAJE_PROGRESO_URL, ID_PAGE, TOTAL_LENGTH), end='') 
                                    
            listings = extract_listings(page)

            TOTAL_LISTING = len(listings)

            for id_listing, listing in enumerate(listings):

                try:
                    OFFSET_PROGRESS_LISTING = MAX_OFFSET//TOTAL_LISTING
                    PORCENTAJE_PROGRESO_LISTING = round(id_listing/TOTAL_LISTING,2)*100


                    print('\rListing progreso|{}{}|{}%'.format(
                                        '='*id_listing*OFFSET_PROGRESS_LISTING,
                                            " "*((TOTAL_LISTING*OFFSET_PROGRESS_LISTING)-(id_listing*OFFSET_PROGRESS_LISTING)-OFFSET_PROGRESS_LISTING), 
                                            PORCENTAJE_PROGRESO_LISTING), end='')                    
            
                    features = extract_listing_features(listing, RULES_SEARCH_PAGE)
                    detailed_url = features['url']
                    location_features = scrape_detail_page(detailed_url)
                    features_all = {**features, **location_features}
                    features_list.append(features_all)
                except:
                    pass
        self.base_features_list = features_list
        

    def save(self):
        pd.DataFrame(self.base_features_list).to_csv(self.out_file)

             
    def parse(self):
        print("build_urls")
        self.build_urls()
        print("process_search_pages")
        self.process_search_pages()
        print("save")
        self.save()





def create_link(people, country, city, datein, dateout):
    url = "https://www.booking.com/searchresults.html?checkin_month={in_month}" \
      "&checkin_monthday={in_day}&checkin_year={in_year}&checkout_month={out_month}" \
      "&checkout_monthday={out_day}&checkout_year={out_year}&group_adults={people}" \
      "&group_children=0&order=price&ss={city}%2C%20{country}"\
      .format(in_month=str(datein.month),
              in_day=str(datein.day),
              in_year=str(datein.year),
              out_month=str(dateout.month),
              out_day=str(dateout.day),
              out_year=str(dateout.year),
              people=people,
              city=city,
              country=country)
    print(url)
    file = 'bookinng_' + city + '_' + str(datein) + '_' + str(dateout)
    return url, file 
## las fechas tienen que estar en datetime object osea hay que hacerles esto:
## pd.to_datetime('2023-09-25').date()
## el offset empieza en zero pero para las siguientes iteraciones es cada 25 listings
## y hay entre 38 y 40 paginas por busqueda 
## &offset={offset}

if __name__ == "__main__":

    # Create gateway object and initialise in AWS
    gateway = ApiGateway("https://www.booking.com")
    gateway.start()


    # Assign gateway to session
    session = requests.Session()
    session.mount("https://www.booking.com", gateway)

    people = 2 
    country = 'Mexico'
    city = 'Mexico-City'
    """
    2023-08-25/2023-08-27
    2023-09-01/2023-09-03
    2023-09-08/2023-09-10
    2023-09-15/2023-09-17
    2023-09-22/2023-09-24
    2023-09-29/2023-10-01
    2023-10-06/2023-10-08
    2023-10-13/2023-10-15
    2023-10-20/2023-10-22
    2023-10-27/2023-10-29
    2023-11-03/2023-11-05
    2023-11-10/2023-11-12
    2023-11-17/2023-11-19
    2023-11-24/2023-11-26
    """
    datein = pd.to_datetime('2023-09-29').date()
    dateout = pd.to_datetime('2023-10-01').date()
    link, file = create_link(people, country, city, datein, dateout)
    new_parser = Parser(link, file + '.csv')
    t0 = time.time()
    new_parser.parse()
    print(time.time() - t0)
    




