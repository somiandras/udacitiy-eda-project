#!usr/bin/env python
#-*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import logging
from datetime import date
from calendar import monthrange
import re
from pymongo import MongoClient

client = MongoClient()
db = client.cars


logging.basicConfig(filename='scrape.log', filemode='w', level='ERROR')

BASE_URL = 'https://www.hasznaltauto.hu/auto/ford/focus'
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.115 Safari/537.36'}


def get_ad_links():
    '''Download initial page of results list, extract the number of result pages,
    loop through them and save all the individual ad links to a txt file'''
    ad_links = []
    # Initial page to get page numbers
    r = requests.get(BASE_URL, headers=HEADERS)
    if r.status_code == requests.codes.ok:
        soup = BeautifulSoup(r.text, 'lxml')
        last_page_number = int(soup.find(title='Utolsó oldal').text)

        # Loop through results pages and get ad links
        for page in range(1, last_page_number + 1):
            url = BASE_URL + '/page{}'.format(page)
            ad_page = requests.get(url, headers=HEADERS)
            if ad_page.status_code == requests.codes.ok:
                soup = BeautifulSoup(ad_page.text, 'lxml')
                result_items = soup.find_all('div', class_='talalati_lista_head')
                for result_item in result_items:
                    link = result_item.find('a')
                    ad_links.append(link.get('href'))
            else:
                logging.error('Cannot get {}'.format(url))

        # Save ad links
        with open('links.txt', 'w') as f:
            for link in ad_links:
                f.write(link + '\n')
    else:
        logging.error('Cannot get {}'.format(BASE_URL))


def get_car_details(url):
    '''Extract car details from HTML and return data in dict'''
    data = {}
    data['Id'] = url.strip()[-8:]
    data['url'] = url.strip()
    r = requests.get(url, headers=HEADERS)
    soup = BeautifulSoup(r.text, 'lxml')
    data['Title'] = soup.find('span', property='p:name').text

    data_table = soup.find('table', class_='hirdetesadatok')
    cells = data_table.find_all('td')

    for cell_index in range(0, len(cells), 2):
        key = cells[cell_index].text.strip()
        value = cells[cell_index + 1].text.strip()
        cleaned = clean_key_value(key, value)
        data[cleaned[0]] = cleaned[1]
    return data


def clean_key_value(key, value):
    '''Clean key-value pair'''
    numeric_fields = ['Csomagtartó', 'Hengerűrtartalom', 'Kilométeróra állása', 'Saját tömeg', 'Szállítható szem. száma', 'Össztömeg', 'Vételár', 'Ár (EUR)']
    new_key = key[:-1]
    new_value = value

    if new_key in numeric_fields:
        value = re.sub('\xa0Ft', 'Ft', value)
        value = re.sub('€\s', '', value)
        value = value.replace(' ', '').replace('.', '')
        match = re.match('([0-9]*)(.*)?', value)
        if len(match.group(1)) > 0:
            new_value = int(match.group(1))
        else:
            new_value = ''
        if new_key != 'Ár (EUR)':
            new_key = new_key + ' ({})'.format(match.group(2))

    if new_key == 'Ajtók száma':
        new_value = int(value)

    if new_key == 'Teljesítmény':
        match = re.match('([0-9]{2,3})\skW,\s([0-9]{2,3})\sLE', value)
        new_value = int(match.group(2))
        new_key = new_key + ' (LE)'

    return (new_key, new_value)


def scrape():
    '''Loop through ad links and save data to MongoDB collection'''
    with open('links.txt', 'r') as links:
        for link in links:
            check = db.focus.find_one({'url': link.strip()})
            if check is None:
                print(link)
                try:
                    details = get_car_details(link)
                except Exception as e:
                    logging.error(link)
                    logging.error(e)
                else:
                    db.focus.update_one({'url': link}, {'$set': details}, upsert=True)


if __name__ == '__main__':
    get_ad_links()
    scrape()
