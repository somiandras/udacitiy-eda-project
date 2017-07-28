import pprint as pp
from pymongo import MongoClient
import pandas as pd
import numpy as np
import re
import json

client = MongoClient()
db = client.cars

# Dictionary map between HUN and ENG strings
dictionary = json.load(open('dictionary.json', 'r', encoding='utf-8'))

# Column name translation map
name_map = {
    'Ajtók száma': 'No_of_doors',
    'C-Max': 'C-Max_variant',
    'Csomagtartó (liter)': 'Trunk_capacity_liters',
    'Hajtás': 'Drive',
    'Hengerűrtartalom (cm³)': 'Capacity',
    'Kilométeróra állása (km)': 'Kilometers_run',
    'Kivitel': 'Form',
    'Klíma fajtája': 'Air_conditioning',
    'Műszaki vizsga érvényes': 'Documents_valid_until',
    'Okmányok jellege': 'Documents_type',
    'Saját tömeg (kg)': 'Own weight_kg',
    'Sebességváltó fajtája': 'Transmission',
    'Szín': 'Color',
    'Teljesítmény (LE)': 'Horsepower',
    'Állapot': 'Condition',
    'Ár (EUR)': 'Price_EUR',
    'Össztömeg (kg)': 'Total_weight_kg',
    'Üzemanyag': 'Fuel',
    'Évjárat év': 'Year_of_manufacturing',
    'Évjárat hónap': 'Month_of_manufacturing'
}

# Columns to drop from raw data
unnecessary_columns = [
    'Akciós ár',
    'Alaptípus ára',
    'Alvázszám',
    'Bérelési lehetőség',
    'Egyéb költségek',
    'Extrákkal növelt ár',
    'Finanszírozás',
    'Finanszírozás típusa CASCO nélkül',
    'Finanszírozás típusa CASCO-val',
    'Futamidő',
    'Futamidő CASCO nélkül',
    'Futásidő',
    'Garancia',
    'Havi részlet',
    'Hátsó nyári gumi méret',
    'Hátsó téli gumi méret',
    'Kezdőrészlet',
    'Kezdőrészlet CASCO nélkül',
    'Kilométeróra állása (Nincsmegadva)',
    'Kárpit színe (1)',
    'Kárpit színe (2)',
    'Szavatossági garancia',
    'Tető',
    'Téli gumi méret',
    '_id',
    'url',
    'Átrozsdásodási garancia',
    'Vételár ()',
    'Vételár (Ft)',
    'Vételár (FtHitelkalkulátoritt›)',
    'Vételár (Árnélkül)',
    'Teljes vételár',
    'Nyári gumi méret',
    'Title',
    'Id',
    'Henger-elrendezés']


def get_month(string):
    '''Extract month from manufacture date'''
    match = re.match('[0-9]{4}\/?([0-9]{2})?', string)
    if match and match.group(1):
        return match.group(1)
    else:
        return None


def translate(string):
    '''Get the English version from dictionary dict if it exists'''
    if string in dictionary:
        return dictionary[string]
    else:
        return string


def get_gears(string):
    '''Extract number of gears from transmission string if possible'''
    try:
        match = re.match('^(Automata|Manuális)\s\(([4-7])\sfokozatú\)', string)
        if match:
            return int(match.group(2))
        else:
            return np.nan
    except Exception:
        return np.nan


def is_metallic(string):
    '''Decide if the given color is metallic or basic'''
    try:
        words = string.split(' ')
        return '(metál)' in words
    except Exception:
        return np.nan


def get_color(string):
    '''Extract color from string and return translated'''
    try:
        match = re.match('(.*\s)(?:\(metál\))?', string)
        if match:
            return translate(match.group(1).strip())
        else:
            return np.nan
    except Exception:
        return np.nan


if __name__ == '__main__':
    cars = db.focus.find()
    df = pd.DataFrame(list(cars))

    # Drop unneeded rows and columns
    df.drop(unnecessary_columns, axis=1, inplace=True)
    df.dropna(subset=['Ár (EUR)'], inplace=True)

    # Transform price and manufacturing columns
    df['Ár (EUR)'] = df['Ár (EUR)'].astype('int')
    df['Évjárat év'] = df['Évjárat'].apply(lambda x: x[:4])
    df['Évjárat hónap'] = df['Évjárat'].apply(get_month)

    # Translate remaingin columns
    df.rename(columns=name_map, inplace=True)

    # Translate categorical values
    df['Drive'] = df['Drive'].apply(translate)
    df['Form'] = df['Form'].apply(translate)
    df['Air_conditioning'] = df['Air_conditioning'].apply(translate)
    df['Transmission_type'] = df['Transmission'].apply(translate)
    df['Gears'] = df['Transmission'].apply(get_gears)
    df['Documents'] = df['Documents_type'].apply(translate)
    df['Condition'] = df['Condition'].apply(translate)
    df['Fuel'] = df['Fuel'].apply(translate)
    df['Paint'] = df['Color'].apply(get_color)
    df['Metallic'] = df['Color'].apply(is_metallic)

    # Drop unnecessary columns
    df.drop(['Transmission', 'Documents_type', 'Évjárat', 'Color'], axis=1, inplace=True)

    # Save to file
    df.to_csv('../used_ford_focuses.csv', encoding='utf-8', index=False)
