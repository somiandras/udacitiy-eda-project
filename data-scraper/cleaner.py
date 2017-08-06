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
    'Ajtók száma': 'Doors',
    'C-Max': 'C-Max_variant',
    'Csomagtartó (liter)': 'Trunk capacity liters',
    'Hajtás': 'Drive',
    'Hengerűrtartalom (cm³)': 'Capacity',
    'Kilométeróra állása (km)': 'Mileage km',
    'Kivitel': 'Form',
    'Klíma fajtája': 'A/C type',
    'Műszaki vizsga érvényes': 'Documents valid',
    'Okmányok jellege': 'Documents type',
    'Saját tömeg (kg)': 'Own weight kg',
    'Sebességváltó fajtája': 'Transmission',
    'Szín': 'Color',
    'Teljesítmény (LE)': 'Horsepower',
    'Állapot': 'Condition',
    'Ár (EUR)': 'Price EUR',
    'Össztömeg (kg)': 'Total weight kg',
    'Üzemanyag': 'Fuel',
    'Évjárat év': 'Manufactured year',
    'Évjárat hónap': 'Manufactured month'
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
        words = string.split(' ')
        return translate(words[0])
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
    df['A/C type'] = df['A/C type'].apply(translate)
    df['Transmission type'] = df['Transmission'].apply(translate)
    df['Gears'] = df['Transmission'].apply(get_gears)
    df['Documents'] = df['Documents type'].apply(translate)
    df['Condition'] = df['Condition'].apply(translate)
    df['Fuel'] = df['Fuel'].apply(translate)
    df['Paint'] = df['Color'].apply(get_color)
    df['Metallic'] = df['Color'].apply(is_metallic)

    # Drop unnecessary columns
    df.drop(['Transmission', 'Documents type', 'Évjárat', 'Color'], axis=1, inplace=True)

    # Save to file
    df.to_csv('../used_ford_focuses.csv', encoding='utf-8', index=False)
