#!/usr/bin/env python
# coding: utf-8

# # Piotrek Magiczny Parser v.0.2

# ## Imports

import glob, requests
import pandas as pd
import numpy as np
import re
from datetime import datetime


# ## Constants

INPUT_FOLDER_PATH: str = glob.glob('./input/*')[0]
OUTPUT_FOLDER_PATH: str = f"./output/Aktualna_Kolekcja_{str(datetime.now().strftime('%Y-%m-%d_%H:%M')).split('.')[0].replace(' ', '').replace(':', '').replace('-', '')}.csv"
CARD_EXCEPTIONS_FILE: str = './card_exceptions.xlsx'
SET_EXCEPTIONS_FILE: str = './set_exceptions.xlsx'


# # Functions

# ## Set's full name to abbreviation

def create_set_dictionary() -> dict:
    ''' Get set full names and abbreviations from Scryfall.com API. '''
    request = requests.get('https://api.scryfall.com/sets/')
    response: list[dict] = request.json()['data']

    set_dict: dict = [
        {'code': element['code'], 'name': element['name']} for element in response
    ]
    return set_dict


# ## Set names

def cleanse_set_name(set_name: str) -> str:
    ''' Delete unnecessary word additions to set names from MTGCB import. '''
    set_name_cleansed: str = set_name
    to_mass_replace: list[str] = [' Variants', ' Decks', ' Edition']
    mass_replace_exceptions: list[str] = ['Starter Commander Decks']
    
    # TODO
    # Replace regex ('cause it's slower) with manual index search to replace only one occurance at the end of string
    if set_name not in mass_replace_exceptions:
        for element in to_mass_replace:
            set_name_cleansed = re.sub(f'{element}$', '', set_name_cleansed)
    
    return set_name_cleansed


def find_correct_set(set_name: set, set_dicts: list[set]) -> str:
    ''' Check if set from MTGCB matches set in Scryfall database. '''
    correct_set: str = set_name
    
    try:
        correct_set = [ele for ele in set_dicts if ele['name'] == cleanse_set_name(set_name)][0]['code']
    except IndexError:
        correct_set = 'NOT FOUND'
        print(f"Couldn't find set named \"{set_name}\", cleansed set name is: \"{cleanse_set_name(set_name)}\"")

    return correct_set


# ## Card names

def cleanse_card_name(card_name: str) -> str:
    ''' Remove additional info in brackets from MTGCB. '''
    if "(" not in card_name:
        return card_name
    
    result: str = [ele for ele in card_name.split(" (")][0]

    return result


def decide_if_land(card_name: str) -> bool:
    ''' Check if it's a basic land, to set collector number in that card. '''
    lands: list = ['plains', 'island', 'swamp', 'mountain', 'forest']
    result: bool = True if card_name.lower() in lands else False
        
    return result


# ## Set exceptions

def handle_data_set_exceptions(row, index, set_exceptions, output_df) -> None:
    ''' Change set info according to set_exceptions.xlsx file. '''
    data: list[dict] = set_exceptions.to_dict('records')

    for record in data:
        if row['Set'] != record['Old Name']:
            continue
        
        output_df.at[index, 'Set'] = record['New Name']


# ## Card exceptions

def handle_data_card_exceptions(row, index, card_exceptions, output_df) -> None:
    ''' Change card info according to card_exceptions.xlsx file. '''
    data: list[dict] = card_exceptions.to_dict('records')

    for record in data:
        if record['Old Name']:
            if row['Name'] != record['Old Name']:
                continue
        if record['Old CN']:
            if str(row['Number']) != str(record['Old CN']):
                continue
        if record['Old Set']:
            if row['Set'] != record['Old Set']:
                continue
        
        if record['New Name']:
            output_df.at[index, 'Name'] = record['New Name']
        
        if record['New CN']:
            output_df.at[index, 'Collector Number'] = str(record['New CN'])
        
        if record['New Set']:
            output_df.at[index, 'Set'] = record['New Set']
        
        if record['To Delete']:
            output_df.drop(index, inplace=True)


# # Main

# ### Inits

df: pd.DataFrame = pd.read_csv(INPUT_FOLDER_PATH)
sets: dict = create_set_dictionary()
card_exceptions_df: pd.DataFrame = pd.read_excel(CARD_EXCEPTIONS_FILE).replace(np.nan, None)
set_exceptions_df: pd.DataFrame = pd.read_excel(SET_EXCEPTIONS_FILE).replace(np.nan, None)


# ### Change QTY to Count

df['Count'] = df['Qty']
df = df.drop('Qty', axis=1)


# ### Add Edition and Collector Number Columns

df.insert(len(df.columns), 'Edition', "")
df.insert(len(df.columns), 'Collector Number', "")


# ### Parse Input DF based on Excel Exceptions Sheets

for i, row in df.iterrows():
    handle_data_card_exceptions(row, i, card_exceptions_df, df)
    handle_data_set_exceptions(row, i, set_exceptions_df, df)


# ### Correct columns relevant to Moxfield's format

for i, row in df.iterrows():
    df.at[i, 'Name'] = cleanse_card_name(row['Name']).replace("Æ","Ae")
    df.at[i, 'Edition'] = find_correct_set(row['Set'], sets)
    df.at[i, 'Foil'] = "foil" if row['Foil'] else ''
    
    if not row['Collector Number']:
        df.at[i, 'Collector Number'] = row['Number'] if decide_if_land(cleanse_card_name(row['Name'])) else ""


# ### Drop unnecessary columns

df = df.drop('Set', axis=1)
df = df.drop('Low Price', axis=1)
df = df.drop('Rarity', axis=1)
df = df.drop('Number', axis=1)


# ### Finalize column order

order = ['Count', 'Name', 'Edition', 'Foil', 'Collector Number']
df = df[order]


# ### Save DF to CSV (output folder)

df.to_csv(OUTPUT_FOLDER_PATH, index=False)


# ### Create .py file from this notebook (.ipynb to .py)

get_ipython().system('jupyter nbconvert --to script main.ipynb --no-prompt')

