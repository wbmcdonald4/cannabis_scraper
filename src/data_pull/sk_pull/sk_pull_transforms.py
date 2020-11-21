import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

def get_soup(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    return soup

def pull_tag_list(soup):
    tag_list = []
    for city in soup.find("div", id='content').find_all('h1'):
        next_node = city
        city_list = []
        while True:
            next_node = next_node.find_next('p')
            if 'style' in next_node.attrs:
                if next_node.attrs['style'] == 'text-align: right;':
                    break
                else:
                    type = 'line'
                    city_list.append({'type':type, 'node':next_node})
            else:
                type = 'grouped'
                city_list.append({'type':type, 'node':next_node})
        tag_list.extend(city_list)
    return tag_list

def split_into_groups(tag_list):
    grouped_list = [a['node'] for a in tag_list if a['type'] == 'grouped']
    line_list = [a['node'] for a in tag_list if a['type'] == 'line']
    return grouped_list, line_list

def pull_data_from_grouped_list(grouped_list):
    store_list = []
    for group in grouped_list:
        data = group.text.split('\n')
        name = data[0]
        street_address = data[1]
        city = data[2]
        if len(data) > 3:
            website = data[3]
            store_dict = {'store_name':name, 'BillingStreet':street_address, 'BillingCity':city, 'Website':website}
            store_list.append(store_dict)
        else:
            store_dict = {'store_name':name, 'BillingStreet':street_address, 'BillingCity':city}
            store_list.append(store_dict)
    return store_list

def pull_data_from_line_list(line_list):
    group_list = []
    i=0
    for line in line_list:
        if 'strong' in [a.name for a in line.find_all()]:
            i += 1
        group_dict = {'group':i, 'line':line}
        group_list.append(group_dict)

    store_list = []
    for x in range(1, i+1):
        store_dict = {}
        data = [a['line'].text for a in group_list if a['group'] == x]

        if len(data) == 1:
            data = data[0].split('\n')
        name = data[0]
        street_address = data[1]
        city = data[2]
        if len(data) > 3:
            website = data[3]
            store_dict = {'store_name':name, 'BillingStreet':street_address, 'BillingCity':city, 'Website':website}
            store_list.append(store_dict)
        else:
            store_dict = {'store_name':name, 'BillingStreet':street_address, 'BillingCity':city}
            store_list.append(store_dict)
    return store_list

def combine_lists(list_1, list_2):
    final_list = list_1 + list_2
    df = pd.DataFrame(final_list)
    return df


def clean_up_df(df):
    for column in df.columns:
        df[column] = df[column].fillna("").astype(str).apply(lambda x: x.strip())
    df['BillingCity'] = df['BillingCity'].apply(lambda x: x.split(",")[0])
    df['BillingCity'] = df['BillingCity'].apply(lambda x: x.split(" SK")[0])
    df['BillingState'] = 'SK'
    df['Status__c'] = 'Open'
    return df

def format_sk_query():
    on_query = """
        SELECT Id, Name, BillingStreet
        FROM Account
        WHERE BillingState = 'SK'
        """
    return on_query

def find_new_stores(df, sf_df):
    street_list = list(sf_df['BillingStreet'])
    df = df.loc[~df['BillingStreet'].isin(street_list)].reset_index(drop=True)
    return df

def format_parent_query():
    parent_query = """
        SELECT Id, Name
        FROM Account
        WHERE Type = 'Corporate Parent'
        """
    return parent_query

def get_parent_list(parent_df):
    parent_list = list(parent_df['Name'])
    return parent_list

def get_parent(x, parent_list):
    z = [a for a in parent_list if a in x]
    if len(z) > 0:
        x = z[0]
    else:
        x = None
    return x

def apply_get_parent(df, parent_list):
    df['Name'] = df['store_name'].apply(lambda x: get_parent(x, parent_list))
    return df

def merge_parent_df(df, parent_df):
    df = pd.merge(df, parent_df, how='left', on='Name')
    df['Name'] = df['Name'].fillna(df['store_name'])
    return df

# this is a temporary function. there needs to be a way that checks for accounts with the same nam in the crm
def check_for_new_parents(df):
    no_parent_mask = df['Id'].isna()
    new_parents_list = (df[no_parent_mask][df[no_parent_mask].duplicated(subset='Name', keep=False)]['Name'].unique())
    if len(new_parents_list) > 0:
        print(new_parents_list)
    else:
        print("no new parents")

def rename_columns(df):
    df = df.rename(columns={'Id':'ParentId'})
    return df

def broadcast_columns(df):
    df['Name'] = df['Name'] + ' - ' + df['BillingStreet'] + ' - ' + df['BillingCity']
    df['Name__c'] = df['Name']
    df['RecordTypeId'] = '0126g0000007pRIAAY'
    df['Type'] = 'Retail Store'
    if len(df) > 0:
        df.loc[df['ParentId'].isna(), 'Retail_Store_Type__c'] = 'Independent'
        df.loc[~df['ParentId'].isna(), 'Retail_Store_Type__c'] = 'Banner'
    df['ParentId'] = df['ParentId'].replace({np.nan: None})
    df = df.drop(columns='store_name')
    return df

def format_slack_message(row):
    account_name = row['Name']
    province = row['BillingState']
    status = row['Status__c']
    licence_number = row['BillingStreet']
    message = f"*New account* :house: \n```Name: {account_name} \nProvince: {province} \nStatus: {status} \nBilling Street: {licence_number}```"
    return message


def apply_format_slack_message(df):
    df['message'] = df.apply(format_slack_message, axis=1)
    return df


def get_message_list(df):
    message_list = list(df['message'])
    return message_list