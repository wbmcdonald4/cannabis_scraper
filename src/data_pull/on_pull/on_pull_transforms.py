import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup

def get_num_pages(base_url):
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    last_page_link = soup.find("li", class_="pager-last last").a['href']
    num_pages = int(last_page_link[-2:]) + 1
    return num_pages

def scrape_website(num_pages, base_url):
    print("Scraping", num_pages, "pages...")
    list_of_stores = []
    for i in range(num_pages):
        url = f'{base_url}?page={i}'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        tbody = soup.find('tbody')
        for row in tbody.find_all('tr'):
            city = row.find('td', class_="views-field views-field-cannabis-license-city active").text.strip()
            store_name = row.find('td', class_="views-field views-field-cannabis-license-name").text.strip()
            address = row.find('td', class_="views-field views-field-cannabis-license-address").text.strip()
            status = row.find('td', class_="views-field views-field-cannabis-license-status").text.strip()
            store_dict = {'BillingCity':city, 'Establishment Name':store_name, 'BillingStreet':address, 'Status__c':status}
            list_of_stores.append(store_dict)
    df = pd.DataFrame(list_of_stores)
    return df

def clean_up_df(df):
    for column in df.columns:
        df[column] = df[column].fillna("").astype(str).apply(lambda x: x.strip())
        df[column] = df[column].apply(lambda x: x.title())
    df = df.drop_duplicates(subset ='BillingStreet', keep='first').reset_index(drop=True)
    return df

def status_map(df):
    df.loc[df['Status__c'].str.startswith('Public Notice'), 'Status__c'] = 'Public Notice'
    df['Status__c'] = df['Status__c'].map({'Public Notice':'Pending', 'Authorized To Open':'Open', 'In Progress':'Pending'})
    return df

def format_on_query():
    on_query = """
        SELECT Id, Name, BillingStreet
        FROM Account
        WHERE BillingState = 'ON'
        """
    return on_query

def create_status_df(df, sf_df):
    status_df = pd.merge(df, sf_df, how='left', on='BillingStreet')
    status_df = status_df[['Id','Status__c']]
    status_list = list(status_df['Status__c'].unique())
    new_status_list = [a for a in status_list if a not in ['Open', 'Pending', 'Closed']]
    if len(new_status_list) > 0:
        print("new status, update script or salesforce")
        exit()
    else:
        print("No new Status")
    status_df = status_df.dropna()
    return status_df

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
    df['Name'] = df['Establishment Name'].apply(lambda x: get_parent(x, parent_list))
    return df

def merge_parent_df(df, parent_df):
    df = pd.merge(df, parent_df, how='left', on='Name')
    df['Name'] = df['Name'].fillna(df['Establishment Name'])
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
    df.loc[df['ParentId'].isna(), 'Retail_Store_Type__c'] = 'Independent'
    df.loc[~df['ParentId'].isna(), 'Retail_Store_Type__c'] = 'Banner'
    df['ParentId'] = df['ParentId'].replace({np.nan: None})
    df['BillingState'] = 'ON'
    df = df.drop(columns='Establishment Name')
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