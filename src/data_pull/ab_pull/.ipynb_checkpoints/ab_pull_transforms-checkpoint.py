import numpy as np
import pandas as pd


def pull_df(download_url):
    df = pd.read_excel(download_url)
    return df


def clean_up_df(df):
    df = df.drop(columns=['Manager Name',
                          'Initial Effective Date',
                          'Site Address Line 2',
                          'Site Address Line 3',
                          'Fax Number',
                          'Mailing Address Line 1',
                          'Mailing Address Line 2',
                          'Mailing Address Line 3',
                          'Mailing City Name',
                          'Mailing Province Abbrev',
                          'Mailing Postal Code'])
    for column in df.columns:
        df[column] = df[column].fillna("").astype(
            str).apply(lambda x: x.strip())
    return df


def format_ab_query():
    ab_query = """
        SELECT Id, Name, Authorization_Number_AB__c
        FROM Account
        WHERE BillingState = 'AB'
        AND Id != '0016g00000MDjliAAD'
        """
    return ab_query


def find_new_stores(df, sf_df):
    num_list = list(sf_df['Authorization_Number_AB__c'])
    df = df.loc[~df['Authorization Number'].astype(
        float).isin(num_list)].reset_index(drop=True)
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
    df['Name'] = df['Establishment Name'].apply(
        lambda x: get_parent(x, parent_list))
    return df


def merge_parent_df(df, parent_df):
    df = pd.merge(df, parent_df, how='left', on='Name')
    df['Name'] = df['Name'].fillna(df['Establishment Name'])
    return df


def rename_columns(df):
    df = df.rename(columns={
        'Site City Name': 'BillingCity',
        'Site Address Line 1': 'BillingStreet',
        'Site Province Abbrev': 'BillingState',
        'Site Postal Code': 'BillingPostalCode',
        'Telephone Number': 'Phone',
        'Id': 'ParentId',
        'Authorization Number': 'Authorization_Number_AB__c'})
    return df


def broadcast_columns(df):
    df['Name'] = df['Name'] + ' - ' + \
        df['BillingStreet'] + ' - ' + df['BillingCity']
    df['Name__c'] = df['Name']
    df['RecordTypeId'] = '0126g0000007pRIAAY'
    df['Type'] = 'Retail Store'
    if len(df) > 0:
        df.loc[df['ParentId'].isna(), 'Retail_Store_Type__c'] = 'Independent'
        df.loc[~df['ParentId'].isna(), 'Retail_Store_Type__c'] = 'Banner'
    df['ParentId'] = df['ParentId'].replace({np.nan: None})
    df['Status__c'] = 'Open'
    df = df.drop(columns='Establishment Name')
    return df


def format_slack_message(row):
    account_name = row['Name']
    province = row['BillingState']
    status = row['Status__c']
    licence_number = row['Authorization_Number_AB__c']
    message = f"*New account* :house: \n```Name: {account_name} \nProvince: {province} \nStatus: {status} \nLicence: {licence_number}```"
    return message


def apply_format_slack_message(df):
    df['message'] = df.apply(format_slack_message, axis=1)
    return df


def get_message_list(df):
    message_list = list(df['message'])
    return message_list

