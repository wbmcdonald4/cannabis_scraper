import numpy as np
import pandas as pd


def pull_df(download_url):
    df = pd.read_csv(download_url)
    return df


def clean_up_df(df):
    for column in df.columns:
        df[column] = df[column].fillna("").astype(
            str).apply(lambda x: x.strip())
    return df


def status_map(df):
    df['Status'] = df['Status'].map(
        {'Open': 'Open', 'Coming Soon': 'Pending', 'Closed': 'Closed'})
    return df


def split_df(df):
    public_mask = df['Licence'] == 'Public Store'
    public_df = df.loc[public_mask].reset_index(drop=True)
    df = df.loc[~public_mask].reset_index(drop=True)
    df['Licence'] = df['Licence'].astype(float)
    return df, public_df


def format_bc_query():
    bc_query = """
        SELECT Id, Name, BC_Licence_Number__c
        FROM Account
        WHERE BillingState = 'BC'
        AND Parent.Name != 'BCLDB'
        """
    return bc_query


def create_status_df(df, sf_df):
    status_df = pd.merge(df, sf_df, how='left',
                         left_on='Licence', right_on='BC_Licence_Number__c')
    status_df = status_df[['Id', 'Status']]
    status_list = list(status_df['Status'].unique())
    new_status_list = [a for a in status_list if a not in [
        'Open', 'Pending', 'Closed']]
    if len(new_status_list) > 0:
        print("new status, update script or salesforce")
        exit()
    else:
        print("No new Status")
    status_df = status_df.rename(columns={'Status': 'Status__c'})
    status_df = status_df.dropna()
    return status_df


def find_new_stores(df, sf_df):
    num_list = list(sf_df['BC_Licence_Number__c'])
    df = df.loc[~df['Licence'].isin(num_list)].reset_index(drop=True)
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


def check_for_new_parents(df):
    no_parent_mask = df['Id'].isna()
    new_parents_list = (df[no_parent_mask][df[no_parent_mask].duplicated(
        subset='Name', keep=False)]['Name'].unique())
    if len(new_parents_list) > 0:
        print(new_parents_list)
    else:
        print("no new parents")


def rename_columns(df):
    df = df.rename(columns={
        'City': 'BillingCity',
        'Address': 'BillingStreet',
        'Postal': 'BillingPostalCode',
        'Status': 'Status__c',
        'Id': 'ParentId',
        'Licence': 'BC_Licence_Number__c'})
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
    df['BillingState'] = 'BC'
    df = df.drop(columns='Establishment Name')
    return df


def format_bc_public_query():
    bc_public_query = """
        SELECT Id, Name, BillingStreet
        FROM Account
        WHERE BillingState = 'BC'
        AND Parent.Name = 'BCLDB'
        """
    return bc_public_query


def find_new_public_stores(df, sf_df):
    num_list = list(sf_df['BillingStreet'])
    df = df.loc[~df['Address'].isin(num_list)].reset_index(drop=True)
    return df


def broadcast_public_columns(df):
    df['Name'] = df['Establishment Name'] + ' - ' + \
        df['BillingStreet'] + ' - ' + df['BillingCity']
    df['Name__c'] = df['Name']
    df['RecordTypeId'] = '0126g0000007pRIAAY'
    df['Type'] = 'Retail Store'
    df['Retail_Store_Type__c'] = 'Banner'
    df['BillingState'] = 'BC'
    df['ParentId'] = '0016g00000ItSJ2AAN'
    df = df.drop(columns=['Establishment Name', 'BC_Licence_Number__c'])
    return df


def format_slack_message(row):
    account_name = row['Name']
    province = row['BillingState']
    status = row['Status__c']
    licence_number = row['BC_Licence_Number__c']
    message = f"*New account* :house: \n```Name: {account_name} \nProvince: {province} \nStatus: {status} \nLicence: {licence_number}```"
    return message


def apply_format_slack_message(df):
    df['message'] = df.apply(format_slack_message, axis=1)
    return df


def format_public_slack_message(row):
    account_name = row['Name']
    province = row['BillingState']
    status = row['Status__c']
    message = f"*New Public BC Account* :house: \n```Name: {account_name} \nProvince: {province} \nStatus: {status}```"
    return message


def apply_format_public_slack_message(df):
    df['message'] = df.apply(format_public_slack_message, axis=1)
    return df


def get_message_list(df):
    message_list = list(df['message'])
    return message_list
