import pandas as pd
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta, MO


def get_last_monday():
    today = date.today()
    last_monday = today + relativedelta(weekday=MO(-1))
    last_monday = str(last_monday)
    return last_monday


def convert_gsheets_values_to_df(gs_values):
    df = pd.DataFrame(gs_values[1:], columns=gs_values[0])
    df = df.apply(lambda x: x.str.strip()).replace("", np.nan)
    return df


def adjustments_on_import(df):
    df = df.loc[~df['Account ID'].isna()].reset_index(drop=True)
    df = df.rename(columns={'Billing State/Province': 'BillingState'})
    return df

def format_sf_account_query():
    query = """
        SELECT Id
        FROM Account
        """
    return query

def quick_sf_acc_check(df, sf_account_df):
    sf_account_df['Id'] = sf_account_df['Id'].str[:-3]
    sf_account_id_list = list(sf_account_df['Id'])
    df = df.loc[df['Account ID'].isin(sf_account_id_list)].reset_index(drop=True)
    return df


def create_update_df(df):
    update_df = df[['Account ID', 'Website', 'Online Menu Available']]
    update_df = update_df.replace({np.NaN: None})
    update_df = update_df.rename(
        columns={'Account ID': 'Id', 'Online Menu Available': 'Online_Menu_Available__c'})
    update_df['Online_Menu_Available__c'] = update_df['Online_Menu_Available__c'].astype(int)
    return update_df


def make_boolean(df, column):
    yes_mask = df[column] == 'yes'
    df.loc[yes_mask, column] = True
    df.loc[~yes_mask, column] = False
    df[column] = df[column].astype(bool)
    return df


def clean_up_df(df):
    df = df.loc[df['Status'] == 'Open'].reset_index(drop=True)
    df = df.drop(columns='Status')
    for column in df.drop(columns=['Account ID', 'Account Name', 'BillingState']).columns:
        df = make_boolean(df, column)
    return df


def melt_df(df):
    product_list = [a for a in list(df.columns) if a.startswith(
        'AB') or a.startswith('ON') or a.startswith('BC')]
    df = pd.melt(df, id_vars=['Account ID',
                              'BillingState'], value_vars=product_list)
    df = df.loc[df['BillingState'] == df['variable'].apply(
        lambda x: x[:2])].reset_index(drop=True)
    df = df.rename(columns={'variable': 'Product Name',
                            'value': 'CarryingProduct', 'Account ID': 'Account__c'})
    df = df.drop(columns=['BillingState'])
    return df


def broadcast_columns(df, date):
    df['OwnerId'] = '0056g000004KxWzAAK'
    df['Check_In_Date__c'] = date
    df['upload__c'] = True
    return df


def create_check_in_df(df):
    check_in_df = df.drop_duplicates(
        subset=['Account__c', 'OwnerId', 'Check_In_Date__c', 'upload__c'], keep='first')
    check_in_df = check_in_df.drop(
        columns=['Product Name', 'CarryingProduct']).reset_index(drop=True)
    return check_in_df

def format_sf_product_query():
    query = """
        SELECT Id, Name
        FROM Product2
        WHERE IsActive = True
        """
    return query


def merge_product_df(df, product_df):
    product_df = product_df.rename(
        columns={'Id': 'Product__c', 'Name': 'Product Name'})
    df = pd.merge(df, product_df, how='left',
                  on='Product Name').reset_index(drop=True)
    df = df.rename(columns={'CarryingProduct': 'CarryingProduct__c'})
    df.loc[df['CarryingProduct__c'] == True,
           'Avg_Weekly_Product_Sell_Through_Units__c'] = 1
    df['Avg_Weekly_Product_Sell_Through_Units__c'] = df['Avg_Weekly_Product_Sell_Through_Units__c'].fillna(
        0)
    df = df.drop(columns=['CarryingProduct__c',
                          'Product Name', 'Check_In_Date__c'])
    return df


def format_sf_sc_query(sheet_name):
    query = f"""
        SELECT Id, Account__c
        FROM Store_Check_In__c
        WHERE upload__c = True
        AND Check_In_Date__c = {sheet_name}
        """
    return query

def merge_sc_df(df, sc_df):
    sc_df = sc_df.rename(columns={'Id': 'Store_Check_In__c'})
    sc_df['Account__c'] = sc_df['Account__c'].apply(lambda x: x[:-3])
    sc_df['Store_Check_In__c'] = sc_df['Store_Check_In__c'].apply(
        lambda x: x[:-3])
    df = pd.merge(df, sc_df, how='left', on='Account__c')
    df = df.drop(columns='Account__c')
    return df