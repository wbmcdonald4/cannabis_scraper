from os import environ

import pandas as pd
from simple_salesforce import Salesforce


class SFDC:
    def __init__(self, client_id="Analytics pipeline"):

        username = environ.get("SF_USER")
        password = environ.get("SF_PASS")
        security_token = environ.get("SF_SECURITY_TOKEN")

        self.sf = Salesforce(
            username=username,
            password=password,
            security_token=security_token,
            client_id=client_id,
        )

        self.sf_bulk_object_dict = {
            "account": self.sf.bulk.Account,
            "opportunity": self.sf.bulk.Opportunity,
            "lead": self.sf.bulk.Lead,
            "user": self.sf.bulk.User,
            "contact": self.sf.bulk.Contact,
            "product": self.sf.bulk.Product2,
            "store_check_in": self.sf.bulk.Store_Check_In__c,
            "check_in_item": self.sf.bulk.Check_In_Item__c,
            "asset": self.sf.bulk.asset,
        }

        self.sf_object_dict = {
            "account": self.sf.Account,
            "opportunity": self.sf.Opportunity,
            "lead": self.sf.Lead,
            "user": self.sf.User,
            "contact": self.sf.Contact,
            "product": self.sf.Product2,
            "store_check_in": self.sf.Store_Check_In__c,
            "check_in_item": self.sf.Check_In_Item__c,
            "asset": self.sf.asset,
        }

    def query(self, soql_query):
        return self.sf.query_all(soql_query)

    def query_to_df(self, soql_query):
        soql_result = self.sf.query_all(soql_query)
        df = pd.DataFrame(soql_result.get("records"))
        if df.empty:
            return df
        else:
            df = df.drop(columns=["attributes"])
            return df

    @staticmethod
    def chunk_list(big_list, chunk_size):
        """DOCSTRING NEEDED"""

        chunked_list = [
            big_list[i: i + chunk_size] for i in range(0, len(big_list), chunk_size)
        ]
        return chunked_list

    def bulk_upsert(self, df, sf_object, _id):
        list_of_dicts = df.to_dict("records")
        chunked_list = SFDC.chunk_list(list_of_dicts, 200)
        list_length = len(list_of_dicts)
        counter = 0
        if list_length > 0:
            for chunk in chunked_list:
                result = self.sf_bulk_object_dict[sf_object].upsert(chunk, _id)
                counter += len([i for i in result if i["success"] == True])
                print(f"Performed upsert {counter} of {list_length} records")
        else:
            print(f"No records to upsert")

    def bulk_update(self, df, sf_object, _id):
        list_of_dicts = df.to_dict("records")
        chunked_list = SFDC.chunk_list(list_of_dicts, 200)
        list_length = len(list_of_dicts)
        counter = 0
        if list_length > 0:
            for chunk in chunked_list:
                result = self.sf_bulk_object_dict[sf_object].update(chunk, _id)
                counter += len([i for i in result if i["success"] == True])
                print(f"Performed update {counter} of {list_length} records")
        else:
            print(f"No records to update")

    def bulk_insert(self, df, sf_object):
        list_of_dicts = df.to_dict("records")
        chunked_list = SFDC.chunk_list(list_of_dicts, 200)
        list_length = len(list_of_dicts)
        counter = 0
        if list_length > 0:
            for chunk in chunked_list:
                result = self.sf_bulk_object_dict[sf_object].insert(chunk)
                counter += len([i for i in result if i["success"] == True])
                print(f"Performed insert {counter} of {list_length} records")
        else:
            print(f"No records to insert")

    def bulk_delete(self, df, sf_object):
        list_of_dicts = df.to_dict("records")
        chunked_list = SFDC.chunk_list(list_of_dicts, 200)
        list_length = len(list_of_dicts)
        counter = 0
        if list_length > 0:
            for chunk in chunked_list:
                result = self.sf_bulk_object_dict[sf_object].delete(chunk)
                counter += len([i for i in result if i["success"] == True])
                print(f"Performed delete {counter} of {list_length} records")
        else:
            print(f"No records to delete")

    def object_description(self, sf_object):
        desc = self.sf_object_dict[sf_object].describe()
        return desc

