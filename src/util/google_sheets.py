import os
import webbrowser

import requests


class GoogleSheets:
    """
    Google OAUTH PROCESS
    Step 1: Get Google client id/secret
            - https://developers.google.com/sheets/api/quickstart/python 
            - follow step 1 on the page (click enable google sheets api)
            - copy the client id & client secret from the overlay and paste it into your bash_profile as the following environment varibles:
                    - GOOGLE_CLIENT_ID
                    - GOOGLE_CLIENT_SECRET
    Step 2: Acquire refresh token
            - call GoogleSheets.get_refresh_token() and follow user consent process
            - you will be redirected to a link with the authorization code in the url (see example below)
            - http://localhost/?code=4/uAFtaVnOLgX5z-nlvntl_Zi-sq-B7ws7kiws46a_OTAF0G7MKiRqDi4UB5ClflEcBQ9qHIqYyC2gFDUISPJgT8Io&scope=https://www.googleapis.com/auth/drive'
            - the required authorization code is between 'code=' and '&scope'
            - copy the authorization code into the input parameter on the terminal where you called GoogleSheets.get_refresh_token()
            - The program will then instruct you to save the printed refresh token as the following environment variable:
                    - GOOGLE_REFRESH_TOKEN
    Step 3: You can now use this class as intended. A new access token is uptained upon class initallization has a 1 hour expiration time
    """

    def __init__(self):
        data = {
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
            "grant_type": "refresh_token",
            "refresh_token": os.getenv("GOOGLE_REFRESH_TOKEN"),
        }
        r = requests.post("https://oauth2.googleapis.com/token", data=data)
        self.access_token = r.json()["access_token"]
        self.headers = {
            "authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/vnd.api+json",
        }

    def get_google_sheet(self, google_sheet_id: str, sheet_name: str, _range: str):
        """_range is in A1 notation (i.e. A:I gives all rows for columns A to I)"""

        url = f"https://sheets.googleapis.com/v4/spreadsheets/{google_sheet_id}/values/{sheet_name}!{_range}"
        r = requests.get(url, headers=self.headers)
        values = r.json()["values"]
        return values

    @staticmethod
    def get_refresh_token():
        scope = "https://www.googleapis.com/auth/drive"
        redirect_uri = "http://localhost"
        authorization_url = f"https://accounts.google.com/o/oauth2/auth?client_id={os.getenv('GOOGLE_CLIENT_ID')}&redirect_uri={redirect_uri}&response_type=code&scope={scope}"
        webbrowser.open(authorization_url, new=2)
        authorization_code = input("Enter Google authorization Code:")
        data = {
            "client_id": os.getenv("GOOGLE_CLIENT_ID"),
            "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
            "redirect_uri": redirect_uri,
            "grant_type": "authorization_code",
            "code": authorization_code,
        }
        r = requests.post("https://oauth2.googleapis.com/token", data=data)
        print(
            f"Save '{r.json()['refresh_token']}' as environment variable GOOGLE_REFRESH_TOKEN"
        )
