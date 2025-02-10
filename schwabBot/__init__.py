# schwab.py

from datetime import datetime, timedelta
import urllib.parse as up
import time
import requests
from assets.helper_functions import modifiedAccountID
from assets.exception_handler import exception_handler
import json

class Schwab:
    def __init__(self, mongo, user, account_id, logger, push_notification, client):
        """
        Initializes a Schwab instance.

        Args:
            mongo: A MongoDB connection (or object) from which the 'users' collection is accessed.
            user (dict): The trader’s user data.
            account_id (str): The account identifier.
            logger: A logger object for logging messages.
            push_notification: A push notification object.
            client: An instance of the Client class (see client.py) for API calls.
        """
        self.user = user
        self.account_id = account_id
        self.logger = logger
        self.users = mongo.users
        self.push_notification = push_notification
        self.no_go_token_sent = False
        self.client_id = self.user["ClientID"]
        self.header = {}  # You may add additional headers if needed.
        self.client = client  # Client instance (from client.py)
        self.terminate = False
        self.invalid_count = 0

    @exception_handler
    def initialConnect(self):
        self.logger.info(
            f"CONNECTING {self.user['Name']} TO SCHWAB ({modifiedAccountID(self.account_id)})",
            extra={'log': False}
        )
        isValid = self.checkTokenValidity()
        if isValid:
            self.logger.info(
                f"CONNECTED {self.user['Name']} TO SCHWAB ({modifiedAccountID(self.account_id)})",
                extra={'log': False}
            )
            return True
        else:
            self.logger.error(
                f"FAILED TO CONNECT {self.user['Name']} TO SCHWAB ({self.account_id})",
                extra={'log': False}
            )
            return False

    @exception_handler
    def checkTokenValidity(self):
        """
        Checks if the current access token is still valid.
        Returns:
            bool: True if valid (or successfully refreshed), False otherwise.
        """
        # Get user data from the database
        user = self.users.find_one({"Name": self.user["Name"]})
        account = user["Accounts"][self.account_id]

        # Calculate token expiration (assuming created_at is a Unix timestamp and expires_in is in seconds)
        token_valid_until = account['created_at'] + account['expires_in']
        current_time = time.time()

        if current_time < token_valid_until - 60:
            return True

        # Refresh access token using the client (which handles OAuth calls)
        self.client._update_access_token()
        token_data = self.client._read_tokens_file()
        if token_data:
            new_expires_in = token_data[2].get('expires_in')
            self.users.update_one(
                {"Name": self.user["Name"]},
                {"$set": {
                    f"Accounts.{str(self.account_id)}.expires_in": new_expires_in,
                    f"Accounts.{str(self.account_id)}.access_token": token_data[2]["access_token"],
                    f"Accounts.{str(self.account_id)}.created_at": current_time
                }}
            )
        else:
            return False

        # Check refresh token expiry (stored as "YYYY-MM-DD")
        current_date = datetime.now().date()
        refresh_exp_date = datetime.strptime(account["refresh_exp_date"], "%Y-%m-%d").date()
        days_left = (refresh_exp_date - current_date).days
        if days_left <= 5:
            self.client._update_refresh_token()
            token_data = self.client._read_tokens_file()
            if token_data:
                self.users.update_one(
                    {"Name": self.user["Name"]},
                    {"$set": {
                        f"Accounts.{str(self.account_id)}.refresh_token": token_data[2]['refresh_token'],
                        f"Accounts.{str(self.account_id)}.refresh_exp_date": (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")
                    }}
                )
            else:
                return False

        return True

    @exception_handler
    def getAccount(self):
        """
        Retrieves account details using the account hash obtained via the client.
        """
        # Use the client's account_linked() method to get the linked account data.
        response = self.client.account_linked()  
        if not response.ok:
            raise Exception("Could not retrieve account hash from client's account_linked()")
        
        account_data = response.json()

        if not account_data or "hashValue" not in account_data[0]:
            raise Exception("Account hash not found in account_linked() response")
        
        account_hash = account_data[0]["hashValue"]
        fields = "positions,orders"
        url = f"{self.client._base_api_url}/trader/v1/accounts/{account_hash}?fields={fields}"
        
        return self.client.account_details(account_hash).json()

    @exception_handler
    def placeSchwabOrder(self, data):
        """
        Places an order by sending a POST request.
        """
        url = f"https://api.schwabapi.com/v1/accounts/{self.account_id}/orders"
        return self.sendRequest(url, method="POST", data=data)

    @exception_handler
    def getBuyingPower(self):
        """
        Retrieves the buying power from the account summary.
        """
        account = self.getAccount()
        
        buying_power = account["securitiesAccount"]["initialBalances"]["cashAvailableForTrading"]
        return float(buying_power)

    @exception_handler
    def sendRequest(self, url, method="GET", data=None):
        """
        Sends an HTTP request.
        """
        isValid = self.checkTokenValidity()
        if isValid:
            if method == "GET":
                resp = requests.get(url, headers=self.header)
                return resp.json()
            elif method == "POST":
                resp = requests.post(url, headers=self.header, json=data)
                return resp
            elif method == "PATCH":
                resp = requests.patch(url, headers=self.header, json=data)
                return resp
            elif method == "PUT":
                resp = requests.put(url, headers=self.header, json=data)
                return resp
            elif method == "DELETE":
                resp = requests.delete(url, headers=self.header)
                return resp
        else:
            return

    # (Other API endpoints such as quotes, orders, etc. can be added below if needed.)
