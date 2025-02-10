# client.py

import json
import time
import base64
import datetime
import requests
import threading
import webbrowser
import urllib.parse
from .stream import Stream  # Ensure that the stream module is correctly located
import logger  # Adjust as needed for your logging module

class Client:
    def __init__(self, app_key, app_secret, callback_url="https://127.0.0.1", tokens_file="tokens.json", timeout=5, verbose=False, update_tokens_auto=True):
        """
        Initializes a client to access the Schwab API.

        Args:
            app_key (str): The application key.
            app_secret (str): The application secret.
            callback_url (str): The callback URL (must be https).
            tokens_file (str): Path to the JSON file for storing tokens.
            timeout (int): Request timeout in seconds.
            verbose (bool): If True, prints extra information.
            update_tokens_auto (bool): If True, starts a background thread to update tokens automatically.
        """
        if app_key is None:
            raise Exception("app_key cannot be None.")
        elif app_secret is None:
            raise Exception("app_secret cannot be None.")
        elif callback_url is None:
            raise Exception("callback_url cannot be None.")
        elif tokens_file is None:
            raise Exception("tokens_file cannot be None.")
        elif len(app_key) != 32 or len(app_secret) != 16:
            raise Exception("App key or app secret invalid length.")
        elif callback_url[0:5] != "https":
            raise Exception("callback_url must be https.")
        elif callback_url[-1] == "/":
            raise Exception("callback_url cannot be path (ends with \"/\").")
        elif tokens_file[-1] == '/':
            raise Exception("Tokens file cannot be path.")
        elif timeout <= 0:
            raise Exception("Timeout must be greater than 0 and is recommended to be 5 seconds or more.")

        self._app_key = app_key
        self._app_secret = app_secret
        self._callback_url = callback_url
        self.access_token = None
        self.refresh_token = None
        self.id_token = None
        self._access_token_issued = None
        self._refresh_token_issued = None
        self._access_token_timeout = 1800  # seconds
        self._refresh_token_timeout = 7    # days
        self._tokens_file = tokens_file
        self.timeout = timeout
        self.verbose = verbose
        self.stream = Stream(self)
        self.awaiting_input = False

        # Try to load tokens from the tokens file.
        at_issued, rt_issued, token_dictionary = self._read_tokens_file()
        if None not in [at_issued, rt_issued, token_dictionary]:
            self.access_token = token_dictionary.get("access_token")
            self.refresh_token = token_dictionary.get("refresh_token")
            self.id_token = token_dictionary.get("id_token")
            self._access_token_issued = at_issued
            self._refresh_token_issued = rt_issued
            if self.verbose:
                print(self._access_token_issued.strftime("Access token last updated: %Y-%m-%d %H:%M:%S") +
                      f" (expires in {self._access_token_timeout - (datetime.datetime.now(datetime.timezone.utc) - self._access_token_issued).seconds} seconds)")
                print(self._refresh_token_issued.strftime("Refresh token last updated: %Y-%m-%d %H:%M:%S") +
                      f" (expires in {self._refresh_token_timeout - (datetime.datetime.now(datetime.timezone.utc) - self._refresh_token_issued).days} days)")
            self.update_tokens()
        else:
            if self.verbose:
                print(f"Token file does not exist or invalid formatting, creating \"{tokens_file}\"")
            open(self._tokens_file, 'w').close()
            self._update_refresh_token()

        if update_tokens_auto:
            def checker():
                while True:
                    self.update_tokens()
                    time.sleep(60)
            threading.Thread(target=checker, daemon=True).start()
        elif not self.verbose:
            print("Warning: Tokens will not be updated automatically.")

        if self.verbose:
            print("Schwabdev Client Initialization Complete")

    def update_tokens(self, force=False):
        if (datetime.datetime.now(datetime.timezone.utc) - self._refresh_token_issued).days >= (self._refresh_token_timeout - 1) or force:
            print("The refresh token has expired, please update!")
            self._update_refresh_token()
        elif ((datetime.datetime.now(datetime.timezone.utc) - self._access_token_issued).days >= 1) or (
                (datetime.datetime.now(datetime.timezone.utc) - self._access_token_issued).seconds > (self._access_token_timeout - 61)):
            if self.verbose:
                print("The access token has expired, updating automatically.")
            self._update_access_token()

    def update_tokens_auto(self):
        import warnings
        warnings.warn("update_tokens_auto() is deprecated and is now started when the client is created (if update_tokens_auto=True (default)).", DeprecationWarning, stacklevel=2)

    def _update_access_token(self):
        access_token_time_old, refresh_token_issued, token_dictionary_old = self._read_tokens_file()
        for i in range(3):
            response = self._post_oauth_token('refresh_token', token_dictionary_old.get("refresh_token"))
            if response.ok:
                self._access_token_issued = datetime.datetime.now(datetime.timezone.utc)
                self._refresh_token_issued = refresh_token_issued
                new_td = response.json()
                self.access_token = new_td.get("access_token")
                self.refresh_token = new_td.get("refresh_token")
                self.id_token = new_td.get("id_token")
                self._write_tokens_file(self._access_token_issued, refresh_token_issued, new_td)
                if self.verbose:
                    print(f"Access token updated: {self._access_token_issued}")
                break
            else:
                print(response.text)
                print(f"Could not get new access token ({i+1} of 3).")
                time.sleep(10)

    def _update_refresh_token(self):
        self.awaiting_input = True
        auth_url = f'https://api.schwabapi.com/v1/oauth/authorize?client_id={self._app_key}&redirect_uri={self._callback_url}'
        print(f"Open to authenticate: {auth_url}")
        webbrowser.open(auth_url)
        response_url = input("After authorizing, paste the address bar url here: ")
        code = f"{response_url[response_url.index('code=') + 5:response_url.index('%40')]}@"
        response = self._post_oauth_token('authorization_code', code)
        if response.ok:
            self._access_token_issued = self._refresh_token_issued = datetime.datetime.now(datetime.timezone.utc)
            new_td = response.json()
            self.access_token = new_td.get("access_token")
            self.refresh_token = new_td.get("refresh_token")
            self.awaiting_input = False
            self.id_token = new_td.get("id_token")
            self._write_tokens_file(self._access_token_issued, self._refresh_token_issued, new_td)
            if self.verbose:
                print("Refresh and Access tokens updated")
        else:
            print(response.text)
            print("Could not get new refresh and access tokens, check these:\n    1. App status is \"Ready For Use\".\n    2. App key and app secret are valid.\n    3. You pasted the whole url within 30 seconds. (it has a quick expiration)")

    def _post_oauth_token(self, grant_type, code):
        headers = {
            'Authorization': f'Basic {base64.b64encode(bytes(f"{self._app_key}:{self._app_secret}", "utf-8")).decode("utf-8")}',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        if grant_type == 'authorization_code':
            data = {'grant_type': 'authorization_code', 'code': code, 'redirect_uri': self._callback_url}
        elif grant_type == 'refresh_token':
            data = {'grant_type': 'refresh_token', 'refresh_token': code}
        else:
            raise Exception("Invalid grant type; options are 'authorization_code' or 'refresh_token'")
        return requests.post('https://api.schwabapi.com/v1/oauth/token', headers=headers, data=data)

    def _write_tokens_file(self, at_issued, rt_issued, token_dictionary):
        try:
            with open(self._tokens_file, 'w') as f:
                toWrite = {"access_token_issued": at_issued.isoformat(), "refresh_token_issued": rt_issued.isoformat(),
                           "token_dictionary": token_dictionary}
                json.dump(toWrite, f, ensure_ascii=False, indent=4)
                f.flush()
        except Exception as e:
            print(e)

    def _read_tokens_file(self):
        try:
            with open(self._tokens_file, 'r') as f:
                d = json.load(f)
                return (datetime.datetime.fromisoformat(d.get("access_token_issued")),
                        datetime.datetime.fromisoformat(d.get("refresh_token_issued")),
                        d.get("token_dictionary"))
        except Exception as e:
            print(e)
            return None, None, None

    def _params_parser(self, params):
        for key in list(params.keys()):
            if params[key] is None:
                del params[key]
        return params

    def _time_convert(self, dt=None, form="8601"):
        if dt is None or isinstance(dt, str):
            return dt
        elif form == "8601":
            return f'{dt.isoformat()[:-9]}Z'
        elif form == "epoch":
            return int(dt.timestamp())
        elif form == "epoch_ms":
            return int(dt.timestamp() * 1000)
        elif form == "YYYY-MM-DD":
            return dt.strftime("%Y-%m-%d")
        else:
            return dt

    def _format_list(self, l: list | str | None):
        if l is None:
            return None
        elif isinstance(l, list):
            return ",".join(l)
        else:
            return l

    _base_api_url = "https://api.schwabapi.com"

    # ------------------------------
    # API Endpoints
    # ------------------------------

    def account_linked(self) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/accountNumbers',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            timeout=self.timeout)

    def account_details_all(self, fields=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'fields': fields}),
                            timeout=self.timeout)

    def account_details(self, accountHash: str, fields=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'fields': fields}),
                            timeout=self.timeout)

    def account_orders(self, accountHash: str, fromEnteredTime: 'datetime | str', toEnteredTime: 'datetime | str', maxResults=None, status=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/orders',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser(
                                {'maxResults': maxResults,
                                 'fromEnteredTime': self._time_convert(fromEnteredTime, "8601"),
                                 'toEnteredTime': self._time_convert(toEnteredTime, "8601"),
                                 'status': status}),
                            timeout=self.timeout)

    def order_place(self, accountHash: str, order: dict) -> requests.Response:
        return requests.post(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/orders',
                             headers={"Accept": "application/json", 'Authorization': f'Bearer {self.access_token}',
                                      "Content-Type": "application/json"},
                             json=order,
                             timeout=self.timeout)

    def order_details(self, accountHash: str, orderId: int | str) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/orders/{orderId}',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            timeout=self.timeout)

    def order_cancel(self, accountHash: str, orderId: int | str) -> requests.Response:
        return requests.delete(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/orders/{orderId}',
                               headers={'Authorization': f'Bearer {self.access_token}'},
                               timeout=self.timeout)

    def order_replace(self, accountHash: str, orderId: int | str, order: dict) -> requests.Response:
        return requests.put(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/orders/{orderId}',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {self.access_token}',
                                     "Content-Type": "application/json"},
                            json=order,
                            timeout=self.timeout)

    def account_orders_all(self, fromEnteredTime: 'datetime | str', toEnteredTime: 'datetime | str', maxResults=None, status=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/orders',
                            headers={"Accept": "application/json", 'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser(
                                {'maxResults': maxResults,
                                 'fromEnteredTime': self._time_convert(fromEnteredTime, "8601"),
                                 'toEnteredTime': self._time_convert(toEnteredTime, "8601"),
                                 'status': status}),
                            timeout=self.timeout)

    def transactions(self, accountHash: str, startDate: 'datetime | str', endDate: 'datetime | str', types: str, symbol=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/transactions',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser(
                                {'accountNumber': accountHash,
                                 'startDate': self._time_convert(startDate, "8601"),
                                 'endDate': self._time_convert(endDate, "8601"),
                                 'symbol': symbol,
                                 'types': types}),
                            timeout=self.timeout)

    def transaction_details(self, accountHash: str, transactionId: str | int) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/accounts/{accountHash}/transactions/{transactionId}',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params={'accountNumber': accountHash, 'transactionId': transactionId},
                            timeout=self.timeout)

    def preferences(self) -> requests.Response:
        return requests.get(f'{self._base_api_url}/trader/v1/userPreference',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            timeout=self.timeout)

    def quotes(self, symbols=None, fields=None, indicative=False) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/quotes',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser(
                                {'symbols': self._format_list(symbols),
                                 'fields': fields,
                                 'indicative': indicative}),
                            timeout=self.timeout)

    def quote(self, symbol_id: str, fields=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/{urllib.parse.quote(symbol_id)}/quotes',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'fields': fields}),
                            timeout=self.timeout)

    def option_chains(self, symbol: str, contractType=None, strikeCount=None, includeUnderlyingQuote=None, strategy=None,
                      interval=None, strike=None, range=None, fromDate=None, toDate=None, volatility=None, underlyingPrice=None,
                      interestRate=None, daysToExpiration=None, expMonth=None, optionType=None, entitlement=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/chains',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser(
                                {'symbol': symbol,
                                 'contractType': contractType,
                                 'strikeCount': strikeCount,
                                 'includeUnderlyingQuote': includeUnderlyingQuote,
                                 'strategy': strategy,
                                 'interval': interval,
                                 'strike': strike,
                                 'range': range,
                                 'fromDate': self._time_convert(fromDate, "YYYY-MM-DD"),
                                 'toDate': self._time_convert(toDate, "YYYY-MM-DD"),
                                 'volatility': volatility,
                                 'underlyingPrice': underlyingPrice,
                                 'interestRate': interestRate,
                                 'daysToExpiration': daysToExpiration,
                                 'expMonth': expMonth,
                                 'optionType': optionType,
                                 'entitlement': entitlement}),
                            timeout=self.timeout)

    def option_expiration_chain(self, symbol: str) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/expirationchain',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'symbol': symbol}),
                            timeout=self.timeout)

    def price_history(self, symbol: str, periodType=None, period=None, frequencyType=None, frequency=None, startDate=None,
                      endDate=None, needExtendedHoursData=None, needPreviousClose=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/pricehistory',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'symbol': symbol,
                                                        'periodType': periodType,
                                                        'period': period,
                                                        'frequencyType': frequencyType,
                                                        'frequency': frequency,
                                                        'startDate': self._time_convert(startDate, 'epoch_ms'),
                                                        'endDate': self._time_convert(endDate, 'epoch_ms'),
                                                        'needExtendedHoursData': needExtendedHoursData,
                                                        'needPreviousClose': needPreviousClose}),
                            timeout=self.timeout)

    def movers(self, symbol: str, sort=None, frequency=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/movers/{symbol}',
                            headers={"accept": "application/json",
                                     'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'sort': sort, 'frequency': frequency}),
                            timeout=self.timeout)

    def market_hours(self, symbols, date=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/markets',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'markets': symbols,
                                                        'date': self._time_convert(date, 'YYYY-MM-DD')}),
                            timeout=self.timeout)

    def market_hour(self, market_id: str, date=None) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/markets/{market_id}',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params=self._params_parser({'date': self._time_convert(date, 'YYYY-MM-DD')}),
                            timeout=self.timeout)

    def instruments(self, symbol: str, projection) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/instruments',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            params={'symbol': symbol, 'projection': projection},
                            timeout=self.timeout)

    def instrument_cusip(self, cusip_id: str | int) -> requests.Response:
        return requests.get(f'{self._base_api_url}/marketdata/v1/instruments/{cusip_id}',
                            headers={'Authorization': f'Bearer {self.access_token}'},
                            timeout=self.timeout)
