from datetime import datetime, timedelta
import pytz
import time
from assets.exception_handler import exception_handler
from assets.current_datetime import getDatetime

class Tasks:
    def __init__(self):
        # MongoDB collection handles
        self.balance_history = self.mongo.balance_history
        self.profit_loss_history = self.mongo.profit_loss_history
        self.logger = self.logger
        self.midnight = False
        self.check_options = False
        self.isAlive = True
        self.no_ids_list = []
        # This attribute will track when the Schwab access token expires.
        self.token_expiration = None

    def ensure_valid_token(self):
        """
        Checks if the current Schwab access token is missing or expired.
        If so, calls Schwabdev's refresh() method to update the token.
        
        The Schwabdev documentation indicates that a token refresh
        returns (at minimum) an "access_token" and an "expires_in" value.
        We subtract a safety margin (e.g. 30 seconds) before setting the new expiration.
        """
        now = datetime.now(pytz.UTC)
        if self.token_expiration is None or now >= self.token_expiration:
            self.logger.info("Access token missing or expired – refreshing Schwab token...", extra={'log': False})
            # Use Schwabdev’s refresh method (see documentation)
            token_data = self.schwab.refresh()
            expires_in = token_data.get("expires_in", 3600)  # default to 3600 seconds if not provided
            # Set the new expiration a few seconds early to be safe
            self.token_expiration = now + timedelta(seconds=expires_in - 30)
            # Update the client's access token if needed.
            self.schwab.access_token = token_data["access_token"]

    @exception_handler
    def updateAccountBalance(self):
        """
        Updates the user's account balance in MongoDB.
        First ensures that the Schwab access token is valid, then calls
        Schwabdev's get_account() method to retrieve account info.
        """
        self.ensure_valid_token()
        self.logger.info("CONNECTING TO SCHWAB...", extra={'log': False})
        # Using Schwabdev’s get_account() method.
        account = self.schwab.get_account()
        # Adjust the following key names based on Schwabdev’s response format.
        liquidation_value = float(account["account"]["balances"]["cashAvailableForTrading"])
        self.users.update_one(
            {"Name": self.user["Name"]},
            {"$set": {f"Accounts.{self.id_token}.Account_Balance": liquidation_value}}
        )

    @exception_handler
    def getDatetimeSplit(self):
        dt = datetime.now(tz=pytz.UTC).replace(microsecond=0)
        dt_eastern = dt.astimezone(pytz.timezone('US/Eastern'))
        dt_str = datetime.strftime(dt_eastern, "%Y-%m-%d %H:00")
        dt_only, tm_only = dt_str.split(" ")
        return dt_only.strip(), tm_only.strip()

    @exception_handler
    def balanceHistory(self):
        """Saves the balance history (once per day) into MongoDB."""
        dt_only, _ = self.getDatetimeSplit()
        balance = self.user["Accounts"][str(self.id_token)]["Account_Balance"]
        balance_found = self.balance_history.find_one({
            "Date": dt_only,
            "Trader": self.user["Name"],
            "id_token": self.id_token
        })
        if not balance_found:
            self.balance_history.insert_one({
                "Trader": self.user["Name"],
                "Date": dt_only,
                "id_token": self.id_token,
                "Balance": balance
            })

    @exception_handler
    def profitLossHistory(self):
        """
        Calculates and stores the profit/loss for the day based on the closed positions.
        """
        dt_only, _ = self.getDatetimeSplit()
        profit_loss_found = self.profit_loss_history.find_one({
            "Date": dt_only,
            "Trader": self.user["Name"],
            "id_token": self.id_token
        })
        profit_loss = 0
        closed_positions = self.closed_positions.find({
            "Trader": self.user["Name"],
            "id_token": self.id_token
        })
        for position in closed_positions:
            sell_date = position["Sell_Date"].strftime("%Y-%m-%d")
            if sell_date == dt_only:
                buy_price = position["Buy_Price"]
                sell_price = position["Sell_Price"]
                qty = position["Qty"]
                profit_loss += ((sell_price * qty) - (buy_price * qty))
        if not profit_loss_found:
            self.profit_loss_history.insert_one({
                "Trader": self.user["Name"],
                "Date": dt_only,
                "id_token": self.id_token,
                "Profit_Loss": profit_loss
            })

    @exception_handler
    def killQueueOrder(self):
        """
        Checks the orders in the queue; if an order is older than 2 hours
        (and is still in a pending status), it will be cancelled.
        Also, if an order older than 10 minutes is missing an order ID,
        an alert is sent.
        """
        queue_orders = self.queue.find({
            "Trader": self.user["Name"],
            "id_token": self.id_token
        })
        dt = datetime.now(tz=pytz.UTC).replace(microsecond=0)
        dt_eastern = dt.astimezone(pytz.timezone('US/Eastern'))
        two_hours_ago = dt_eastern - timedelta(hours=2)
        ten_minutes_ago = dt_eastern - timedelta(minutes=10)
        for order in queue_orders:
            order_date = order["Date"]
            order_type = order["Order_Type"]
            order_id = order["Order_ID"]
            forbidden = ["REJECTED", "CANCELED", "FILLED"]
            # If the order is older than two hours, and is a BUY order that hasn't been cancelled, then cancel it.
            if two_hours_ago > order_date and order_type in ["BUY", "BUY_TO_OPEN"] and order_id is not None and order["Order_Status"] not in forbidden:
                self.ensure_valid_token()  # Ensure the token is valid before cancelling.
                # Use Schwabdev’s cancel_order() method.
                resp = self.schwab.cancel_order(order_id)
                # Check response status (adjust based on Schwabdev's response object)
                if hasattr(resp, "status_code") and resp.status_code in [200, 201]:
                    other = {
                        "Symbol": order["Symbol"],
                        "Order_Type": order["Order_Type"],
                        "Order_Status": "CANCELED",
                        "Strategy": order["Strategy"],
                        "id_token": self.id_token,
                        "Trader": self.user["Name"],
                        "Date": getDatetime()
                    }
                    self.other.insert_one(other)
                    self.queue.delete_one({
                        "Trader": self.user["Name"],
                        "Symbol": order["Symbol"],
                        "Strategy": order["Strategy"]
                    })
                    self.logger.INFO(f"CANCELED ORDER FOR {order['Symbol']} - TRADER: {self.user['Name']}", True)
            # Alert if an order is older than 10 minutes but has no order ID.
            if ten_minutes_ago > order_date and order_id is None and order["id_token"] == self.id_token:
                if order["Symbol"] not in self.no_ids_list:
                    self.logger.ERROR(
                        "QUEUE ORDER ID ERROR",
                        f"ORDER ID FOR {order['Symbol']} NOT FOUND - TRADER: {self.user['Name']} - ACCOUNT ID: {self.id_token}"
                    )
                    self.no_ids_list.append(order["Symbol"])
            else:
                if order["Symbol"] in self.no_ids_list:
                    self.no_ids_list.remove(order["Symbol"])

    @exception_handler
    def sellOptionsAtExpiration(self):
        """
        Checks open OPTION positions and, if one day before expiration,
        sends an order to sell the option.
        """
        open_positions = self.open_positions.find({
            "Trader": self.user["Name"],
            "Asset_Type": "OPTION"
        })
        dt = getDatetime()
        for position in open_positions:
            day_before = (position["Exp_Date"] - timedelta(days=1)).strftime("%Y-%m-%d")
            if day_before == dt.strftime("%Y-%m-%d"):
                trade_data = {
                    "Symbol": position["Symbol"],
                    "Pre_Symbol": position["Pre_Symbol"],
                    "Side": "SELL_TO_CLOSE",
                    "Option_Type": position["Option_Type"],
                    "Strategy": position["Strategy"],
                    "Asset_Type": position["Asset_Type"],
                    "Exp_Date": position["Exp_Date"]
                }
                self.placeOrder(trade_data, position)

    @exception_handler
    def updateStrategiesObject(self, strategy):
        """
        Updates the strategies object in MongoDB.
        If the strategy does not exist, adds it with a default
        Position_Size of 1 and sets it as Active.
        """
        self.users.update(
            {"Name": self.user["Name"], f"Accounts.{self.id_token}.Strategies.{strategy}": {"$exists": False}},
            {"$set": {f"Accounts.{self.id_token}.Strategies.{strategy}": {"Position_Size": 1, "Active": True}}}
        )

    def runTasks(self):
        """
        Runs tasks on a loop, with a sleep interval that varies by time of day.
        Tasks include:
          - Cancelling stale queued orders
          - Updating the account balance
          - End-of-day logging of balance and profit/loss
          - Option expirations handling
        """
        self.logger.INFO(f"STARTING TASKS FOR TRADER {self.user['Name']} - ACCOUNT ID: {self.id_token}\n")

        def selectSleep():
            """
            Returns a sleep duration (in seconds) based on the current Eastern Time.
            Pre-market (04:00–09:30), Market hours (09:30–20:00): 5 seconds;
            Otherwise (including weekends): 60 seconds.
            """
            dt = datetime.now(tz=pytz.UTC).replace(microsecond=0)
            dt_eastern = dt.astimezone(pytz.timezone('US/Eastern'))
            day = dt_eastern.strftime("%a")
            tm = dt_eastern.strftime("%H:%M:%S")
            weekends = ["Sat", "Sun"]
            if tm > "20:00" or tm < "04:00" or day in weekends:
                return 60
            return 5

        while self.isAlive:
            try:
                self.killQueueOrder()
                self.updateAccountBalance()
                dt = datetime.now(tz=pytz.UTC).replace(microsecond=0)
                dt_eastern = dt.astimezone(pytz.timezone('US/Eastern'))
                tm = dt_eastern.time().strftime("%H:%M")
                # Sell options at expiration at 09:30 ET (adjust if needed)
                if tm == "09:30":
                    if not self.check_options:
                        self.sellOptionsAtExpiration()
                        self.check_options = True
                else:
                    self.check_options = False
                # End-of-day processing at 23:55 ET
                if tm == "23:55":
                    if not self.midnight:
                        self.balanceHistory()
                        self.profitLossHistory()
                        self.midnight = True
                else:
                    self.midnight = False
            except KeyError:
                self.isAlive = False
            except Exception:
                self.logger.ERROR(f"ACCOUNT ID: {self.id_token} - TRADER: {self.user['Name']}")
            finally:
                time.sleep(selectSleep())

        self.logger.INFO(f"TASK STOPPED FOR ACCOUNT ID {self.id_token}")
