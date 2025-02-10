from assets.helper_functions import getDatetime, modifiedAccountID
from api_trader.tasks import Tasks
from threading import Thread
from assets.exception_handler import exception_handler
from api_trader.order_builder import OrderBuilder
from dotenv import load_dotenv
from datetime import datetime, timedelta
from pathlib import Path
import os
from pymongo.errors import WriteError, WriteConcernError
import traceback
import time
from random import randint
import requests
from schwabBot import Schwab  # Assumes your updated Schwab class (with checkTokenValidity) is imported here
import json

THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
path = Path(THIS_FOLDER)
load_dotenv(dotenv_path=f"{path.parent}/config.env")
RUN_TASKS = True if os.getenv('RUN_TASKS') == "True" else False


class ApiTrader(Tasks, OrderBuilder):

    def __init__(self, user, mongo, push, logger, account_id, schwab, client):
        """
        Args:
            user (dict): USER DATA FOR CURRENT INSTANCE
            mongo (object): MONGO OBJECT CONNECTING TO DB
            push (object): PUSH OBJECT FOR PUSH NOTIFICATIONS
            logger (object): LOGGER OBJECT FOR LOGGING
            account_id (str): USER ACCOUNT ID FOR SCHWAB
            schwab (object): SCHWAB API CLIENT (instance of Schwabdev.Schwab)
            client (object): API CLIENT FOR ORDER PLACEMENT
        """
        self.RUN_LIVE_TRADER = True if user["Accounts"][str(account_id)]["Account_Position"] == "Live" else False
        self.schwab = schwab
        self.client = client
        self.mongo = mongo
        self.account_id = account_id
        self.user = user
        self.users = mongo.users
        self.push = push
        self.open_positions = mongo.open_positions
        self.closed_positions = mongo.closed_positions
        self.strategies = mongo.strategies
        self.rejected = mongo.rejected
        self.canceled = mongo.canceled
        self.queue = mongo.queue
        self.logger = logger
        self.no_ids_list = []

        OrderBuilder.__init__(self)
        Tasks.__init__(self)

        # If user wants to run tasks, run in a separate thread.
        if RUN_TASKS:
            Thread(target=self.runTasks, daemon=True).start()
        else:
            self.logger.info(
                f"NOT RUNNING TASKS FOR {self.user['Name']} ({modifiedAccountID(self.account_id)})\n",
                extra={'log': False}
            )

        self.logger.info(
            f"RUNNING {user['Accounts'][str(account_id)]['Account_Position'].upper()} TRADER ({modifiedAccountID(self.account_id)})\n"
        )

    # -------------------------------------------
    # NEW: Refresh account summary (with token check)
    # -------------------------------------------
    @exception_handler
    def refreshAccountSummary(self):
        """
        Fetch the latest account summary from the Schwab API using the updated getAccount()
        and update self.user with the current liquidation value.
        """
        # Ensure tokens are valid before making the API call.
        if not self.schwab.checkTokenValidity():
            self.logger.error("Token validity check failed in refreshAccountSummary.")
            return

        account_summary = self.schwab.getAccount()

        try:
            # Adjust key names based on the actual API response structure.
            # For example, if the response contains securitiesAccount data:
            liquidation_value = float(account_summary["securitiesAccount"]["initialBalances"]["cashAvailableForTrading"])
            self.user["Liquidation_Value"] = liquidation_value
            self.logger.info(f"Updated liquidation value: {liquidation_value}")
        except Exception as e:
            self.logger.error("Could not retrieve liquidation value from account summary.")
            self.logger.error("Account summary response: " + json.dumps(account_summary, indent=2))



    # -------------------------------------------
    # Update account balance using the latest account summary
    # -------------------------------------------
    @exception_handler
    def updateAccountBalance(self):
        """
        Updates the user's account balance in MongoDB by recalculating the dynamic
        position size. First refreshes the account summary to ensure the latest
        liquidation value is stored in self.user.
        """
        self.refreshAccountSummary()  # Ensure the latest liquidation value is available
        liquidation_value = self.user.get("Liquidation_Value")
        if liquidation_value is None:
            self.logger.error("Liquidation value not available in user data.")
            return

        self.logger.info(f"Liquidation Value: {liquidation_value}")
        position_size_percent =100  # Example: use 10% of the liquidation value
        dynamic_position_size = int((liquidation_value * position_size_percent) / 100)

        update_result = self.strategies.update_many(
            {"account_id": self.account_id},
            {"$set": {"Position_Size": dynamic_position_size}}
        )

        self.logger.info(
            f"Updated {update_result.modified_count} strategies with a dynamic position size of ${dynamic_position_size}"
        )

    # -------------------------------------------
    # STEP ONE: Send Order (with token check)
    # -------------------------------------------
    @exception_handler
    def sendOrder(self, trade_data, strategy_object, direction):
        """
        Builds and sends an order. Before sending, we check token validity so that tokens
        are refreshed only if needed.
        """
        # Check token validity before placing the order.
        if not self.schwab.checkTokenValidity():
            self.logger.error("Token validity check failed in sendOrder. Aborting order placement.")
            return

        symbol = trade_data["Symbol"]
        strategy = trade_data["Strategy"]
        side = trade_data["Side"]
        order_type = strategy_object["Order_Type"]

        if order_type == "STANDARD":
            order, obj = self.standardOrder(trade_data, strategy_object, direction)
        elif order_type == "OCO":
            order, obj = self.OCOorder(trade_data, strategy_object, direction)
        else:
            self.logger.error(f"Unknown order type: {order_type}")
            return

        if order is None and obj is None:
            return

        # PLACE ORDER IF LIVE TRADER
        if self.RUN_LIVE_TRADER:
            # Get the linked account hash.
            accountNumber = self.client.account_linked()
            accountHash = accountNumber.json()[0]['hashValue']
            resp = self.client.order_place(accountHash, order)
            status_code = resp.status_code

            if status_code not in [200, 201]:
                # Try to extract an error message from the response.
                try:
                    error_message = resp.json().get("error", "Unknown error")
                except Exception:
                    error_message = "Unknown error"
                other = {
                    "Symbol": symbol,
                    "Order_Type": side,
                    "Order_Status": "REJECTED",
                    "Strategy": strategy,
                    "Trader": self.user["Name"],
                    "Date": getDatetime(),
                    "account_id": self.account_id
                }
                self.logger.info(
                    f"{symbol} Rejected For {self.user['Name']} ({modifiedAccountID(self.account_id)}) - Reason: {error_message}"
                )
                self.rejected.insert_one(other)
                return

            # GET ORDER ID FROM RESPONSE HEADERS (the new order's ID is expected in the header "Location")
            obj["Order_ID"] = int((resp.headers["Location"]).split("/")[-1].strip())
            obj["Account_Position"] = "Live"
        else:
            # Simulate an order for paper trading.
            obj["Order_ID"] = -1 * randint(100_000_000, 999_999_999)
            obj["Account_Position"] = "Paper"

        obj["Order_Status"] = "QUEUED"
        self.queueOrder(obj)
        response_msg = f"{'Live Trade' if self.RUN_LIVE_TRADER else 'Paper Trade'}: {side} Order for Symbol {symbol} ({modifiedAccountID(self.account_id)})"
        self.logger.info(response_msg)

    # -------------------------------------------
    # STEP TWO: Queue Order
    # -------------------------------------------
    @exception_handler
    def queueOrder(self, order):
        """Queue the order in the MongoDB queue collection."""
        self.queue.update_one(
            {"Trader": self.user["Name"], "Symbol": order["Symbol"], "Strategy": order["Strategy"]},
            {"$set": order},
            upsert=True
        )

    # -------------------------------------------
    # STEP THREE: Update Order Status (with token check)
    # -------------------------------------------
    @exception_handler
    def updateStatus(self):
        """
        Queries the queued orders and uses the order ID to query Schwab’s orders for the account.
        Based on the response, either processes filled orders or moves rejected/canceled orders
        to the corresponding collections.
        """
        # Check token validity before updating status.
        if not self.schwab.checkTokenValidity():
            self.logger.error("Token validity check failed in updateStatus. Aborting status update.")
            return

        queued_orders = self.queue.find({
            "Trader": self.user["Name"],
            "Order_ID": {"$ne": None},
            "account_id": self.account_id
        })

        for queue_order in queued_orders:
            # Use Schwabdev's get_order() method (assumed to be implemented in your Schwab class)
            spec_order = self.schwab.get_order(queue_order["Order_ID"])

            # ORDER ID NOT FOUND. ASSUME REMOVED OR PAPER TRADING
            if "error" in spec_order:
                custom = {
                    "price": queue_order["Entry_Price"] if queue_order["Direction"] == "OPEN POSITION" else queue_order["Exit_Price"],
                    "shares": queue_order["Qty"]
                }
                if self.RUN_LIVE_TRADER:
                    data_integrity = "Assumed"
                    self.logger.warning(
                        f"Order ID Not Found. Moving {queue_order['Symbol']} {queue_order['Order_Type']} Order To {queue_order['Direction']} Positions ({modifiedAccountID(self.account_id)})"
                    )
                else:
                    data_integrity = "Reliable"
                    self.logger.info(
                        f"Paper Trader - Sending Queue Order To PushOrder ({modifiedAccountID(self.account_id)})"
                    )
                self.pushOrder(queue_order, custom, data_integrity)
                continue

            new_status = spec_order.get("status")
            order_type = queue_order["Order_Type"]

            if queue_order["Order_ID"] == spec_order.get("orderId"):
                if new_status == "FILLED":
                    if queue_order["Order_Type"] == "OCO":
                        queue_order = {**queue_order, **self.extractOCOchildren(spec_order)}
                    self.pushOrder(queue_order, spec_order)
                elif new_status in ["CANCELED", "REJECTED"]:
                    self.queue.delete_one({
                        "Trader": self.user["Name"],
                        "Symbol": queue_order["Symbol"],
                        "Strategy": queue_order["Strategy"],
                        "account_id": self.account_id
                    })
                    other = {
                        "Symbol": queue_order["Symbol"],
                        "Order_Type": order_type,
                        "Order_Status": new_status,
                        "Strategy": queue_order["Strategy"],
                        "Trader": self.user["Name"],
                        "Date": getDatetime(),
                        "account_id": self.account_id
                    }
                    if new_status == "REJECTED":
                        self.rejected.insert_one(other)
                    else:
                        self.canceled.insert_one(other)
                    self.logger.info(
                        f"{new_status.upper()} Order For {queue_order['Symbol']} ({modifiedAccountID(self.account_id)})"
                    )
                else:
                    self.queue.update_one(
                        {"Trader": self.user["Name"], "Symbol": queue_order["Symbol"], "Strategy": queue_order["Strategy"]},
                        {"$set": {"Order_Status": new_status}}
                    )

    # -------------------------------------------
    # STEP FOUR: Push Order to Positions
    # -------------------------------------------
    @exception_handler
    def pushOrder(self, queue_order, spec_order, data_integrity="Reliable"):
        """
        Pushes the order to either the open positions or closed positions collection in MongoDB.
        """
        symbol = queue_order["Symbol"]

        if "orderActivityCollection" in spec_order:
            price = spec_order["orderActivityCollection"][0]["executionLegs"][0]["price"]
            shares = int(spec_order["quantity"])
        else:
            price = spec_order["price"]
            shares = int(queue_order["Qty"])

        price = round(price, 2) if price >= 1 else round(price, 4)
        strategy = queue_order["Strategy"]
        side = queue_order["Side"]
        account_id = queue_order["account_id"]
        position_size = queue_order["Position_Size"]
        asset_type = queue_order["Asset_Type"]
        position_type = queue_order["Position_Type"]
        direction = queue_order["Direction"]
        account_position = queue_order["Account_Position"]
        order_type = queue_order["Order_Type"]

        obj = {
            "Symbol": symbol,
            "Strategy": strategy,
            "Position_Size": position_size,
            "Position_Type": position_type,
            "Data_Integrity": data_integrity,
            "Trader": self.user["Name"],
            "account_id": account_id,
            "Asset_Type": asset_type,
            "Account_Position": account_position,
            "Order_Type": order_type
        }

        if asset_type == "OPTION":
            obj["Pre_Symbol"] = queue_order["Pre_Symbol"]
            obj["Exp_Date"] = queue_order["Exp_Date"]
            obj["Option_Type"] = queue_order["Option_Type"]

        collection_insert = None
        message_to_push = None

        if direction == "OPEN POSITION":
            obj["Qty"] = shares
            obj["Entry_Price"] = price
            obj["Entry_Date"] = getDatetime()
            collection_insert = self.open_positions.insert_one
            message_to_push = (
                f">>>> \n Side: {side} \n Symbol: {symbol} \n Qty: {shares} \n Price: ${price} \n "
                f"Strategy: {strategy} \n Asset Type: {asset_type} \n Date: {getDatetime()} \n "
                f"Trader: {self.user['Name']} \n Account Position: {'Live Trade' if self.RUN_LIVE_TRADER else 'Paper Trade'}"
            )
        elif direction == "CLOSE POSITION":
            position = self.open_positions.find_one({
                "Trader": self.user["Name"],
                "Symbol": symbol,
                "Strategy": strategy
            })
            obj["Qty"] = position["Qty"]
            obj["Entry_Price"] = position["Entry_Price"]
            obj["Entry_Date"] = position["Entry_Date"]
            obj["Exit_Price"] = price
            obj["Exit_Date"] = getDatetime()
            collection_insert = self.closed_positions.insert_one
            message_to_push = (
                f"____ \n Side: {side} \n Symbol: {symbol} \n Qty: {position['Qty']} \n "
                f"Entry Price: ${position['Entry_Price']} \n Entry Date: {position['Entry_Date']} \n "
                f"Exit Price: ${price} \n Exit Date: {getDatetime()} \n Strategy: {strategy} \n "
                f"Asset Type: {asset_type} \n Trader: {self.user['Name']} \n "
                f"Account Position: {'Live Trade' if self.RUN_LIVE_TRADER else 'Paper Trade'}"
            )
            # REMOVE FROM OPEN POSITIONS
            is_removed = self.open_positions.delete_one({
                "Trader": self.user["Name"],
                "Symbol": symbol,
                "Strategy": strategy
            })
            try:
                if int(is_removed.deleted_count) == 0:
                    self.logger.error(
                        f"INITIAL FAIL OF DELETING OPEN POSITION FOR SYMBOL {symbol} - {self.user['Name']} ({modifiedAccountID(self.account_id)})"
                    )
                    self.open_positions.delete_one({
                        "Trader": self.user["Name"],
                        "Symbol": symbol,
                        "Strategy": strategy
                    })
            except Exception:
                msg = f"{self.user['Name']} - {modifiedAccountID(self.account_id)} - {traceback.format_exc()}"
                self.logger.error(msg)

        try:
            collection_insert(obj)
        except WriteConcernError as e:
            self.logger.error(
                f"INITIAL FAIL OF INSERTING POSITION FOR SYMBOL {symbol} - DATE/TIME: {getDatetime()} - DATA: {obj} - {e}"
            )
            collection_insert(obj)
        except WriteError as e:
            self.logger.error(
                f"INITIAL FAIL OF INSERTING POSITION FOR SYMBOL {symbol} - DATE/TIME: {getDatetime()} - DATA: {obj} - {e}"
            )
            collection_insert(obj)
        except Exception:
            msg = f"{self.user['Name']} - {modifiedAccountID(self.account_id)} - {traceback.format_exc()}"
            self.logger.error(msg)

        self.logger.info(
            f"Pushing {side} Order For {symbol} To {'Open Positions' if direction == 'OPEN POSITION' else 'Closed Positions'} ({modifiedAccountID(self.account_id)})"
        )
        # REMOVE FROM QUEUE
        self.queue.delete_one({
            "Trader": self.user["Name"],
            "Symbol": symbol,
            "Strategy": strategy,
            "account_id": self.account_id
        })
        self.push.send(message_to_push)

    # -------------------------------------------
    # RUN TRADER: Main loop to process trade data (with token check)
    # -------------------------------------------
    @exception_handler
    def runTrader(self, trade_data):
        """
        Iterates over the trade data and makes decisions on whether to buy or sell.
        Args:
            trade_data (list): Contains trade data for each stock.
        """
        # Check token validity at the start.
        if not self.schwab.checkTokenValidity():
            self.logger.error("Token validity check failed in runTrader. Aborting trader run.")
            return

        self.logger.info("RUN TRADER\n", extra={'log': False})
        self.updateStatus()

        # Update user info from DB
        self.user = self.mongo.users.find_one({"Name": self.user["Name"]})

        # FORBIDDEN SYMBOLS
        forbidden_symbols = self.mongo.forbidden.find({"account_id": str(self.account_id)})

        for row in trade_data:
            strategy = row["Strategy"]
            symbol = row["Symbol"]
            asset_type = row["Asset_Type"]
            side = row["Side"]

            # CHECK OPEN POSITIONS AND QUEUE
            open_position = self.open_positions.find_one({
                "Trader": self.user["Name"],
                "Symbol": symbol,
                "Strategy": strategy,
                "account_id": self.account_id
            })
            queued = self.queue.find_one({
                "Trader": self.user["Name"],
                "Symbol": symbol,
                "Strategy": strategy,
                "account_id": self.account_id
            })
            strategy_object = self.strategies.find_one({
                "Strategy": strategy,
                "account_id": self.account_id
            })

            if not strategy_object:
                self.addNewStrategy(strategy, asset_type)
                strategy_object = self.strategies.find_one({
                    "account_id": self.account_id,
                    "Strategy": strategy
                })
                print(strategy_object)

            position_type = strategy_object["Position_Type"]
            row["Position_Type"] = position_type

            if not queued:
                direction = None
                # Check if there is already an open position for this symbol/strategy combo.
                if open_position:
                    direction = "CLOSE POSITION"
                    # Additional logic for SHORT/LONG or options can be added here.
                    if side == "BUY" and position_type == "SHORT":
                        pass
                    elif side == "SELL" and position_type == "LONG":
                        pass
                    elif side == "SELL_TO_CLOSE" and position_type == "LONG":
                        pass
                    elif side == "BUY_TO_CLOSE" and position_type == "SHORT":
                        pass
                    else:
                        continue
                elif not open_position and symbol not in forbidden_symbols:
                    direction = "OPEN POSITION"
                    if side == "BUY" and position_type == "LONG":
                        pass
                    elif side == "SELL" and position_type == "SHORT":
                        pass
                    elif side == "SELL_TO_OPEN" and position_type == "SHORT":
                        pass
                    elif side == "BUY_TO_OPEN" and position_type == "LONG":
                        pass
                    else:
                        continue

                if direction is not None:
                    self.sendOrder(
                        row if not open_position else {**row, **open_position},
                        strategy_object,
                        direction
                    )
