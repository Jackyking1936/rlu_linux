from fubon_neo.sdk import FubonSDK, Mode, Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction

import json
import logging
from datetime import datetime, timedelta

class rlu_inveotries_getter():
    def __init__(self, login_path):
        self.login_path = login_path

        log_formatter = logging.Formatter("%(asctime)s.%(msecs)03d [%(threadName)s] [%(levelname)s]: %(message)s", datefmt = '%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger("RLU")
        self.logger.setLevel(logging.DEBUG)

        log_path = './log'
        file_name = 'rlu_inv_log'
        self.today_date = datetime.today()
        self.today_str = datetime.strftime(self.today_date, "%Y%m%d")
        file_handler = logging.FileHandler("{0}/{1}.log.{2}".format(log_path, file_name, self.today_str), 'a', 'utf-8')
        file_handler.setFormatter(log_formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        self.logger.addHandler(console_handler)

        self.sdk = FubonSDK()
        self.sdk_login()
        self.rlu_last_day_inv, self.rlu_inv_avg_price = self.get_rlu_inventories()
        
        self.logger.info(f"last day inventory: {self.rlu_last_day_inv}")
        self.logger.info(f"last day inventory avg price: {self.rlu_inv_avg_price}")
        with open("last_day_inv.json", "w") as outfile:
            json.dump(self.rlu_last_day_inv, outfile)
        
        with open("last_day_inv_avg_price.json", "w") as outfile:
            json.dump(self.rlu_inv_avg_price, outfile)

    def sdk_login(self):
        with open(self.login_path) as user_file:
            acc_json = json.load(user_file)

        accounts = self.sdk.login(acc_json['id'], acc_json['pwd'], acc_json['cert_path'], acc_json['cert_pwd'])
        self.logger.info(str(accounts))

        for acc in accounts.data:
            if acc.account == acc_json['target_acc']:
                self.active_acc = acc
        self.logger.info("Current use: {}".format(self.active_acc))
    
    def get_rlu_inventories(self):
        hist_trades_data = []
        day_gap = 1
        fetch_date_str = ''
        while not hist_trades_data:
            if day_gap>5:
                self.logger.error(f"day_gap is {day_gap} now, inventories must wrong")
            fetch_date = self.today_date-timedelta(days=day_gap)
            fetch_date_str = datetime.strftime(fetch_date, "%Y%m%d")
            hist_trades = self.sdk.stock.filled_history(self.active_acc, fetch_date_str)
            if hist_trades.is_success:
                hist_trades_data = hist_trades.data                    
            else:
                self.logger.error(f"fetech inv fail, error message {hist_trades.message}")
            day_gap+=1
        self.logger.info(f"{hist_trades_data}")

        rlu_inv = {'date': fetch_date_str}
        rlu_inv_avg_price = {}
        for data in hist_trades_data:
            if data.user_def == 'rlu':
                if data.stock_no in rlu_inv:
                    rlu_inv[data.stock_no] += data.filled_qty
                    rlu_inv_avg_price[data.stock_no] = data.filled_avg_price
                else:
                    rlu_inv[data.stock_no] = data.filled_qty
                    rlu_inv_avg_price[data.stock_no] = data.filled_avg_price
        return rlu_inv, rlu_inv_avg_price

if __name__ == "__main__":
    rlu_inv_getter = rlu_inveotries_getter("my_acc_config.json")