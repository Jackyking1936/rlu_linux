import fubon_neo
from fubon_neo.sdk import FubonSDK, Mode, Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction

import json
import logging
import pandas as pd
from datetime import datetime
from threading import Timer
import signal
import sys

class RepeatTimer(Timer):
    def run(self):
        self.function(*self.args, **self.kwargs)
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)

class rlu_trader():
    def __init__(self, login_path, config_path):
        self.login_path = login_path
        self.config_path = config_path
        self.sdk = FubonSDK()
        self.sdk.set_on_event(self.trade_on_event)
        self.active_acc = None
        self.sub_percent = None
        self.snap_interval = None
        self.single_budget = None
        self.total_budget = None
        self.vol_threshold = None

        log_formatter = logging.Formatter("%(asctime)s.%(msecs)03d [%(threadName)s] [%(levelname)s]: %(message)s", datefmt = '%Y-%m-%d %H:%M:%S')
        self.logger = logging.getLogger("RLU")
        self.logger.setLevel(logging.DEBUG)

        log_path = './log'
        file_name = 'rlu_log'
        self.today_date = datetime.today()
        self.today_str = datetime.strftime(self.today_date, "%Y%m%d")
        file_handler = logging.FileHandler("{0}/{1}.log.{2}".format(log_path, file_name, self.today_str), 'a', 'utf-8')
        file_handler.setFormatter(log_formatter)
        self.logger.addHandler(file_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(log_formatter)
        self.logger.addHandler(console_handler)

        self.logger.info("current SDK version: {}".format(fubon_neo.__version__))
        self.sdk_login()
        self.load_confing()

        self.sdk.init_realtime(Mode.Speed)
        self.reststock = self.sdk.marketdata.rest_client.stock
        self.wsstock = self.sdk.marketdata.websocket_client.stock

        self.wsstock.on('message', self.handle_message)
        self.wsstock.on('connect', self.handle_connect)
        self.wsstock.on('disconnect', self.handle_disconnect)
        self.wsstock.on('error', self.handle_error)
        self.wsstock.connect()

        open_time = self.today_date.replace(hour=9, minute=0, second=0, microsecond=0)
        self.open_unix = int(datetime.timestamp(open_time)*1000000)
        self.close_time = self.today_date.replace(hour=13, minute=31, second=0, microsecond=0)

        self.subscribed_ids = {}
        self.is_ordered = {}
        self.used_budget = 0
        self.order_tag = 'rlu'
        self.fake_price_cnt = 0
        self.keep_trade = True
        self.last_day_inv = {}
        self.inv_avg_price = {}
        self.inv_ref_price = {}
        self.keep_inv = {}

        self.read_inv_and_subscribe()

        self.snapshot_timer = RepeatTimer(self.snap_interval, self.snapshot_n_subscribe)
        self.snapshot_timer.name = 'snapshot_thread'
        self.snapshot_timer.start()
    
    def read_inv_and_subscribe(self):
        with open("last_day_inv.json", "r") as file:
            self.last_day_inv = json.load(file)
        
        with open("last_day_inv_avg_price.json", "r") as file:
            self.inv_avg_price = json.load(file)

        self.last_day_inv.pop("date")
        self.logger.info(f"load last day inv done: {self.last_day_inv}")
        self.logger.info(f"load inv avg price done: {self.inv_avg_price}")

        for symbol, share in self.last_day_inv.items():
            ticker_res = self.reststock.intraday.ticker(symbol=symbol)
            try:
                self.inv_ref_price[symbol] = ticker_res['referencePrice']
            except Exception as e:
                self.logger.error(f"{symbol} fetch ticker fail: {e}")

            self.wsstock.subscribe({
                'channel': 'trades',
                'symbol': symbol
            })
        self.logger.info(f"All rlu inv ref price: {self.inv_ref_price}")

    def fake_ws_data(self):
        if self.fake_price_cnt % 2==0:
            self.price_interval = 0
            self.fake_ws_timer = RepeatTimer(1, self.fake_message)
            self.fake_ws_timer.start()
        else:
            self.fake_ws_timer.cancel()

        self.fake_price_cnt+=1

    def fake_message(self):
        # stock_no = list(self.subscribed_ids.keys())[0]
        stock_no = 5907
        self.price_interval+=1
        json_template = '''{{"event":"data","data":{{"symbol":"{symbol}","type":"EQUITY","exchange":"TWSE","market":"TSE","price":{price},"size":713,"bid":16.67,"ask":{price}, "isOpen":true, "volume":8066, "time":1718343000000000,"serial":9475857}},"id":"w4mkzAqYAYFKyEBLyEjmHEoNADpwKjUJmqg02G3OC9YmV","channel":"trades"}}'''
        json_price = 10+self.price_interval
        if json_price >= 20:
            json_template = '''{{"event":"data","data":{{"symbol":"{symbol}","type":"EQUITY","exchange":"TWSE","market":"TSE","price":{price},"size":713,"bid":16.67,"ask":{price}, "isLimitUpPrice":true, "volume":500,"isClose":true,"time":1718343000000000,"serial":9475857}},"id":"w4mkzAqYAYFKyEBLyEjmHEoNADpwKjUJmqg02G3OC9YmV","channel":"trades"}}'''
        json_str = json_template.format(symbol=stock_no, price=str(json_price))
        self.handle_message(json_str)

    def sell_market_order(self, symbol, sell_qty, tag='rlu_out'):
        order = Order(
            buy_sell = BSAction.Sell,
            symbol = symbol,
            price =  None,
            quantity =  int(sell_qty),
            market_type = MarketType.Common,
            price_type = PriceType.Market,
            time_in_force = TimeInForce.ROD,
            order_type = OrderType.Stock,
            user_def = tag # optional field
        )

        order_res = self.sdk.stock.place_order(self.active_acc, order)
        return order_res

    def buy_market_order(self, symbol, buy_qty, tag='rlu'):
        order = Order(
            buy_sell = BSAction.Buy,
            symbol = symbol,
            price =  None,
            quantity =  int(buy_qty),
            market_type = MarketType.Common,
            price_type = PriceType.Market,
            time_in_force = TimeInForce.ROD,
            order_type = OrderType.Stock,
            user_def = tag # optional field
        )

        order_res = self.sdk.stock.place_order(self.active_acc, order)
        return order_res

    def handle_message(self, message):
        msg = json.loads(message)
        event = msg["event"]
        data = msg["data"]
        # print(event, data)

         # subscribed事件處理
        if event == "subscribed":
            if type(data) == list:
                for subscribed_item in data:
                    sub_id = subscribed_item["id"]
                    symbol = subscribed_item["symbol"]
                    self.logger.info('訂閱成功...'+symbol)
                    self.subscribed_ids[symbol] = sub_id
            else:
                sub_id = data["id"]
                symbol = data["symbol"]
                self.logger.info('訂閱成功...'+symbol)
                self.subscribed_ids[symbol] = sub_id
        
        elif event == "unsubscribed":
            for key, value in self.subscribed_ids.items():
                if value == data["id"]:
                    print(value)
                    remove_key = key
            self.subscribed_ids.pop(remove_key)
            self.logger.info(remove_key+"...成功移除訂閱")

        elif event == "data":
            symbol = data['symbol']

            if 'isTrial' in data:
                if data['isTrial']:
                    return
            
            print(event, data)

            if ('isLimitUpPrice' in data) and (symbol not in self.is_ordered):
                if (self.single_budget <= (self.total_budget-self.used_budget)):
                    if data['isLimitUpPrice']:
                        if data['volume'] >= self.vol_threshold:
                            if 'price' in data:
                                buy_qty = self.single_budget//(data['price']*1000)*1000
                                
                            if buy_qty <= 0:
                                self.logger.info(symbol+'...額度不足購買1張')
                            else:
                                self.logger.info(symbol+'...委託'+str(buy_qty)+'股')
                                order_res = self.buy_market_order(symbol, buy_qty, self.order_tag)
                                if order_res.is_success:
                                    self.logger.info(symbol+"...市價單發送成功，單號: "+order_res.data.order_no)
                                    self.is_ordered[symbol] = buy_qty
                                    self.used_budget+=buy_qty*data['price']
                                    if symbol in self.keep_inv:
                                        self.keep_inv[symbol]+=buy_qty
                                        self.logger.info(f"{symbol} 成功加碼，現在持倉量 {self.keep_inv[symbol]}")
                                else:
                                    self.logger.error(symbol+"...市價單發送失敗...")
                                    self.logger.error(str(order_res.message))
                        else:
                            self.logger.info(symbol+"...交易量不足，不下單...")
                else:
                    self.logger.info(symbol+" 總額度超限 "+"已使用額度/總額度: "+str(self.used_budget)+'/'+str(self.total_budget))

            if 'isOpen' in data:
                if symbol in self.last_day_inv:
                    up_percent = (data['price']-self.inv_ref_price[symbol])/self.inv_ref_price[symbol]*100
                    if  up_percent > self.keep_percent:
                        self.keep_inv[symbol] = self.last_day_inv[symbol]
                        self.used_budget+=self.keep_inv[symbol]*self.inv_avg_price[symbol]
                        self.logger.info(f"{symbol}...漲幅:{up_percent} add to keep_inv list, use budget {self.keep_inv[symbol]*self.inv_avg_price[symbol]}")
                    else:
                        sell_res = self.sell_market_order(symbol, self.last_day_inv[symbol], "rlu_out")
                        if sell_res.is_success:
                            self.logger.info(f"{symbol}...漲幅:{up_percent}, 開盤賣出發送成功，單號: {sell_res.data.order_no}")
                        else:
                            self.logger.error(symbol+"...市價單賣出發送失敗...")
                            self.logger.error(str(sell_res.message))
            
            if symbol in self.keep_inv:
                chg_percent = (data['price']-self.inv_ref_price[symbol])/self.inv_ref_price[symbol]*100
                if  chg_percent < self.keep_sl_percent:
                    sell_res = self.sell_market_order(symbol, self.keep_inv[symbol], "rlu_out")
                    if sell_res.is_success:
                        self.logger.info(f"{symbol}...漲幅:{chg_percent}, 留倉失敗, 停損發送成功, 單號: {sell_res.data.order_no}")
                        self.keep_inv.pop(symbol)
                    else:
                        self.logger.error(f"{symbol}...留倉失敗, 停損發送失敗 Something wrong")
                        self.logger.error(str(sell_res.message))
                    

    def handle_connect(self):
        self.logger.info('market data connected')
    
    def handle_disconnect(self, code, message):
        if not code and not message:
            self.logger.info(f'WebSocket已停止')
        else:
            self.logger.info(f'market data disconnect: {code}, {message}')
    
    def handle_error(self, error):
        self.logger.error(f'market data error: {error}')

    def snapshot_n_subscribe(self):
        try:
            self.logger.info("snapshoting...")
            TSE_movers = self.reststock.snapshot.movers(market='TSE', type='COMMONSTOCK', direction='up', change='percent', gte=self.sub_percent)
            TSE_movers_df = pd.DataFrame(TSE_movers['data'])
            OTC_movers = self.reststock.snapshot.movers(market='OTC', type='COMMONSTOCK', direction='up', change='percent', gte=self.sub_percent)
            OTC_movers_df = pd.DataFrame(OTC_movers['data'])

            all_movers_df = pd.concat([TSE_movers_df, OTC_movers_df])
            all_movers_df = all_movers_df[all_movers_df['lastUpdated']>self.open_unix]

            new_subscribe = list(all_movers_df['symbol'])
            new_subscribe = list(set(new_subscribe).difference(set(self.subscribed_ids.keys())))
            self.logger.info("NEW UP SYMBOL: "+str(new_subscribe))

            if new_subscribe:
                self.wsstock.subscribe({
                    'channel': 'trades',
                    'symbols': new_subscribe
                })

            if datetime.now() > self.close_time:
                self.logger.info("After market close, close automatically.")
                self.close_trader()
        except Exception as e:
            self.logger.error("snapshot unknown error down, exception:{}".format(e))

    def load_confing(self):
        with open(self.config_path) as config_file:
            config_json = json.load(config_file)

        self.sub_percent = config_json['sub_percent']
        self.snap_interval = config_json['snap_interval']
        self.single_budget = config_json['single_budget']*10000
        self.total_budget = config_json['total_budget']*10000
        self.vol_threshold = config_json['vol_threshold']
        self.keep_percent = config_json['keep_percent']
        self.keep_sl_percent = config_json['keep_sl_percent']

        self.logger.info("Current subscribe percent: {}".format(self.sub_percent))
        self.logger.info("Current snapshot interval in second: {}".format(self.snap_interval))
        self.logger.info("Current single budget: {}".format(self.single_budget))
        self.logger.info("Current total budget: {}".format(self.total_budget))
        self.logger.info("Current volume threshold: {}".format(self.vol_threshold))
        self.logger.info("Current keep percent: {}".format(self.keep_percent))
        self.logger.info("Current keep sl percent: {}".format(self.keep_sl_percent))

    def sdk_login(self):
        with open(self.login_path) as user_file:
            acc_json = json.load(user_file)

        accounts = self.sdk.login(acc_json['id'], acc_json['pwd'], acc_json['cert_path'], acc_json['cert_pwd'])
        self.logger.info(str(accounts))

        for acc in accounts.data:
            if acc.account == acc_json['target_acc']:
                self.active_acc = acc
        self.logger.info("Current use: {}".format(self.active_acc))
    
    # 視窗關閉時要做的事，主要是關websocket連結
    def close_trader(self):
        # do stuff
        self.logger.info("try exit, disconnect websocket...")
        self.wsstock.disconnect()
        if self.snapshot_timer.is_alive():
            self.snapshot_timer.cancel()
        else:
            self.logger.error("snapshot timer is not alive")

        self.logger.info("close snapshot timer")
        
        try:
            if self.fake_ws_timer.is_alive():
                self.fake_ws_timer.cancel()
        except AttributeError:
            self.logger.error("no fake ws timer exist")
        
        self.sdk.logout()
        self.logger.info("logout sdk finish")
        self.keep_trade = False
    
    def trade_on_event(self, code, content):
        self.logger.info("Trade Callback "+str(code)+" "+str(content))
        if code == '300':
            self.logger.info('unknown error out, try login again')

def signal_handler(sig, frame):
    print('Pressed Ctrl+C to exit')
    my_trader.close_trader()
    sys.exit(0)

if __name__ == '__main__':
    my_trader = rlu_trader('my_acc_config.json', 'trade_config.json')
    signal.signal(signal.SIGINT, signal_handler)

    my_trader.keep_trade = True
    while my_trader.keep_trade:
        user_input = input("\"fake_ws\" for simulate websocket data to buy\n\"exit\" for close program\n")
        if user_input == 'fake_ws':
            my_trader.fake_ws_data()
        elif user_input == 'exit':
            my_trader.close_trader()

