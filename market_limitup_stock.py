from fubon_neo.sdk import FubonSDK, Mode, Order
from fubon_neo.constant import TimeInForce, OrderType, PriceType, MarketType, BSAction

import json
import pandas as pd
import sys
import os

sub_percent = -0.01
price_threshold = 50
vol_threshold = 500

def sdk_login(login_path, sdk):
    with open(login_path) as user_file:
        acc_json = json.load(user_file)

    accounts = sdk.login(acc_json['id'], acc_json['pwd'], acc_json['cert_path'], acc_json['cert_pwd'])

    active_acc = None
    for acc in accounts.data:
        if acc.account == acc_json['target_acc']:
            active_acc = acc
    return active_acc    

sdk = FubonSDK()
active_acc = sdk_login('my_acc_config.json', sdk)

if active_acc:
    print("login success")
else:
    print("something wrong")

sdk.init_realtime()
reststock = sdk.marketdata.rest_client.stock

def snap_screener(sub_percent, price_threshold, vol_threshold):
    TSE_movers = reststock.snapshot.movers(market='TSE', type='COMMONSTOCK', direction='up', change='percent', gte=sub_percent)
    TSE_movers_df = pd.DataFrame(TSE_movers['data'])
    OTC_movers = reststock.snapshot.movers(market='OTC', type='COMMONSTOCK', direction='up', change='percent', gte=sub_percent)
    OTC_movers_df = pd.DataFrame(OTC_movers['data'])

    all_movers_df = pd.concat([TSE_movers_df, OTC_movers_df])
    all_movers_df = all_movers_df[all_movers_df['lastPrice']<price_threshold]
    all_movers_df = all_movers_df[all_movers_df['tradeVolume']>vol_threshold]
    all_movers_df.reset_index(drop=True, inplace=True)
    print(all_movers_df[['symbol', 'name', 'lastPrice', 'changePercent', 'tradeVolume']])

snap_screener(sub_percent, price_threshold, vol_threshold)
os._exit(0)