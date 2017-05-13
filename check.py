#!/usr/bin/env python3.5
# coding=utf-8

"""
Проверка расчётов в тестовом задании.
"""

import pandas as pd
from configparser import ConfigParser
from datetime import datetime

__author__ = "Tim Mironov"
__email__ = "timirlan666@gmail.com"

def check():
    config = ConfigParser()
    config.read('config.ini')

    installs = pd.read_csv(config['input']['installs_data_file'])
    purchases = pd.read_csv(config['input']['purchases_data_file'])


    from_dt, to_dt = config['query']['install_date'].split(',')
    target_mobile_app = int(config['query']['app_id'])
    rpi_from, rpi_to = [int(i) for i in config['rpi']['range'].split(',')]

    target_installs = installs[
            (installs.created >= from_dt) & (installs.created <= to_dt)
    ][
            installs.mobile_app == target_mobile_app
    ]

    target_purchases = purchases[
            (purchases.install_date >= from_dt)\
                    & (purchases.install_date <= to_dt)
    ][
            purchases.mobile_app == target_mobile_app
    ]
    target_purchases['install_date'] = pd.to_datetime(target_purchases.install_date)
    target_purchases['created'] = pd.to_datetime(target_purchases.created)

    target_purchases['time_from_install'] =\
            target_purchases.created - target_purchases.install_date

    target_purchases['days_from_install'] =\
            target_purchases.time_from_install.transform(lambda td: td.days)

    target_purchases = target_purchases[target_purchases.days_from_install <= rpi_to]
    
    total_purchases = target_purchases[[
            'country',
            'days_from_install',
            'revenue',
    ]].groupby(['country', 'days_from_install']).sum()

    total_purchases['total_revenue'] = 0
    for country in total_purchases.index.get_level_values(0):
        total_purchases['total_revenue'][country] = total_purchases.loc[country]['revenue'].cumsum()

    total_installs = target_installs[['country']].groupby(target_installs.country).count().rename(columns={'country':'installs'})

    results = total_installs.join(total_purchases, how='inner')
    results['rpi'] = results['total_revenue'] / results['installs']
    print(results)

if __name__ == '__main__':
    check()
