#!/usr/bin/env python3
# coding=utf-8

"""
Решение тестового задания.
"""

import configparser
from collections import defaultdict
from csv import reader
from datetime import datetime as dt

__author__ = "Tim Mironov"
__email__ = "timirlan666@gmail.com"

def get_config() -> configparser.ConfigParser:
    """
    Получение конфигов для входных и выходных данных. Лень парсить аргументы.
    """
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def collect_install_counts(installs_data_file_name: str) -> dict:
    """
    Возвращает словарь: страна -> количество установок.
    """
    results = defaultdict(int)
    with open(installs_data_file_name) as installs_file:
        for *_, country_code in reader(installs_file):
            results[country_code] += 1
    return dict(results)

DF = '%Y-%m-%d %H:%M:%S'

def collect_rpis(
        purchases_file_name: str,
        date_range: tuple,
        rpi_delta: tuple,
        required_app_id: int
    ) -> list:
    """
    Считает rpi delta_range[*] дня для платежей из инервала date_range.
    """
    date_from, date_to = date_range
    rpi_delta_range = range(*rpi_delta)
    results = defaultdict(float)
    with open(purchases_file_name) as purchases_file:
        for payment_str, app_id, country, install_srt, revenue in reader(purchases_file):
            payment_timest = dt.strptime(payment_str, DF)
            install_timest = dt.strptime(install_srt, DF)
            print(int(app_id), required_app_id, app_id == required_app_id)
            if int(app_id) == required_app_id and date_from <= install_timest < date_to:
                days = (payment_timest - install_timest).days
                print(days)
                if days in rpi_delta_range:
                    for handled_day in range(rpi_delta_range.start, days):
                        results[(country, handled_day)] += float(revenue)
    return dict(results)

def collect_rpi_into_csv() -> None:
    """
    Основная фунция. Собирает rpi по дням и создаёт файл с результатом.
    """
    config = get_config()
    install_counts = collect_install_counts(config['input']['installs_data_file'])
    print(collect_rpis(
        config['input']['purchases_data_file'],
        (
            dt.strptime(config['cohort']['from'], DF),
            dt.strptime(config['cohort']['to'], DF)
        ),
        (1,10),
        2
    ))

if __name__ == "__main__":
    collect_rpi_into_csv()
