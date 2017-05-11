#!/usr/bin/env python3.5
# coding=utf-8

"""
Решение тестового задания.
"""

from configparser import ConfigParser
from collectors import InstallsCollector, RpisCollector, set_up_collectors_date_format

__author__ = "Tim Mironov"
__email__ = "timirlan666@gmail.com"


def get_config() -> ConfigParser:
    """
    Получение конфигов для входных и выходных данных. Лень парсить аргументы.
    """
    config = ConfigParser()
    config.read('config.ini')
    return config


def collect_rpi_into_csv() -> None:
    """
    Основная фунция. Собирает rpi по дням и создаёт файл с результатом.
    """
    config = get_config()
    default_query = dict(config['query'])
    set_up_collectors_date_format(config['dates']['default_format'])

    # Подсчёт инсталов
    installs_collector = InstallsCollector(default_query)
    installs_collector.collect_from(config['input']['installs_data_file'])

    # Подсчёт rpi
    rpi_range = range(*[int(i) for i in config['rpi']['range'].split(',')])
    rpi_colletor = RpisCollector(default_query, rpi_range)
    rpi_colletor.collect_from(config['input']['purchases_data_file'])
    print(installs_collector.results_to_dict())
    print(rpi_colletor.results_to_dict())

if __name__ == "__main__":
    collect_rpi_into_csv()
