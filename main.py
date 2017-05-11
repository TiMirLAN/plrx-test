#!/usr/bin/env python3.5
# coding=utf-8

"""
Решение тестового задания.
"""

from configparser import ConfigParser
from collectors import InstallsCollector, RevenueCollector, set_up_collectors_date_format
from rpi_writer import RpisWriter

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
    rpi_colletor = RevenueCollector(default_query, rpi_range)
    rpi_colletor.collect_from(config['input']['purchases_data_file'])

    # Подсчёт и запись результатов.
    rpi_writer = RpisWriter(installs_collector.results, rpi_colletor.results, rpi_range)
    rpi_writer.write_to(config['output']['results_data_file'])

if __name__ == "__main__":
    collect_rpi_into_csv()
