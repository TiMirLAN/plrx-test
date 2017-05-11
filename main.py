#!/usr/bin/env python3.5
# coding=utf-8

"""
Решение тестового задания.
"""

from configparser import ConfigParser
from csv import DictWriter
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


class RpisWriter:
    def __init__(self, installs: dict, revenues: dict, rpi_range: range) -> None:
        self.installs = installs
        self.revenues = revenues
        self._fieldnames = (
            'country',
            'installs',
        ) + tuple( 'rpi{}'.format(i) for i in rpi_range)
        self._zero_row = [0.0 for _ in rpi_range]

    def _table_row(self) -> list:
        for country_code, installs in self.installs.items():
            revenues_per_day = self.revenues.get(country_code, self._zero_row)
            row = [country_code, installs] + [
                format(revenue/installs, '.2f') for revenue in revenues_per_day 
            ]
            yield dict(zip(self._fieldnames, row))

    def write_to(self, file_name: str)-> None:
        with open(file_name, 'w') as rpi_file:
            writer = DictWriter(rpi_file, fieldnames=self._fieldnames)
            writer.writeheader()
            writer.writerows(self._table_row())

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
    print(installs_collector.results)
    print(rpi_colletor.results)

    rpi_writer = RpisWriter(installs_collector.results, rpi_colletor.results, rpi_range)
    rpi_writer.write_to(config['output']['results_data_file'])

if __name__ == "__main__":
    collect_rpi_into_csv()
