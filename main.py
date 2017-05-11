#!/usr/bin/env python3.5
# coding=utf-8

"""
Решение тестового задания.
"""

from configparser import ConfigParser
from collections import defaultdict
from csv import DictReader
from datetime import datetime as dt

__author__ = "Tim Mironov"
__email__ = "timirlan666@gmail.com"


class BaseCollector:
    """
    Базовый класс коллекторов статистики.
    Реализован поскольку часть логики обработки файлов статистики совпадает.
    """
    DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    def __init__(self, query: dict) -> None:
        """
        Коснтруктор коллектора статистики.

        :params query: Данные для выборки. Должны содержать install_date в виде двух дат и app_id в виде строки.
        :raises ValueError: Если интервал дат в выборке задан неверно.
        :raises KeyError: Если в queue не хватает полей.
        """
        self.from_datetime, self.to_datetime = [
            self._parse_datetime(d) for d in query['install_date'].split(',')
        ]
        self.app_id = query['app_id']
        if self.from_datetime > self.to_datetime:
            raise ValueError('Bad datetime interval. Check datetime_interval_to_handle.')
        self.results = None
        self._fieldnames = None

    def _parse_datetime(self, timestamp: str) -> dt:
        """
        Всппомогательная функция для парсинга дат.

        :params timestamp: Строка с датой.
        :returns: datetime object
        :raises ValueError: Если дату не удалось распознать. 
        """
        return dt.strptime(timestamp, self.DATETIME_FORMAT)

    def _collect(self,**kargs) -> None:
        """
        Обрабатывает строку данных которая приходит в *args сохраняя результаты в self.results

        Не имплементировано.
        """
        raise NotImplemented()

    def _is_suitable_row(self, install_date: dt, app_id: str, **kwargs) -> bool:
        """
        Функция которая проверяет входит ли строка данных в выборку.

        :returns: bool
        """
        return app_id == self.app_id and\
                (self.from_datetime <= install_date <= self.to_datetime)

    def _prepare_row_data(self, row_data) -> None:
        row_data['install_date'] =self._parse_datetime(row_data['install_date'])

    def collect_from(self, file_name: str) -> None:
        """
        Производит построчную загрузку строк файла, указанного в аргументах, и
        отправлет их в self._collect если они удовлетворяют условиям выборки.

        :params file_name: CSV-файл для загрузки.
        """
        with open(file_name) as data_file:
            for row_data in DictReader(data_file, fieldnames=self._fieldnames):
                self._prepare_row_data(row_data)
                if self._is_suitable_row(**row_data):
                    self._collect(**row_data)

    def results_to_dict(self) -> dict:
        return dict(self.results)

class InstallsCollector(BaseCollector):
    """
    Коллектор количества установок.

    Считает суммарное количество установок по странам за указанный период
    для указанного app_id.
    """
    def __init__(self, query: dict) -> None:
        super().__init__(query)
        self._fieldnames = ['install_date', 'app_id', 'country_code']
        self.results = defaultdict(int)

    def _collect(self, country_code: str, **kwargs) -> None:
        self.results[country_code] += 1

class RpisCollector(BaseCollector):
    """
    Коллектор значений rpi по дням.

    Считает rpi в указанном диапазоне дней за указанный период
    для указанного app_id.
    """
    def __init__(self, query: dict, rpi_range: range) -> None:
        super().__init__(query)
        self.rpi_range = rpi_range
        self._fieldnames = [
            'payment_date',
            'app_id',
            'country_code',
            'install_date',
            'revenue',
        ]
        self.results = defaultdict(float)

    def _prepare_row_data(self, row_data: dict) -> None:
        super()._prepare_row_data(row_data)
        payment_datetime = self._parse_datetime(row_data['payment_date'])
        row_data['payment_date'] = payment_datetime 
        row_data['days_from_install'] = (payment_datetime - row_data['install_date']).days

    def _is_suitable_row(self, install_date: dt, app_id: str, days_from_install: int, **kwargs) -> bool:
        is_suitable = super()._is_suitable_row(install_date, app_id)
        return is_suitable and days_from_install in self.rpi_range

    def _collect(self, days_from_install: int, country_code: str, revenue: str, **kwargs):
        """
        Функция подсчёта. Сохраняет в results суммарную выручку за каждый отдельный день
        из self.rpi_range.
        """
        self.results[(country_code, days_from_install)] += float(revenue)

def get_config() -> ConfigParser:
    """
    Получение конфигов для входных и выходных данных. Лень парсить аргументы.
    """
    config = ConfigParser()
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
    default_query = dict(config['query'])
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
