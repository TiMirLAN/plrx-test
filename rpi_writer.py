# coding=utf-8
"""
Модуль содеркжит класс для вычисления и сохрения rpi по странам.
"""

from csv import DictWriter

__author__ = "Tim Mironov"
__email__ = "timirlan666@gmail.com"

class RpisWriter:
    """
    Класс для вычисления и сборки rpi по странам.

    Построчно считает rpi для стран из данных собранных InstallsCollector'ом
    и RevenueCollector'ом и записывает их в файл file_name
    """
    def __init__(self, installs: dict, revenues: dict, rpi_range: range) -> None:
        """
        Конструктор класса, принимает данные от коллекторов и
        диапазон расчёта rpi (для удобства).

        :params installs: Словарь вида код_страны -> количество_установок
        :params revenues: Словарь вида код_страны -> [суммарная_прибыль_за_день, ...]
        :params range: Диапазон дней для расчёта rpi.
        """
        self.installs = installs
        self.revenues = revenues
        self._fieldnames = (
            'country',
            'installs',
        ) + tuple('rpi{}'.format(i) for i in rpi_range)
        self._zero_row = [0.0 for _ in rpi_range]

    def table_row(self) -> dict:
        """
        Генератор строк данных для записи в результирующую
        таблицу rpi.
        """
        for country_code, installs in self.installs.items():
            revenues_per_day = self.revenues.get(country_code, self._zero_row)
            row = [country_code, installs] + [
                format(revenue/installs, '.2f') for revenue in revenues_per_day
            ]
            yield dict(zip(self._fieldnames, row))

    def write_to(self, file_name: str)-> None:
        """
        Функция построчной записи данных результирующей таблицы
        в файл с указанным именем.
        """
        with open(file_name, 'w') as rpi_file:
            writer = DictWriter(rpi_file, fieldnames=self._fieldnames)
            writer.writeheader()
            writer.writerows(self.table_row())
