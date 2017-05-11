# coding=utf-8

"""
Модуль со сборщиками статистики.
"""

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
    DATETIME_FORMAT = None
    def __init__(self, query: dict) -> None:
        """
        Коснтруктор коллектора статистики.

        :params query: Данные для выборки. Должны содержать install_date
        в виде двух дат и app_id в виде строки.
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
        Обрабатывает строку данных которая приходит в *args
        сохраняя результаты в self.results

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
        """
        Функция подготвоки часто используемых данных.

        Операции производятся только над теми даными,
        которые необходимы на каждой итерации сборки.
        """
        row_data['install_date'] = self._parse_datetime(row_data['install_date'])

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

def set_up_collectors_date_format(strptime_format: str) -> None:
    """
    Настройка формата искользуемой для парсинга даты коллекторами.
    """
    BaseCollector.DATETIME_FORMAT = strptime_format
