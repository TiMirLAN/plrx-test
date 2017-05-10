#!/usr/bin/env python3
# coding=utf-8

"""
Решение тестового задания.
"""

import configparser
from csv import reader

__author__ = "Tim Mironov"
__email__ = "timirlan666@gmail.com"

def get_config() -> configparser.ConfigParser:
    """
    Получение конфигов для входных и выходных данных. Лень парсить аргументы.
    """
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def collect_rpi_into_csv() -> None:
    """
    Основная фунция. Собирает rpi по дням и создаёт файл с результатом.
    """
    config = get_config()

if __name__ == "__main__":
    collect_rpi_into_csv()
