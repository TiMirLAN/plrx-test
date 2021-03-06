# Решение тестового задания на вакансию Marketing Programmer

Само [задание](https://docs.google.com/document/d/1q99ks9PqxLrIX4LBtUFdJaB7R9uNtQGqW3rSVd3_CtA/edit).

Для проверки работы:

    - распаковать архив `test_marketing.zip` в папке `./data`.
    - запустить исполняемый `./main.py`.

##Результаты 

| country | installs | rpi1 | rpi2 | rpi3 | rpi4 | rpi5 | rpi6  | rpi7  | rpi8  | rpi9  | rpi10 | 
|---------|----------|------|------|------|------|------|-------|-------|-------|-------|-------| 
| US      | 113547   | 1.49 | 3.03 | 6.06 | 8.34 | 9.86 | 10.60 | 11.38 | 12.10 | 12.88 | 13.63 | 
| UK      | 112569   | 1.18 | 2.42 | 4.84 | 6.63 | 7.86 | 8.46  | 9.06  | 9.68  | 10.29 | 10.91 | 
| AU      | 84765    | 0.84 | 1.74 | 3.39 | 4.66 | 5.56 | 5.96  | 6.42  | 6.87  | 7.29  | 7.70  | 
| NZ      | 84472    | 0.77 | 1.57 | 3.22 | 4.43 | 5.19 | 5.59  | 5.99  | 6.38  | 6.78  | 7.18  | 
| CA      | 84391    | 1.09 | 2.09 | 4.17 | 5.71 | 6.70 | 7.22  | 7.77  | 8.28  | 8.79  | 9.25  | 
| BY      | 28353    | 0.24 | 0.48 | 0.98 | 1.35 | 1.60 | 1.72  | 1.83  | 1.97  | 2.09  | 2.21  | 
| TH      | 28332    | 0.35 | 0.72 | 1.40 | 1.96 | 2.37 | 2.55  | 2.75  | 2.96  | 3.16  | 3.35  | 
| KZ      | 28309    | 0.42 | 0.85 | 1.62 | 2.19 | 2.61 | 2.84  | 3.02  | 3.19  | 3.34  | 3.55  | 
| RU      | 28296    | 0.50 | 1.07 | 2.04 | 2.92 | 3.38 | 3.67  | 3.99  | 4.24  | 4.46  | 4.71  |

Скрипт сохраняет результаты в `rpis.csv`.

##P.S.

Для проверки результатов использовал `check.py`, который использует библиотеку *pandas*.
Она как раз для таких задач.

Разбиение по процессам/тредам городить не стал как и слишком упираться в оптимизацию,
т.к. судя по профайлеру производительность упирается
в чтение из файла и парсинг csv (итерация по csv.DictReader).
