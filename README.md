# Scraper

Сервис сбора данных.

1. Внешний сервис с данными отдает два отчета: input(JSON).

2. Ссылки формируются из url сервиса и названия файла: url/filename

3. Таблицы в БД:
a.
```
report_input 
  (user (int)
  ts (timestamp || datetime)
  context (json || text) (Нужна проверка на JSON)
  ip (varchar))
```
b. 
```
data_error 
  (api_report (varchar)
   api_date (date)
   row_text (varchar)
   error_text (varchar)
   ins_ts (timestamp || datetime))
```
  
  4. Данные для подключению к сервису:
  
  a. Url - https://snap.datastream.center/techquest
  
  b. Шаблон имени файла : “{report}-{date}.{format}.gz”
  
  c. Готовые ссылки:i. https://snap.datastream.center/techquest/input-2017-02-01.json.gz
  
  Задача:
  
  1. Загружаем данные из сервиса
  
  2. Валидируем и преобразуем согласно типу данных в таблице.
  
  3. Вставляем данные в таблицу, ошибочные данные помещаются в таблицуdata_error с описанием ошибки
  
  Требования:1. Python >= 3.62. 
  
  Решение выложить на один из сервисов: GitHub, GitLab, BitBucket ...


## Example
$
<code>
docker-compose up postgres 
</code>

$ 
<code>
python3 scraper.py
</code>

