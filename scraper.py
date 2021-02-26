#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import gzip
import shutil
import json
import psycopg2


CLIENT = None

def get_client():
    global CLIENT
    conn_info = {
        'port': '5432',
        'host': '172.18.0.2',
        'password': 'pass',
        'database': 'postgres',
        'user': 'user'}
    if CLIENT is None:
        print(conn_info)
        CLIENT = psycopg2.connect(**conn_info)
    return CLIENT


def get_data(file_name):

    url = 'https://snap.datastream.center/techquest/{}'.format(file_name)
    r = requests.get(url)

    with open('input_file.json.gz', 'wb') as output_file:
        output_file.write(r.content)
        output_file.close()

    f = gzip.open('input_file.json.gz', 'rb')
    file_bytes = f.read()
    file_string = file_bytes.decode('utf-8')
    f.close()
    return file_string

def validate(string):
    err = ''
    try:
        data = json.loads(string)
        if data['ts'] < 1451595600:
           err = 'Invalid time'
           return string, err
        return data, err 
    except json.decoder.JSONDecodeError:
        err = 'Invalid JSON'
        return string, err

def main():
    report = 'input'
    date = '2017-02-01'
    f_format = 'json'
    schema = 'test'
    input_table = 'report_input'
    error_table = 'data_error'
    file_name = '{0}-{1}.{2}.gz'.format(report, date, f_format)

    create_tables(schema, input_table, error_table)

    data = get_data(file_name)

    content_strings = data.split('\n')
    for string in content_strings:
        if string != '':
            data, err = validate(string)
            if err == '':
                insert_valid_row(schema, input_table, data)
            else:
                insert_error_row(schema, error_table, report, date, data, err)

def create_tables(schema, input_table, error_table):

    query = ("CREATE SCHEMA " 
             "IF NOT EXISTS {};").format(schema)
    conn = get_client()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = ("CREATE TABLE "
             "IF NOT EXISTS {0}.{1}("
             " id SERIAL PRIMARY KEY,"
             " user_id BIGINT,"
             #  " ts_raw REAL,"
             " ts TIMESTAMP(4), "
             " context JSON, "
             " ip VARCHAR  "
             ");").format(schema, input_table)

    print(query)
    conn = get_client()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

    query = ("CREATE TABLE "
             "IF NOT EXISTS {0}.{1}("
             " id SERIAL PRIMARY KEY,"
             " api_report  VARCHAR,  "
             " api_date    DATE, "
             " row_text   VARCHAR,  "
             " error_text    VARCHAR,  "
             " ins_ts TIMESTAMP "
             ");").format(schema, error_table)

    print(query)
    conn = get_client()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


def insert_valid_row(schema, input_table, data):

    query = ("INSERT INTO "
             "{0}.{1}(user_id, ts, context, ip) "
             "VALUES ({2}, to_timestamp({3}), '{4}', '{5}')"
             ";").format(schema,
                         input_table,
                         data['user'],
                         data['ts'],
                         json.dumps(data['context']),
                         data['ip'])
    print(query)

    conn = get_client()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

def insert_error_row(schema, error_table, api_report, api_date, row_text, error_text):

    query = ("INSERT INTO "
             "{0}.{1}(api_report, api_date, row_text, error_text, ins_ts) "
             "VALUES ('{2}', to_date('{3}', 'YYYY-MM-DD'), '{4}', '{5}', NOW())"
             ";").format(schema,
                         error_table,
                         api_report,
                         api_date,
                         row_text,
                         error_text)
    print(query)

    conn = get_client()
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
if __name__ == "__main__":
    main()  


