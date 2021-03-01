#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import gzip
import shutil
import json
import psycopg2
import postgres as db


def main():
    report = 'input'
    date = '2017-02-01'
    file_format = 'json'
    schema = 'test'
    input_table = 'report_input'
    error_table = 'data_error'
    file_name = '{0}-{1}.{2}.gz'.format(report, date, file_format)

    conn = db.get_client(True)
    i = db.Input(schema, input_table, conn)
    e = db.Error(schema, error_table, conn)

    data = get_data(file_name)

    content_strings = data.split('\n')
    for row in content_strings:
        if row != '':
            data, err = validate(row)
            if err != '':
                db.execute_query(e.insert_row_SQL(report, date, row, str(err).replace("'",'"')), conn, True)
            else:
                db.execute_query(i.insert_row_SQL(data), conn, True)

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

def validate(string, border=1451595600):
    err = ''
    try:
        data = json.loads(string)
        if data['ts'] < border:
            try:
                raise ValueError('Uncorrect date format')
            except ValueError as err:
                return string, err
        return data, err 
    except json.decoder.JSONDecodeError as err:
        err = str(err).replace("'",'"')
        return string, err

if __name__ == "__main__":
    main()  


