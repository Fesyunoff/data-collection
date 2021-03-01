#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import gzip
import shutil
import json
import psycopg2
import postgres as db
import entities as ent


def main():
    report = 'input'
    date = '2017-02-01'
    file_format = 'json'
    schema = 'test'
    input_table = 'report_input'
    error_table = 'data_error'
    file_name = '{0}-{1}.{2}.gz'.format(report, date, file_format)

    conn = db.get_client(True)
    storage = db.Storage(schema, input_table, error_table, conn)
    storage.prepeare_storage()

    data = get_data(file_name)

    content_strings = data.split('\n')
    for row in content_strings:
        if row != '':
            event = ent.Event(report, date)
            event.validate(row)
            storage.insert_to_storage(event)

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

if __name__ == "__main__":
    main()  


