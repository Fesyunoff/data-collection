#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import psycopg2
import entities

def get_client(verbose=False):
    conn_info = {
        'port': '5432',
        'host': '172.18.0.2',
        'database': 'postgres',
        'password': 'pass',
        'user': 'user'}
    if verbose:
        print(conn_info)
    client = psycopg2.connect(**conn_info)
    return client

def execute_query(query, conn, verbose=False):
    if verbose:
        print(query)
    conn = get_client()
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()

class Storage:

    def __init__(self, schema, input_table, error_table, conn):
        self._schema_name = schema
        self._input_table = input_table
        self._error_table = error_table
        self._conn = conn
    
    def prepeare_storage(self):

        execute_query(self.create_input_table_SQL(), self._conn, True)
        execute_query(self.create_error_table_SQL(), self._conn, True)

    
    def insert_to_storage(self, event: entities.Event):
        if event.is_valid():
            execute_query(self.insert_valid_SQL(event.data), self._conn,True)
        else:
            execute_query(self.insert_error_SQL(event.report, 
                                                event.date, 
                                                event.row, 
                                                str(event.error).replace("'",'"')), 
                                                self._conn, True)

    def create_input_table_SQL(self):
        return """
    CREATE SCHEMA IF NOT EXISTS %(schema_name)s;

    CREATE TABLE IF NOT EXISTS %(schema_name)s.%(table_name)s(
          id SERIAL PRIMARY KEY,
          user_id BIGINT, 
          ts TIMESTAMP, 
          context JSON, 
          ip VARCHAR  
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._input_table,
    }

    def create_error_table_SQL(self):
        return """
    CREATE SCHEMA IF NOT EXISTS %(schema_name)s;

    CREATE TABLE IF NOT EXISTS %(schema_name)s.%(table_name)s(
         id SERIAL PRIMARY KEY,
         api_report  VARCHAR, 
         api_date    DATE, 
         row_text   VARCHAR, 
         error_text    VARCHAR, 
         ins_ts TIMESTAMP 
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._error_table,
    }

    def insert_valid_SQL(self, data):
        return """

    INSERT INTO %(schema_name)s.%(table_name)s(
    user_id, ts, context, ip)
    VALUES (
    %(user_id)d, to_timestamp(%(ts)f), '%(context)s', '%(ip)s'
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._input_table,
        'user_id': data['user'],
        'ts': data['ts'],
        'context': json.dumps(data['context']),
        'ip': data['ip']
    }
    
    def insert_error_SQL(self, report, date, row, error):
        return """

    INSERT INTO %(schema_name)s.%(table_name)s(
    api_report, api_date, row_text, error_text, ins_ts)
    VALUES (
    '%(report)s', to_date('%(date)s', 'YYYY-MM-DD'),'%(row)s', '%(error)s', NOW()
         );""" % {
        'schema_name': self._schema_name,
        'table_name': self._error_table,
        'report': report,
        'date': date,
        'row': row,
        'error': error
    }
    
