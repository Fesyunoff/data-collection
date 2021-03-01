#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

class Event:

    def __init__ (self, report, date):
        self.is_valid = False
        self.data = {}
        self.row = ''
        self.error = ValueError('')
        self.report = report
        self.date = date

    def set_valid(self, data):
        self.is_valid = True
        self.data = data
    
    def set_error(self, row, error):
        self.row =row
        self.error = error

    def validate(self, row, border=1451595600):
        try:
            data = json.loads(row)
            if data['ts'] < border:
                try:
                    raise ValueError('Uncorrect date format')

                except ValueError as err:
                    self.set_error(row, err)
            else:
               self.set_valid(data)

        except json.decoder.JSONDecodeError as err:
            self.set_error(row, err)

