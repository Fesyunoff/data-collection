#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json

class Event:

    def __init__ (self, row, report, date):
        self.report = report
        self.date = date
        self.row = row
        self.data = {}
        self.error = ValueError('')

    def _set_data(self, data):
        self.data = data
        return True
    
    def _set_error(self, error):
        self.error = error
        return False

    def is_valid(self, border=1451595600):
        try:
            data = json.loads(self.row)
            if data['ts'] < border:
                return self._set_error(ValueError('Uncorrect date format'))
            else:
               return self._set_data(data)

        except json.decoder.JSONDecodeError as err:
            return self._set_error(err)

