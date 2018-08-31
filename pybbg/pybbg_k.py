# -*- coding: utf-8 -*-
"""
Edited on 18/01/2017

"""

from __future__ import print_function
import blpapi
from collections import defaultdict
from pandas import DataFrame
from datetime import date
import pandas as pd
import numpy as np
import sys
import warnings
import six
from dateutil.relativedelta import relativedelta
import json

def _convert_to_py_list(bql_values, bql_type):
    conversions = {
        'INT': _int_to_py_type,
        'BOOLEAN': _bool_to_py_type,
        'DOUBLE': _double_to_py_type,
        'STRING': _string_to_py_type,
        'ENUM': _string_to_py_type,
        'DATE': _date_to_py_type,
        'DATETIME': _datetime_to_py_type
    }

    converter = conversions.get(bql_type)
    if converter is None:
        raise Exception('No converter for BQL type "{}"'.format(bql_type))

    return [converter(value) for value in bql_values]

def convert_to_np_array(bql_values, bql_type):
    import numpy

    np_types = {
        'INT': 'int64',
        'BOOLEAN': 'bool',
        'DOUBLE': 'float64',
        'STRING': 'object',
        'ENUM': 'object',
        'DATE': 'datetime64[ns]',
        'DATETIME': 'datetime64[ns]'
    }

    py_values = _convert_to_py_list(bql_values, bql_type)

    np_type = np_types.get(bql_type)
    if np_type is None:
        raise Exception('No numpy type for BQL type "{}"'.format(bql_type))

    return numpy.array(py_values, dtype=np_type)

class Pybbg():
    def __init__(self, host='localhost', port=8194):
        """
        Starting bloomberg API session
        close with session.close()
        """
        # Fill SessionOptions
        sessionOptions = blpapi.SessionOptions()
        sessionOptions.setServerHost(host)
        sessionOptions.setServerPort(port)

        self.initialized_services = set()
        self.services = dict()

        # Create a Session
        self.session = blpapi.Session(sessionOptions)

        # Start a Session
        if not self.session.start():
            print("Failed to start session.")

        self.session.nextEvent()

    def _ensure_service(self, svc):
        if svc in self.initialized_services:
            return

        if not self.session.openService(svc):
            print("Failed to open %s" % svc)

        self.session.nextEvent()

        # Obtain previously opened service
        self.services[svc] = self.session.getService(svc)

        self.session.nextEvent()

        self.initialized_services.add(svc)

    def _create_request(self, svc, request):
        return self.services[svc].createRequest(request)

    def _create_refdata_request(self, request):
        return self._create_request('//blp/refdata', request)

    def _create_bql_request(self, request):
        return self._create_request('//blp/bqlsvc', request)

    def _ensure_refdata_service(self):
        """
        init service for refData
        """
        self._ensure_service(svc = '//blp/refdata')

    def _ensure_bql_service(self):
        self._ensure_service(svc = '//blp/bqlsvc')


    def bql(self, ticker_list, fld_list, start_date, end_date, frq= 'd', fill= 'prev'):
        self._ensure_bql_service()

        if isstring(ticker_list):
            ticker_list = [ticker_list]
        if isstring(fld_list):
            fld_list = [fld_list]

        # -------- build the query string ----------
        from six import StringIO
        query_buf = StringIO()
        query_buf.write('get(')
        for i,field in enumerate(fld_list):
            query_buf.write(field)
            query_buf.write('(start=')
            query_buf.write(start_date.strftime('%Y-%m-%d,end='))
            query_buf.write(end_date.strftime('%Y-%m-%d,frq='))
            query_buf.write(frq)
            query_buf.write(',fill=')
            query_buf.write(fill)
            query_buf.write(')')
            if i != len(fld_list)-1:
                query_buf.write(',')

        query_buf.write(")for(['")
        query_buf.write("','".join(ticker_list))
        query_buf.write("'])")


        # ---------- send the request

        query = query_buf.getvalue()
        request = self._create_bql_request('sendQuery')
        request.set('expression',query)

        self.session.sendRequest(request)

        result_json_str = None

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                result_json_str = msg.asElement().getValueAsString()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        if result_json_str is None:
            return None

        result = json.loads(result_json_str)

        # ------------ make JSON into a pandas DataFrame

        if 'results' not in result:
            return None

        results = result['results']
        if results is None or not hasattr(results,'__len__') or len(results) == 0:
            return None

        pandas_maker = dict()
        for col_name in results:
            data = results[col_name]
            if '(' in col_name:
                col_name = col_name[:col_name.index('(')]

            if 'date' not in pandas_maker:
                pandas_maker['date'] = data['secondaryColumns'][0]['values']
                pandas_maker['id'] = data['idColumn']['values']

            pandas_maker[col_name] = data['valuesColumn']['values']

        return_df = DataFrame(pandas_maker)
        return_df['date'] = return_df['date'].astype('datetime64[ns]').dt.date

        return return_df[['date','id'] + fld_list]



    def bdh(self, ticker_list, fld_list, start_date, end_date=date.today().strftime('%Y%m%d'), periodselection='DAILY', overrides=None, other_request_parameters=None, move_dates_to_period_end=False):
        """
        Get ticker_list and field_list
        return pd multi level columns dataframe
        """
        # Create and fill the request for the historical data
        self._ensure_refdata_service()

        if isstring(ticker_list):
            ticker_list = [ticker_list]
        if isstring(fld_list):
            fld_list = [fld_list]

        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y%m%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y%m%d')

        request = self._create_refdata_request("HistoricalDataRequest")
        for t in ticker_list:
            request.getElement("securities").appendValue(t)
        for f in fld_list:
            request.getElement("fields").appendValue(f)
        request.set("periodicitySelection", periodselection)
        request.set("startDate", start_date)
        request.set("endDate", end_date)


        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        if other_request_parameters is not None:
            for k,v in six.iteritems(other_request_parameters):
                request.set(k, v)

        def adjust_date(to_adjust):
            if periodselection == 'MONTHLY':
                # always make the date the last day of the month
                return date(to_adjust.year, to_adjust.month, 1) + relativedelta(months=1) - relativedelta(days=1)
            if periodselection == 'WEEKLY':
                return to_adjust + relativedelta(weekday=4)

            return to_adjust

        # print("Sending Request:", request)
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pd
        data = defaultdict(dict)
        warnings.warn(str(data))
        # Process received events
        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                ticker = msg.getElement('securityData').getElement('security').getValue()
                fieldData = msg.getElement('securityData').getElement('fieldData')
                for i in range(fieldData.numValues()):
                    for j in range(1, fieldData.getValue(i).numElements()):
                        dt = fieldData.getValue(i).getElement(0).getValue()
                        if move_dates_to_period_end:
                            dt = adjust_date(dt)

                        data[(ticker, fld_list[j - 1])][dt] = fieldData.getValue(i).getElement(j).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        if len(data) == 0:
            # security error case
            return DataFrame()

        if len(fld_list) == 1:
            data = {k[0]: v for k, v in data.items()}
            data = DataFrame(data)
            data.index = pd.to_datetime(data.index)
            return data


        data = DataFrame(data)

        # fix column order
        new_col_order = [t for t in ticker_list if t in data.columns]
        data = data[new_col_order]

        data.columns = pd.MultiIndex.from_tuples(data, names=['ticker', 'field'])
        data.index = pd.to_datetime(data.index)
        return data

    def bdib(self, ticker, fld_list, startDateTime, endDateTime, eventType='TRADE', interval=1):
        """
        Get one ticker (Only one ticker available per call); eventType (TRADE, BID, ASK,..etc); interval (in minutes)
                ; fld_list (Only [open, high, low, close, volumne, numEvents] availalbe)
        return pd dataframe with return Data
        """
        self._ensure_refdata_service()
        # Create and fill the request for the historical data
        request = self._create_refdata_request("IntradayBarRequest")
        request.set("security", ticker)
        request.set("eventType", eventType)
        request.set("interval", interval)  # bar interval in minutes
        request.set("startDateTime", startDateTime)
        request.set("endDateTime", endDateTime)

        # print "Sending Request:", request
        # Send the request
        self.session.sendRequest(request)
        # defaultdict - later convert to pd
        data = defaultdict(dict)
        # Process received events
        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                barTickData = msg.getElement('barData').getElement('barTickData')
                for i in range(barTickData.numValues()):
                    for j in range(len(fld_list)):
                        data[(fld_list[j])][barTickData.getValue(i).getElement(0).getValue()] = barTickData.getValue(
                            i).getElement(fld_list[j]).getValue()

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break
        data = DataFrame(data)
        data.index = pd.to_datetime(data.index)
        return data

    def bdp(self, ticker, fld_list, overrides=None):
        # print(ticker, fld_list, overrides)
        self._ensure_refdata_service()

        request = self._create_refdata_request("ReferenceDataRequest")
        if isstring(ticker):
            ticker = [ticker]

        securities = request.getElement("securities")
        for t in ticker:
            securities.appendValue(t)

        if isstring(fld_list):
            fld_list = [fld_list]

        fields = request.getElement("fields")
        for f in fld_list:
            fields.appendValue(f)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        self.session.sendRequest(request)
        data = dict()

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                securityData = msg.getElement("securityData")

                for i in range(securityData.numValues()):
                    fieldData = securityData.getValue(i).getElement("fieldData")
                    secId = securityData.getValue(i).getElement("security").getValue()
                    data[secId] = dict()
                    for field in fld_list:
                        if fieldData.hasElement(field):
                            data[secId][field] = fieldData.getElement(field).getValue()
                        else:
                            data[secId][field] = np.NaN

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        return pd.DataFrame.from_dict(data)

    def bds(self, security, field, overrides=None):

        self._ensure_refdata_service()

        request = self._create_refdata_request("ReferenceDataRequest")
        assert isstring(security)
        assert isstring(field)

        securities = request.getElement("securities")
        securities.appendValue(security)

        fields = request.getElement("fields")
        fields.appendValue(field)

        if overrides is not None:
            overrideOuter = request.getElement('overrides')
            for k in overrides:
                override1 = overrideOuter.appendElement()
                override1.setElement('fieldId', k)
                override1.setElement('value', overrides[k])

        # print(request)
        self.session.sendRequest(request)
        data = dict()

        while (True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = self.session.nextEvent(500)
            for msg in ev:
                # processMessage(msg)
                securityData = msg.getElement("securityData")
                for i in range(securityData.numValues()):
                    fieldData = securityData.getValue(i).getElement("fieldData").getElement(field)
                    for i, row in enumerate(fieldData.values()):
                        for j in range(row.numElements()):
                            e = row.getElement(j)
                            k = str(e.name())
                            v = e.getValue()
                            if k not in data:
                                data[k] = list()

                            data[k].append(v)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completly received, so we could exit
                break

        return pd.DataFrame.from_dict(data)

    def stop(self):
        self.session.stop()


def isstring(s):
    # if we use Python 3
    if (sys.version_info[0] == 3):
        return isinstance(s, str)
    # we use Python 2
    return isinstance(s, basestring)


def processMessage(msg):
    SECURITY_DATA = blpapi.Name("securityData")
    SECURITY = blpapi.Name("security")
    FIELD_DATA = blpapi.Name("fieldData")
    FIELD_EXCEPTIONS = blpapi.Name("fieldExceptions")
    FIELD_ID = blpapi.Name("fieldId")
    ERROR_INFO = blpapi.Name("errorInfo")

    securityDataArray = msg.getElement(SECURITY_DATA)
    for securityData in securityDataArray.values():
        print(securityData.getElementAsString(SECURITY))
        fieldData = securityData.getElement(FIELD_DATA)
        for field in fieldData.elements():
            for i, row in enumerate(field.values()):
                for j in range(row.numElements()):
                    e = row.getElement(j)
                    print("Row %d col %d: %s %s" % (i, j, e.name(), e.getValue()))
