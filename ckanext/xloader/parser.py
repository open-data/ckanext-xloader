# -*- coding: utf-8 -*-
import datetime
from decimal import Decimal, InvalidOperation
import re
import six

from json import dumps

from ckan.plugins.toolkit import asbool
from dateutil.parser import isoparser, parser, ParserError

from ckan.plugins.toolkit import config

from tabulator.parsers.csv import CSVParser

try:
    from unicodecsv import Dialect
except ImportError:
    from csv import Dialect

CSV_SAMPLE_LINES = 1000
DATE_REGEX = re.compile(r'''^\d{1,4}[-/.\s]\S+[-/.\s]\S+''')


class CanadaCSVDialect(Dialect):

    _name = 'csv'

    def __init__(self, static_dialect=None):
        super(CanadaCSVDialect, self).__init__()
        for k in static_dialect:
            setattr(self, k, static_dialect[k])


class CanadaCSVParser(CSVParser):

    options = [
        'static_dialect',
        'logger',
    ]

    def __init__(self, loader, *args, **kwargs):
        super(CanadaCSVParser, self).__init__(loader, *args, **kwargs)
        self.static_dialect = kwargs.get('static_dialect', None)
        self.logger = kwargs.get('logger', None)

    @property
    def dialect(self):
        if self.static_dialect:
            if self.logger:
                self.logger.info('Using Static Dialect for csv: %s', dumps(self.static_dialect))
            return self.static_dialect
        return super(CanadaCSVParser, self).dialect()

    def __prepare_dialect(self, stream):
        sample, dialect = super(CanadaCSVParser, self).__prepare_dialect(stream)
        if self.static_dialect:
            if self.logger:
                self.logger.info('Using Static Dialect for csv: %s', dumps(self.static_dialect))
            return sample, CanadaCSVDialect(self.static_dialect)
        if self.logger:
            self.logger.info('Using Tabulator Dialect for csv: %s', dumps(dialect))
        return sample, dialect


class TypeConverter:
    """ Post-process table cells to convert strings into numbers and timestamps
    as desired.
    """

    def __init__(self, types=None):
        self.types = types

    def convert_types(self, extended_rows):
        """ Try converting cells to numbers or timestamps if applicable.
        If a list of types was supplied, use that.
        If not, then try converting each column to numeric first,
        then to a timestamp. If both fail, just keep it as a string.
        """
        for row_number, headers, row in extended_rows:
            for cell_index, cell_value in enumerate(row):
                if cell_value is None:
                    row[cell_index] = ''
                if not cell_value:
                    continue
                cell_type = self.types[cell_index] if self.types else None
                if cell_type in [Decimal, None]:
                    converted_value = to_number(cell_value)
                    if converted_value:
                        row[cell_index] = converted_value
                        continue
                if cell_type in [datetime.datetime, None]:
                    converted_value = to_timestamp(cell_value)
                    if converted_value:
                        row[cell_index] = converted_value
            yield (row_number, headers, row)


def to_number(value):
    if not isinstance(value, six.string_types):
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def to_timestamp(value):
    if not isinstance(value, six.string_types) or not DATE_REGEX.search(value):
        return None
    try:
        i = isoparser()
        return i.isoparse(value)
    except ValueError:
        try:
            p = parser()
            yearfirst = asbool(config.get('ckanext.xloader.parse_dates_yearfirst', False))
            dayfirst = asbool(config.get('ckanext.xloader.parse_dates_dayfirst', False))
            return p.parse(value, yearfirst=yearfirst, dayfirst=dayfirst)
        except ParserError:
            return None
