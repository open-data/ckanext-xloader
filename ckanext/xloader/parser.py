# -*- coding: utf-8 -*-
import datetime
from decimal import Decimal, InvalidOperation
import re
import six

from ckan.plugins.toolkit import asbool
from dateutil.parser import isoparser, parser, ParserError

from ckan.plugins.toolkit import config

from tabulator.parsers.csv import CSVParser
from tabulator.config import CSV_SAMPLE_LINES

from csv import Dialect
from _csv import Dialect as _Dialect

DATE_REGEX = re.compile(r'''^\d{1,4}[-/.\s]\S+[-/.\s]\S+''')


class CanadaCSVDialect(Dialect):

    _name = 'csv'
    _valid = False
    # placeholders
    delimiter = None
    quotechar = None
    escapechar = None
    doublequote = None
    skipinitialspace = None
    lineterminator = None
    quoting = None

    def __init__(self, static_dialect):
        for k in static_dialect:
            if isinstance(static_dialect[k], six.text_type):
                # must be strings and not unicode
                setattr(self, k, static_dialect[k].encode('utf-8'))
            else:
                setattr(self, k, static_dialect[k])
        if self.__class__ != Dialect and self.__class__ != CanadaCSVDialect:
            self._valid = True
        self._validate()

    def _validate(self):
        # will raise an exception if it is not a valid Dialect
        _Dialect(self)


class CanadaCSVParser(CSVParser):

    options = [
        'static_dialect',
        'logger',
    ]

    def __init__(self, loader, *args, **kwargs):
        super(CanadaCSVParser, self).__init__(loader, *args, **kwargs)
        self.static_dialect = kwargs.get('static_dialect', None)
        self.logger = kwargs.get('logger', None)
        # we only want to mangle the parent method if a static dialect
        # is supplied. Otherwise, we want the parent method to be called as normal.
        if self.static_dialect:
            self._CSVParser__prepare_dialect = self.__mangle__prepare_dialect

    @property
    def dialect(self):
        if self.static_dialect:
            if self.logger:
                self.logger.info('Using Static Dialect for csv: %r', self.static_dialect)
            return self.static_dialect
        return super(CanadaCSVParser, self).dialect

    def __mangle__prepare_dialect(self, stream):

        # Get sample
        # Copied from tabulator.pasrers.csv
        # Needed because we cannot call parent private method while mangling.
        sample = []
        while True:
            try:
                sample.append(next(stream))
            except StopIteration:
                break
            if len(sample) >= CSV_SAMPLE_LINES:
                break

        if self.logger:
            self.logger.info('Using Static Dialect for csv: %r', self.static_dialect)

        return sample, CanadaCSVDialect(self.static_dialect)


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
