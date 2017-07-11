from helpers.filereader import MongoReader
from utils.queryutils import MongoMatch
import pandas as pd
from collections import namedtuple


class BookingDump:
    """Data model class that holds all the Booking Dump model attributes 
    and functionalities"""

    def __init__(self, period=None, serv_indi=None, **kwargs):
        super().__init__(**kwargs)
        self.match_qry = { 'services_indicator': serv_indi }
        self.grp_qry = {}
        self.prj_qry = {}
        self.qry = []
        self.latest = None
        if serv_indi is not None:
            self.match_qry['services_indicator'] = serv_indi
        self.period_qry = \
            MongoMatch(period, self._get_period_fields()).get_period_obj()
        if not self.period_qry:
            self.set_latest_period()
        self.rdr = MongoReader('ent_dump_from_finance')

    def __str__(self):
        """Overriding __str__ method on class"""
        if not self.period_qry:
            return self.__print_self_all()
        return self.__print_self_specific()

    def _get_period_fields(self):
        """Creates and Returns Field namedtuple object with period's MongoDB
        field names"""
        Field = namedtuple('Field', ['year', 'quarter', 'month', 'week'])
        return Field('fiscal_quarter_id', 'fiscal_quarter_id', 'fiscal_period_id',
                     'fiscal_week_id')

    def set_latest_period(self):
        """Queries and returns the lates Booking dump period as 
        tuples containing quarter, month, week"""
        print('Querying Latest Periods...')
        self.prj_qry = { '_id': 0, 'data': '$_id' }
        Latest = namedtuple('Latest', ['year', 'quarter', 'month', 'week'])
        qtr = self.get_latest_quarter()
        self.latest = Latest._make(qtr[:4], qtr, self.get_latest_month(),
                                   self.get_latest_week())
        return

    def get_latest_quarter(self):
        """Returns the latest financial quarter of the booking data"""
        return self._get_max_value('$fiscal_quarter_id')

    def get_latest_month(self):
        """Returns the latest financial month of the booking data"""
        return self._get_max_value('$fiscal_period_id')

    def get_latest_week(self):
        """Returns the latest financial week of the booking data"""
        return self._get_max_value('$fiscal_week_id')

    def _get_max_value(self, field):
        """Calculates and Returns Maximum value"""
        self.grp_qry['_id'] = field
        self._append_qry({'$group': self.grp_qry}, {'$project': self.prj_qry})
        #print(self.qry)
        result = self.rdr.run_agg(self.qry)
        return list(result.max())[0]

    def _append_qry(self, *argv):
        """Receives query objects and appends to the final query obj"""
        del self.qry[:] # Emptying the existing list of objects
        for arg in argv:
            self.qry.append(arg)
        return

    def __print_self_all(self):
        """Prints the 'self' object with all attributes"""
        return """
        =========================================================================
        %s has =>
        Latest Year    => %s
        Latest Quarter => %s
        Latest Month   => %s
        Latest Week    => %s
        =========================================================================
        =========================================================================
        Query Objects
        =============
        Period Object:
        %s
        =========================================================================
        """ % (self.__class__.__name__, self.latest.year, self.latest.quarter,
               self.latest.month, self.latest.week, self.period_qry)

    def __print_self_specific(self):
        """Prints the 'self' object with specific attributes"""
        return """
        =========================================================================
        %s has =>
        Query Objects
        =============
        Period Object:
        %s
        =========================================================================
        """ % (self.__class__.__name__, self.period_qry)
