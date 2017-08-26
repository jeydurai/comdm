import os
import sys
import string
import pandas as pd
from pymongo import MongoClient

class MongoReader:
    """All functionalities to read MongoDB collection"""

    def __init__(self, coll_name, db_name='ccsdm', host='localhost', port=27017):
        self.db_name = db_name
        self.coll_name = coll_name
        self.client = MongoClient(host, port)
        self.db = self.client[self.db_name]
        pd.options.display.float_format = '{:10,.2f}'.format

    def get_cols(self):
        """Queries and returns the clean collections' columns"""
        return list(self.db[self.coll_name].find({}))[0]

    def get_doc_count(self):
        """Finds and Returns the total number of documents in a collection"""
        return self.db[self.coll_name].find({}).count()

    def rows(self, qry):
        """Returns the No of documents matched for the query"""
        return self.db[self.coll_name].find(qry).count()

    def run_find(self, qry, hide):
        """Runs the find query and returns as pandas dataframe"""
        if not hide:
            return pd.DataFrame(list(self.db[self.coll_name].find(qry)))
        return pd.DataFrame(list(self.db[self.coll_name].find(qry, hide)))

    def run_agg(self, qry):
        """Runs the query returns as pandas dataframe"""
        return pd.DataFrame(list(self.db[self.coll_name].aggregate(qry)))
        

class ExcelReader:
    """All functionalities for reading excel files"""

    def __init__(self, filepath, sheetname):
        self.filepath = filepath
        self.sheetname = sheetname
        self.df = None
        self.rows = 0
        self.cols = 0

    def file_exists(self):
        """Checks if the file exists"""
        return os.path.exists(self.filepath) and \
            os.path.isfile(self.filepath)

    def read(self):
        """Reads Excel data and return as Pandas object"""
        if self.sheetname is not None:
            self._read_by_sheetname()
        else:
            self._read_from_default_sheet()
        self.print_data_shape()
        return

    def _read_from_default_sheet(self):
        """Reads the excel from the default sheet and sets 
        and sets the dataframe and the other credentials"""
        print('Reading from the default sheet...')
        try:
            self.df = pd.read_excel(self.filepath)
            self.rows = len(self.df.index)
            self.cols = len(self.df.columns)
        except:
            print('Error (ExcelRead): ', sys.exc_info()[0])
        return
            
    def _read_by_sheetname(self):
        """Reads the excel by sheetname and sets dataframe 
        and other credentials"""
        print('Reading data from sheet "%s"...' % self.sheetname)
        try:
            xl = pd.ExcelFile(self.filepath)
            self.df = xl.parse(self.sheetname)
            self.rows = len(self.df.index)
            self.cols = len(self.df.columns)
        except:
            print("Error (ExcelRead): ", sys.exc_info()[0])
        return
    
    def print_data_shape(self):
        """Prints the data shape (Rows & Columns) from the 
        Pandas dataframe"""
        head = '%s file contains' % self.filepath
        head_underline = '=' * len(head)
        print("""
        %s
        %s
        Row(s): %d
        Column(s): %d
        """ % (head, head_underline, self.rows, self.cols))
        return

    def set_standard_cols(self, coll_name):
        """Special operation for uploading in 'ent_dump_from_finance' 
        collection"""
        print('Setting standard columns...')
        self.df.rename(columns=str.lower, inplace=True)
        self.df.fillna(value=0, inplace=True)
        if coll_name == 'ent_dump_from_finance':
            mong_rdr = MongoReader('booking_dump_cols')
            cln_cols = mong_rdr.get_booking_dump_cols()
            print(cln_cols)
            self.df.rename(columns=cln_cols, inplace=True)
            print(self.df.columns)
            self.df.drop('not_to_be_mapped', axis=1, inplace=True)
        return
