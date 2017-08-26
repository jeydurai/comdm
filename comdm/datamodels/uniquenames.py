from ..utils.querymaker import Assembler
from ..utils.querymaker import MongoTools
from ..utils.stringutilities import BasicStringUtils
from ..helpers.reader import MongoReader
from ..helpers.loophelper import Progress
import pandas as pd
from pprint import pprint
import timeit


class UniqueNames(MongoReader):
    """Holds the data and functionalities of mapping unique customers and partners"""

    coll_name = 'universal_unique_names'
    cols_in_order = [ '_id', 'names', 'unique_names', 'name_catagory', 'vertical' ]
    names = 'names'
    unames = 'unique_names'
    cus_name = 'customer_name'
    ucus_name = 'unique_customers'
    par_name = 'partner_name'
    upar_name = 'unique_partners'
    cus_err_file = 'customers_unmapped.xlsx'
    par_err_file = 'partners_unmapped.xlsx'

    def __init__(self):
        self.df = None
        self.df_cus = None
        self.df_par = None
        self.qry = {}
        self.shape = None
        self.shape_dedup = None
        super().__init__(UniqueNames.coll_name)

    def read_and_set(self):
        """Queries MongoDB and stores the data"""
        print("[Info]: <from %s> Reading Data..." % self.__class__)
        self.df = self.run_find(self.qry, {})
        self.shape = self.df.shape
        return

    def clean_up(self):
        """Executes the set of private methods to clean up the data"""
        print("[Info]: <from %s> Cleaning up the Data..." % self.__class__)
        self.__lower_case_the_columns()
        self.__set_index_as_id()
        self.__reindex_axis()
        self.__drop_name_category_column()
        self.__change_names_case()
        self.__remove_duplicates()
        self.__separate_unique_names()
        return

    def map_unique_customers(self, bd):
        """Executes the actual mapping of customers' unique names"""
        bd = pd.merge(bd, self.df_cus, on=UniqueNames.cus_name, how='left')
        if bd.loc[pd.isnull(bd[UniqueNames.ucus_name])].shape[0] != 0:
            fname = UniqueNames.cus_err_file
            cols_writable = [UniqueNames.cus_name, UniqueNames.ucus_name]
            bd.loc[pd.isnull(bd[UniqueNames.ucus_name])][cols_writable].to_excel(fname, index=False)
            raise UnmappedCustomersFoundError("[Error]: Please check %s file for more info" % fname)
        return bd

    def map_unique_partners(self, bd):
        """Executes the actual mapping of partners' unique names"""
        bd = pd.merge(bd, self.df_par, on=UniqueNames.par_name, how='left')
        if bd.loc[pd.isnull(bd[UniqueNames.upar_name])].shape[0] != 0:
            fname = UniqueNames.par_err_file
            cols_writable = [UniqueNames.par_name, UniqueNames.upar_name]
            bd.loc[pd.isnull(bd[UniqueNames.upar_name])][cols_writable].to_excel(fname, index=False)
            raise UnmappedPartnersFoundError("[Error]: Please check %s file for more info" % fname)
        return bd

    def __remove_duplicates(self):
        """Removes the duplicate rows"""
        print("[Info]: <from %s> Removing duplicate rows..." % self.__class__)
        self.df.drop_duplicates('names', keep='first', inplace=True)
        self.shape_dedup = self.df.shape
        removed = self.shape[0] - self.shape_dedup[0]
        print("[Info]: <from %s> Deduplication has removed %d row(s)..." % (self.__class__, removed))
        return

    def __separate_unique_names(self):
        self.df_cus = self.__customize(what='cus')
        self.df_par = self.__customize(what='par')
        return
    
    def __customize(self, what=None):
        """Customizes the unique_names df as customer and partner specifc"""
        df = self.df.copy(deep=True)
        if what is None:
            raise UniqueNamesCustomizableError("[Error]: type of customize is None!!!")
        cols_renamable = {}
        if what.lower() == 'cus':
            cols_renamable['names'] = 'customer_name'
            cols_renamable['unique_names'] = 'unique_customers'
        elif what.lower() == 'par':
            cols_renamable['names'] = 'partner_name'
            cols_renamable['unique_names'] = 'unique_partners'
            df.drop('vertical', axis=1, inplace=True)
        else:
            raise UniqueNamesCustomizableError("[Error]: parameter 'what' must be either 'cus' OR 'par'")
        df.rename(columns=cols_renamable, inplace=True)
        return df
            
    def __lower_case_the_columns(self):
        """Changes the column names case to lower case"""
        print("[Info]: <from %s> Changing columns case as lower case..." % self.__class__)
        self.df.rename(columns=str.lower, inplace=True)
        return

    def __set_index_as_id(self):
        """Sets pandas index as _id"""
        print("[Info]: <from %s> Setting index as _id..." % self.__class__)
        self.df.set_index('_id')
        return

    def __reindex_axis(self):
        """Sets the columns in order"""
        print("[Info]: <from %s> Making the column names in order..." % self.__class__)
        self.df.reindex_axis(UniqueNames.cols_in_order, axis=1)
        return

    def __drop_name_category_column(self):
        """Drops the unwanted name category column"""
        print("[Info]: <from %s> Dropping name category column..." % self.__class__)
        print(self.df.columns)
        self.df.drop('name_catagory', axis=1, inplace=True)
        return

    def __change_names_case(self, case='u'):
        """Changes the case of the names and unique_names"""
        print("[Info]: <from %s> Changing case of names and unique_names columns..." % self.__class__)
        change_case = lambda x: x
        if case.lower() == 'u':
            change_case = lambda x: BasicStringUtils.to_upper(x)
        else:
            change_case = lambda x: BasicStringUtils.to_lower(x)
            
        self.df.loc[:, UniqueNames.names] = self.df[UniqueNames.names].map(change_case) 
        self.df.loc[:, UniqueNames.unames] = self.df[UniqueNames.unames].map(change_case)
        return

    
class UniqueNamesCustomizableError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


class UnmappedCustomersFoundError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


class UnmappedPartnersFoundError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
