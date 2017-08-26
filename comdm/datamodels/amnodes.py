from ..utils.querymaker import Assembler
from ..utils.querymaker import MongoTools
from ..utils.stringutilities import BasicStringUtils
from ..helpers.reader import MongoReader
from ..helpers.loophelper import Progress
import pandas as pd
from pprint import pprint
import timeit


class AMNodes(MongoReader):
    """Holds the data of AM with nodes"""

    coll_name = 'commercial_am_unique_nodes'
    yr = 'fiscal_year_id'
    cname = 'customer_name'
    sl4 = 'sales_level_4_m'
    sl5 = 'sales_level_5_m'
    sl6 = 'sales_level_6_m'
    sales_agent_m = 'sales_agent_m'
    sales_agent_c = 'sales_agent_c'
    sales_agent_c_m = 'sales_agent_c_m'
    is_vsam = 'is_vsam'
    on_list = [yr, cname, sl4, sl5, sl6, sales_agent_m]
    cols_writable = [yr, cname, sl4, sl5, sl6, sales_agent_m, sales_agent_c, sales_agent_c_m,
                     is_vsam]


    def __init__(self):
        self.df = None
        self.qry = {}
        self.shape = None
        super().__init__(AMNodes.coll_name)

    def read_and_set(self):
        """Queries in MongoDB and stores the data"""
        print("[Info]: <from %s> Reading Data..." % self.__class__)
        self.df = self.run_find(self.qry, {})
        self.shape = self.df.shape
        return

    def clean_up(self):
        """Executes the set of private methods to clean up the data"""
        print("[Info]: <from %s> Cleaning up the Data..." % self.__class__)
        self.__lower_case_the_columns()
        self.__change_names_case()
        self.__remove_duplicates()
        return

    def __remove_duplicates(self):
        """Removes the duplicate rows"""
        print("[Info]: <from %s> Removing duplicate rows..." % self.__class__)
        self.df.drop_duplicates(AMNodes.on_list, keep='first', inplace=True)
        self.shape_dedup = self.df.shape
        removed = self.shape[0] - self.shape_dedup[0]
        print("[Info]: <from %s> Deduplication has removed %d row(s)..." % (self.__class__, removed))
        return

    def __lower_case_the_columns(self):
        """Changes the column names case to lower case"""
        print("[Info]: <from %s> Changing columns case as lower case..." % self.__class__)
        self.df.rename(columns=str.lower, inplace=True)
        return

    def __change_names_case(self, case='u'):
        """Changes the case of the customer names"""
        print("[Info]: <from %s> Changing case of customer_name columns..." % self.__class__)
        change_case = lambda x: x
        if case.lower() == 'u':
            change_case = lambda x: BasicStringUtils.to_upper(x)
        else:
            change_case = lambda x: BasicStringUtils.to_lower(x)
            
        self.df.loc[:, AMNodes.cname] = self.df[AMNodes.cname].map(change_case) 
        return

    def map_account_managers(self, bd):
        """Maps the Account Managers to the booking data based on nodes"""
        bd.loc[:, AMNodes.yr] = bd[AMNodes.yr].astype('int64')
        bd.loc[:, AMNodes.sl4] = bd[AMNodes.sl4].astype('object')
        bd = pd.merge(bd, self.df, on=AMNodes.on_list, how='left')
        if bd.loc[pd.isnull(bd[AMNodes.sales_agent_c])].shape[0] != 0:
            fname = 'AM_unmapped_error.xlsx'
            null_check_col = AMNodes.sales_agent_c
            cols_writable = AMNodes.cols_writable
            bd.loc[pd.isnull(bd[null_check_col])][cols_writable].to_excel(fname, index=False)
            raise UnmappedAMFoundError("[Error]: Please check %s file for more info!" % fname)
        return bd


class UnmappedAMFoundError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)
