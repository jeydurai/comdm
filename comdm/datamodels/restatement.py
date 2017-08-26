from ..helpers.reader import MongoReader
from ..utils.stringutilities import BasicStringUtils
import pandas as pd
from pprint import pprint



class InternalRestatement(MongoReader):
    """Provides all reading functionalities in 'ncr_sl_mm_restatement' collection"""

    coll_name = 'ncr_sl_mm_restatement'
    cus_name = 'customer_name'
    sl4_m = 'sales_level_4_m'
    sl5 = 'sales_level_5'
    sl5_m = 'sales_level_5_m'
    sl6_m = 'sales_level_6_m'
    sub_scms_m = 'sub_scms_m'
    sales_agent_m = 'sales_agent_m'
    tbm = 'tbm'

    def __init__(self):
        self.hide_fields = { '_id': 0 }
        self.qry = {}
        self.df = None
        super().__init__(InternalRestatement.coll_name)

    def read_and_set(self):
        """Queries MongoDB and stores the data"""
        self.df = self.run_find(self.qry, self.hide_fields)
        return

    def change_cus_case(self, case='u'):
        """Changes the customer name's case"""
        change_case = lambda x: x
        if case.lower() == 'u':
            change_case = lambda x: BasicStringUtils.to_upper(x)
        else:
            change_case = lambda x: BasicStringUtils.to_lower(x)
            
        self.df.loc[:, InternalRestatement.cus_name] = \
                                                       self.df[InternalRestatement.cus_name].map(change_case) 
        return

    def restate(self, bd):
        """Executes the restatement"""
        print("Internal Restatement on progress...")
        for idx, row in self.df.iterrows():
            cus_name = row[InternalRestatement.cus_name]
            sl5 = row[InternalRestatement.sl5]
            sl4_m = row[InternalRestatement.sl4_m]
            sl5_m = row[InternalRestatement.sl5_m]
            sl6_m= row[InternalRestatement.sl6_m]
            sales_agent = row[InternalRestatement.sales_agent_m]
            sub_scms_m = row[InternalRestatement.sub_scms_m]
            mask = (bd[InternalRestatement.cus_name] == cus_name) & (bd[InternalRestatement.sl5] == sl5)
            recv_col = [
                InternalRestatement.sl4_m, InternalRestatement.sl5_m, InternalRestatement.sl6_m,
                InternalRestatement.sales_agent_m, InternalRestatement.sub_scms_m
            ]
            send_col = [ sl4_m, sl5_m, sl6_m, sales_agent, sub_scms_m ]
            bd.loc[mask, recv_col] = send_col
        return bd
        
