from ..helpers.reader import MongoReader
from ..utils.stringutilities import BasicStringUtils
import pandas as pd
from pprint import pprint


class TechSpec(MongoReader):
    """Provides all reading functionalities in 'tech_spec1' collection"""

    coll_name = 'tech_spec1'
    tech_name = 'tech_name'
    sub_be = 'internal_sub_business_entity_name'
    arch1 = 'arch1'
    arch2 = 'arch2'

    def __init__(self):
        self.hide_fields = { '_id': 0 }
        self.qry = {}
        self.df = None
        self.err_df = None
        super().__init__(TechSpec.coll_name)

    def read_and_set(self):
        """Queries MongoDB and stores the data"""
        self.df = self.run_find(self.qry, self.hide_fields)
        return

    def map_techs(self, bd):
        """Executes Technology mapping to the data given as 'bd'"""
        print("Mapping Technologies and Architectures on progress...")
        bd = pd.merge(bd, self.df, on=TechSpec.sub_be, how='left')
        if bd.loc[pd.isnull(bd[TechSpec.tech_name])].shape[0] != 0:
            self.err_df = bd.loc[pd.isnull(bd[TechSpec.tech_name])]
        return bd
        
