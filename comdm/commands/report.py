from datamodels.bookingdumpnodes import BookingDumpSL3
from collections import namedtuple


class Report:
    """Class that contains all the report generating functionalities"""

    def __init__(self, home_dir, is_complex, rsource, rtype, serv_indi,
                 sl3, sl4, sl5, sl6, sa, yr, qtr, mth, wk):
        self.home_dir = home_dir
        self.is_complex = is_complex
        self.rep_cat = self._get_report_catagory()
        self.rsource = self._get_clean_rsource(rsource)
        self.source_collname = self._get_source_collname()
        self.rtype = self._get_report_type(rtype)
        self.serv_indi = self._get_services_indicator(serv_indi)
        self.prod_serv = self._get_prod_serv()
        self._set_business_nodes(sl3, sl4, sl5, sl6, sa)
        self._set_period_nodes(yr, qtr, mth, wk)

    def __str__(self):
        return """
        %s Object =>
        ========================================================
        Home Directory => %s
        Report Catagory => %s
        Report Source Collection => %s
        Report Type => %s
        Service_Indicator => %s
        Sales_Level_3 => %s
        Sales_Level_4 => %s
        Sales_Level_5 => %s
        Sales_Level_6 => %s
        Sales_Agent => %s
        Fin Year => %s
        Fin Quarter => %s
        Fin Month => %s
        Fin Week => %s
        ========================================================
        """ % (self.__class__.__name__, self.home_dir, self.rep_cat,
               self.source_collname, self.rtype, self.prod_serv,
               self.node.sl3, self.node.sl4, self.node.sl5, self.node.sl6, self.node.sa,
               self.period.year, self.period.quarter, self.period.month, self.period.week)

    def _get_clean_rsource(self, rsource):
        """Validates and Returns clean Report Source"""
        if rsource:
            return rsource.lower()
        return rsource

    def _get_source_collname(self):
        """Checks the 'rsource' variable and returns the correct source 
        collection name"""
        if self.rsource == 'bd':
            return 'ent_dump_from_finance'
        return None

    def _get_clean_SL3(self, sl3):
        """Validates sl3 and returns the correct sales_level_3"""
        if sl3 is None:
            return 'INDIA_COMM_1'
        return sl3.upper()

    def _get_clean_business_node(self, node):
        """Validates business node and returns the correct one"""
        if node is None:
            return node
        return node.upper()

    def _get_clean_business_nodes(self, *nodev):
        """Validates all the business nodes and returns the correct ones"""
        arr = []
        for node in nodev:
            if node is not None:
                node = node.upper()
            arr.append(node)
        return tuple(arr)

    def _get_clean_period_nodes(self, *nodev):
        """Validates all the period nodes and returns the correct ones"""
        arr = []
        for node in nodev:
            if node is not None:
                node = node.upper()
            arr.append(node)
        return tuple(arr)

    def _set_business_nodes(self, sl3, sl4, sl5, sl6, sa):
        """Sets all the business nodes"""
        Node = namedtuple('Node', ['sl3', 'sl4', 'sl5', 'sl6', 'sa'])
        sl3 = self._get_clean_SL3(sl3)
        self.node = Node._make(self._get_clean_business_nodes(sl3, sl4, sl5, sl6, sa))
        return

    def _set_period_nodes(self, yr, qtr, mth, wk):
        """Sets all the period nodes"""
        Period = namedtuple('Period', ['year', 'quarter', 'month', 'week'])
        self.period = Period._make(self._get_clean_period_nodes(yr, qtr, mth, wk))
        print(self.period)
        return

    def _get_report_catagory(self):
        """Validates is_complex variable and returns report catagory"""
        if self.is_complex:
            return 'Catagorized as "COMPLEX REPORT"'
        return 'Catagorized as "SIMPLE REPORT"'

    def _get_report_type(self, rtype):
        """Validates and returns the report type"""
        if rtype is None:
            return 'SNAPSHOT'
        return rtype.upper()

    def _get_prod_serv(self):
        """Returns whether 'Product/Service' based on serv_indi"""
        if self.serv_indi == 'N':
            return 'PRODUCTS'
        return 'SERVICES'
    
    def _get_services_indicator(self, si):
        """Validates and returns the services_indicator"""
        if si is None:
            return 'N'
        if si.upper() == 'Y' or si.upper() == 'YES' or si.upper().startswith('S'):
            return 'Y'
        if si.upper() == 'N' or si.upper() == 'NO' or si.upper().startswith('P'):
            return 'N'

    def run(self):
        """Runs the Report"""
        bd = BookingDumpSL3(node=self.node, period=self.period, serv_indi=self.serv_indi)
        print(bd)
        print('Runs the Report!')
        return

        
