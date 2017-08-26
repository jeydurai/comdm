from ..utils.querymaker import Assembler
from ..utils.querymaker import MongoTools
from ..utils.stringutilities import BasicStringUtils
from ..helpers.reader import MongoReader
from ..helpers.loophelper import Progress
import pandas as pd
from pprint import pprint
import timeit


class BookingDump(MongoReader):
    """Provides all reading functionalities in 'ent_dump_from_finance' collection"""

    coll_name = 'ent_dump_from_finance'
    fin_yr = 'fiscal_year_id'
    fin_qtr = 'fiscal_quarter_id'
    fin_mth = 'fiscal_period_id'
    fin_wk = 'fiscal_week_id'
    cus_name = 'customer_name'
    par_name = 'partner_name'
    bk_adj_code = 'bookings_adjustments_code'
    sl3 = 'sales_level_3'
    sl4 = 'sales_level_4'
    sl5 = 'sales_level_5'
    sl6 = 'sales_level_6'
    sub_scms = 'sub_scms'
    sales_agent = 'sales_agent'
    sl3_m = 'sales_level_3_m'
    sl4_m = 'sales_level_4_m'
    sl5_m = 'sales_level_5_m'
    sl6_m = 'sales_level_6_m'
    sub_scms_m = 'sub_scms_m'
    sales_agent_m = 'sales_agent_m'
    tbm = 'tbm'
    serv_indi = 'services_indicator'
    prod_serv_map = { 'N': 'products', 'Y': 'services'  }
    col_renameable = { 'tms_sales_allocated_bookings_base_list': 'base_list', 'tbm': 'sales_agent' }

    def __init__(self, sl3, sl4, sl5, sl6, salesagent, sensitivity, servindi, onlycom,
                 init=False, historic_years=0, app_to_app=False):
        self.sl3 = sl3
        self.sl4 = sl4
        self.sl5 = sl5
        self.sl6 = sl6
        self.salesagent = salesagent
        self.servindi = servindi
        self.sensitivity = sensitivity
        self.historic_years = historic_years
        self.app_to_app = app_to_app
        if not isinstance(historic_years, int): self.historic_years = int(historic_years)
        self.onlycom = onlycom
        super().__init__(BookingDump.coll_name)
        self.hide_fields = MongoTools.hide_fields(self.sensitivity)
        self.matched_rows = None
        self.df = None
        self.df_cur = pd.DataFrame()
        self.df_prev = pd.DataFrame()
        self.prev_years = None
        if init: self.set_latest_periods()
        self.shape_init = None
        self.shape_amendsl4 = None
        self.shape_removecus = None

    def subset_data(self, what=None):
        """Subsets the services data from the main DataFrame"""
        mask = None
        if what is None:
            return None
        if what.lower() == 'services':
            mask = (self.df[BookingDump.serv_indi] == 'Y')
        elif what.lower() == 'products':
            mask = (self.df[BookingDump.serv_indi] == 'N')
        elif what.lower() == 'non-cloud':
            mask =((self.df[BookingDump.serv_indi] == 'N') & \
                   (-self.df[BookingDump.bk_adj_code].str.startswith('L')))
        return self.df.loc[mask, :]
        
    def set_latest_periods(self):
        """Queries in MongoDB and sets all the latest periods"""
        print("Setting all the lastest period...")
        self.lquarter = self.latest_quarter()
        self.lyear = self.lquarter[:4]
        self.lweeks = self.latest_weeks()
        self.lweek = str(self.lweeks[-1])
        self.lmonth = self.lweek[:6]
        self.prev_years = self.get_prev_years()
        self.prev_years_weeks = self.get_prev_years_weeks()
        return

    def remove_customers(self, customers=[]):
        """Removes the data of the customers giving in the list"""
        print("[Progress]: Removing customers on progress...")
        tic = timeit.default_timer()
        for cus in customers:
            if cus['lookup'].lower() == 'startswith':
                mask = (-(self.df[BookingDump.cus_name].str.startswith(cus['name'].upper()).fillna(True)))
            mask = (-(self.df[BookingDump.cus_name].str.contains(cus['name'].upper()).fillna(True)))
            self.df = self.df.loc[mask, :]
        self.shape_removecus = self.df.shape
        self.__validate_remove_customers()
        Progress.elapsed(tic, "Removing Customers")
        return
    
    def __validate_remove_customers(self):
        """Validates the whether removing list of customers from the dataframe caused any problem or not"""
        self.__display_remove_customers()
        if self.shape_init[0] > self.shape_removecus[0]:
            print("[Pass]: Customers removal has happened!")
        elif self.shape_init[0] == self.shape_removecus[0]:
            print("[Warning]: No customers removal has happended")
        else:
            raise CustomersRemovalError("This ain't an usual error, please take action!!!!!")
        return

    def __display_remove_customers(self):
        """Displays the details of SL4 Amendment"""
        print("Before removing customers data had %d row(s) %d col(s)" % self.shape_init)
        print("After removing customers data has %d row(s) %d col(s)" % self.shape_removecus)
        return

    def amend_sl4(self):
        """Amends and Re-groups the Sales_Level_4 for specical cases"""
        print("[Progress]: Amending SL4 on progress...")
        tic = timeit.default_timer()
        for amend in self.__get_sl4_amendments():
            self.df.loc[amend['mask'], amend['change_field']] = amend['change_string']
            self.df.loc[amend['mask'], amend['change_field'] + '_m'] = amend['change_string']
        self.shape_amendsl4 = self.df.shape
        self.__validate_sl4_amendment()
        Progress.elapsed(tic, "SL4 amendment")
        return

    def __validate_sl4_amendment(self):
        """Validates the whether SL4 amendment went well or not"""
        self.__display_sl4_amendment()
        if self.shape_init[0] == self.shape_amendsl4[0]:
            print("[Pass]: SL4 Amenment went well!")
        else:
            raise SL4AmendmentError("Some unexpected Error")
        return

    def __display_sl4_amendment(self):
        """Displays the details of SL4 Amendment"""
        print("Before SL4 amendment data had %d row(s) %d col(s)" % self.shape_init)
        print("After SL4 amendment data has %d row(s) %d col(s)" % self.shape_amendsl4)
        return

    def __get_sl4_amendments(self):
        """Returns the list of SL4 amendment credential objects"""
        return [
            {
                'mask': (self.df[BookingDump.sl4] == 'INDIA_COMM_WST'),
                'change_field': BookingDump.sl4,
                'change_string': 'INDIA_COMM_SW_GEO'
            },
            {
                'mask': (self.df[BookingDump.sl4] == 'INDIA_COMM_STH'),
                'change_field': BookingDump.sl4,
                'change_string': 'INDIA_COMM_SW_GEO'
            },
            {
                'mask': (self.df[BookingDump.sl4] == 'INDIA_COMM_NORTH_EAST'),
                'change_field': BookingDump.sl4,
                'change_string': 'INDIA_COMM_NE_GEO'
            },
            {
                'mask': (self.df[BookingDump.sl4] == 'INDIA_COMM_MISC'),
                'change_field': BookingDump.sl4,
                'change_string': 'INDIA_COMM_1-MISCL4'
            },
        ]
    
    def add_new_columns(self):
        """Adds new columns for reflecting internal restatement"""
        print("Adding on new columns...")
        cols = [
            BookingDump.sl4, BookingDump.sl5, BookingDump.sl6, BookingDump.sales_agent,
            BookingDump.sub_scms
        ]
        self.df.loc[:, BookingDump.fin_yr] = self.df[BookingDump.fin_qtr].map(lambda x: x[:4])
        for c in cols:
            self.df.loc[:, c + '_m'] = self.df[c]
        return
    
    def rename_columns(self):
        """Make longer column names into readable column names"""
        print("Renaming columns...")
        self.df.rename(columns=BookingDump.col_renameable, inplace=True)
        return

    def change_cus_par_case(self, case='u'):
        """Changes the case of the customer and partner name"""
        print("Changing partner and customer cases...")
        change_case = lambda x: x
        if case.lower() == 'u':
            change_case = lambda x: BasicStringUtils.to_upper(x)
        else:
            change_case = lambda x: BasicStringUtils.to_lower(x)
            
        self.df.loc[:, BookingDump.cus_name] = self.df[BookingDump.cus_name].map(change_case) 
        self.df.loc[:, BookingDump.par_name] = self.df[BookingDump.par_name].map(change_case)
        return

    def read_data_by_week(self):
        """Queries and updates dataframe by week"""
        for w in self.lweeks:
            qry = Assembler(self.sl3, self.sl4, self.sl5, self.sl6, self.salesagent,
                            self.onlycom, '', '', '', int(w), self.servindi).query
            temp_df = self.run_find(qry, self.hide_fields)
            self.df_cur = self.df_cur.append(temp_df)
        print("Current year's data reading is complete!")
        self.__read_prev_years_data()
        print("Concatenating current and previous years' data")
        self.df = self. __concat_current_and_prev_data()
        self.shape_init = self.df.shape
        return

    def __read_prev_years_data(self):
        """Queries and updates dataframe by week for the previous years"""
        for year, weeks in self.prev_years_weeks.items():
            print("Starting to read %s data..." % year)
            for w in weeks:
                qry = Assembler(self.sl3, self.sl4, self.sl5, self.sl6, self.salesagent,
                                self.onlycom, '', '', '', int(w), self.servindi).query
                temp_df = self.run_find(qry, self.hide_fields)
                self.df_prev = self.df_prev.append(temp_df)
            print("Previous year's data reading is complete!")
        return

    def __concat_current_and_prev_data(self):
        """Concats the current year dataframe and previous years dataframe"""
        return pd.concat([self.df_cur, self.df_prev])
        
    def __mirror_current_year_week(self, cur_yr_wk):
        """Filters All Correct previous years' weeks"""
        def mirror(prev_wk):
            if int(str(prev_wk)[-3:]) <= int(str(cur_yr_wk)[-3:]):
                return prev_wk
        return mirror

    def get_prev_years(self):
        """Returns the previous years credentials"""
        return  [ int(self.lyear) - (y+1) for y in range(self.historic_years) ]

    def get_prev_years_weeks(self):
        """Queries and returns a list of previous years weeks"""
        weeks_of_years = {}
        filter_mask = self.__mirror_current_year_week(self.lweek)
        for y in self.prev_years:
            match = { BookingDump.fin_qtr: { '$regex': '^' + str(y)  } }
            qry = MongoTools.qry_groupby(BookingDump.fin_wk, match=match)
            weeks = list(self.run_agg(qry).loc[:, 'result'])
            if self.app_to_app: weeks = list(filter(filter_mask, weeks))
            weeks_of_years[str(y)] = weeks
        return weeks_of_years

    def latest_weeks(self):
        """Returns the set of all weeks for the latest period"""
        match = { BookingDump.fin_qtr: { '$regex': '^' + self.lyear  } }
        qry = MongoTools.qry_groupby(BookingDump.fin_wk, match=match)
        return list(self.run_agg(qry).loc[:, 'result'])

    def latest_year(self):
        """Queries in MongoDB for Lastest Quarter, from which, year is sliced"""
        return self.latest_quarter()[:4]
    
    def latest_quarter(self):
        """Queries in MongoDB and returns the latest financial quarter"""
        print('Fetching Lastest Quarter...')
        qry = MongoTools.qry_aggregation(BookingDump.fin_qtr, based='max')
        return self.run_agg(qry).loc[0, 'result']
        
    def generate_query(self, year, quarter, month, week, servindi, for_cmd=''):
        """Generates query object specific to 'subset' command"""
        if month:
            month = int(month)
        if week:
            week = int(week)
        if for_cmd == 'subset':
            return Assembler(self.sl3, self.sl4, self.sl5, self.sl6, self.salesagent,
                             self.onlycom, year,  quarter,  month, week, servindi).query
        return {}

    def prepare_data(self, qry):
        """Runs the Database query and store the data as Pandas DataFrame"""
        self.matched_rows = self.rows(qry)
        self.df = self.run_find(qry, self.hide_fields)
        self.make_services_indicator_readable()
        return

    def make_services_indicator_readable(self):
        """Transforms N as 'products' Y as 'services'"""
        self.transform_field(BookingDump.serv_indi, BookingDump.prod_serv_map)
        return
        
    def transform_field(self, col_name, mapper):
        """Modifies certain columns for convenience"""
        self.df[col_name] = self.df[col_name].map(mapper)
        return

    def exclude_customers(self, customers):
        """Based on the switch received from 'cli' further subsetting data"""
        for c in customers:
            mask = (
                -(self.df[BookingDump.cus_name].str.contains(c, case=False).fillna(True)))
            self.__reassign_dataframe(mask)
        return

    def exclude_cloud(self):
        """Removes the cloud data from the main dataframe"""
        mask = (-self.df[BookingDump.bk_adj_code].str.startswith('L'))
        self.__reassign_dataframe(mask)
        return

    def __reassign_dataframe(self, mask):
        """Subsets and reassigns the dataframe based on the mask"""
        self.df = self.df.loc[mask, :]
        return

    def report_node_level(self):
        """Checks and returns which node level is suitable for the display"""
        if self.sl6:
            return BookingDump.tbm
        elif self.sl5:
            return BookingDump.sl6
        elif self.sl4:
            return BookingDump.sl5
        elif self.sl3:
            return BookingDump.sl4
        elif self.onlycom:
            return BookingDump.sl4
        return BookingDump.sl3
        
    def make_run(self):
        """Prints more verbose information in STDOUT"""
        print("\n")
        print("="*120)
        print("Query String")
        print("============")
        pprint(self.query)
        print("Fields' Projection")
        print("============")
        pprint(self.hide_fields)
        print("="*120)
        print("\n")
        return

        
class SL4AmendmentError(Exception):
    def __init__(self, msg):
        print("[Failed]: Serious Error occured while doing amendent in SL4 node")
        Exception.__init__(self, msg)


class CustomersRemovalError(Exception):
    def __init__(self, msg):
        print("[Failed]: Some unexpected error happened in removing customers")
        Exception.__init__(self, msg)


