from ..datamodels.bookingdump import BookingDump
from ..datamodels.restatement import InternalRestatement
from ..datamodels.techspec import TechSpec
from ..datamodels.uniquenames import UniqueNames
from ..datamodels.amnodes import AMNodes
from ..helpers.loophelper import Progress
from pprint import pprint
import timeit
import string


class CleanBookingDump(BookingDump):
    "Contains functionalities of clean booking dump creation"""

    tech_err_file = 'tech_map_error_report.xlsx'
    
    def __init__(self, ops):
        self.cloud = ops['cloud']
        self.xlude_cus = ops['excludecus']
        self.outfile = ops['out']
        self.writeas = ops['writeas']
        self.writeable = ops['writeable']
        self.verbose = ops['verbose']
        self.should_map_uname = ops['unamemap']
        self.should_map_am = ops['ammap']
        self.onlycom = True
        self.apptoapp = False
        if not ops['comparefull']: self.apptoapp = True
        super().__init__(ops['sl3'], ops['sl4'], ops['sl5'], ops['sl6'], ops['salesagent'],
                         ops['sensitivity'], ops['servindi'], self.onlycom, init=True,
                         historic_years=ops['years'], app_to_app=self.apptoapp)
        self.shape_restate = None
        self.shape_techmap = None
        self.df_prod = None
        self.df_serv = None
        self.df_noncloud = None

    def process(self):
        """Executes the whole process"""
        tic = timeit.default_timer()
        self.__read_data()
        self.__restate_data()
        self.__map_techs()
        self.amend_sl4()
        self.remove_customers([{ 'name': 'jasper', 'lookup': 'contains'  }])
        if self.should_map_uname: self.__map_unique_names()
        if self.should_map_am: self.__map_am_names()
        self.df_prod = self.subset_data(what='products')
        self.df_serv = self.subset_data(what='services')
        if self.verbose: self.verbose_display()
        if self.writeable:
            self.__write_data()
        else:
            print("[Warning]: Writing on to disc has not been executed as requested!")
        Progress.elapsed(tic, "Command 'cleanbd' execution")
        return

    def __write_data(self):
        """Checks and decides as to what to write as what"""
        file_suffix = None
        df = None
        if self.writeable and self.writeable.lower() == 'all':
            file_suffix = 'Products + Services'
            df = self.df
        elif self.writeable and self.writeable.lower() == 'products':
            file_suffix = 'Products'
            df = self.df_prod
        elif self.writeable and self.writeable.lower() == 'services':
            file_suffix = 'Services'
            df = self.df_serv
        elif self.writeable and self.writeable.lower() == 'noncloud':
            file_suffix = 'Products_Noncloud'
            df = self.df_noncloud

        if file_suffix is None:
            self.__write_as_xlsx_OR_csv(df, 'Products+Services', self.writeas)
        else:
            self.__write_as_xlsx_OR_csv(df, file_suffix, self.writeas)
            print("[Warning]: Writeable format string is missing!!!")
        return
    
    def __write_as_xlsx_OR_csv(self, df, file_suffix, writeas='csv'):
        """Writes Non cloud product data either as CSV or XLSX"""
        tic = timeit.default_timer()
        filename = 'COM_Booking_YTD_' + file_suffix
        print("[Info]: Writing %s as %s..." % (filename, self.writeas))
        self.df = self.__stringify_unicode_columns(self.df)
        if self.writeas.lower() == 'xlsx':
            filename += '.xlsx'
            self.df.to_excel(filename, index=False)
        elif self.writeas.lower() == 'csv':
            filename += '.csv'
            self.df.to_csv(filename, index=False)
        else:
            filename += '.pkl'
            self.df.to_pickle(filename)
        Progress.elapsed(tic, "Writing file on Disc")
        return
    
    def __stringify_unicode_columns(self, df):
        """Stringifies the unicoded columns so as to enable the write as excel easier"""
        df.loc[:, BookingDump.cus_name] = df[BookingDump.cus_name].astype('str')
        if self.should_map_uname:
            df.loc[:, UniqueNames.ucus_name] = df[UniqueNames.ucus_name].astype('str')
        df.loc[:, BookingDump.par_name] = df[BookingDump.par_name].astype('str')
        if self.should_map_uname:
            df.loc[:, UniqueNames.upar_name] = df[UniqueNames.upar_name].astype('str')
        return df
    
    def __read_data(self):
        """Reads the Booking Dump data"""
        tic = timeit.default_timer()
        self.read_data_by_week()
        self.rename_columns()
        self.change_cus_par_case() # by default, converts to uppercase
        self.add_new_columns()
        Progress.elapsed(tic, "Reading Booking Data")
        return

    def __restate_data(self):
        """Executes restatement and validates the integrity of the same"""
        tic = timeit.default_timer()
        restate = InternalRestatement()
        restate.read_and_set()
        restate.change_cus_case()
        self.df = restate.restate(self.df)
        self.shape_restate = self.df.shape
        self.__validate_restatement()
        Progress.elapsed(tic, "Restating Data")
        return

    def __map_techs(self):
        """Maps the technologies and architectures"""
        tic = timeit.default_timer()
        techs = TechSpec()
        techs.read_and_set()
        self.df = techs.map_techs(self.df)
        self.shape_techmap = self.df.shape
        self.__validate_techmap(techs.err_df)
        Progress.elapsed(tic, "Mapping Techs")
        return

    def __map_unique_names(self):
        """Maps the unique names for customer and partner names"""
        tic = timeit.default_timer()
        unames = UniqueNames()
        unames.read_and_set()
        unames.clean_up()
        self.df = unames.map_unique_customers(self.df)
        self.__validate_unique_name_mapping(self.df.shape, 'Customers')
        self.df = unames.map_unique_partners(self.df)
        self.__validate_unique_name_mapping(self.df.shape, 'Partners')
        Progress.elapsed(tic, "Mapping Unique Names")
        return

    def __map_am_names(self):
        """Maps the Account Managers"""
        tic = timeit.default_timer()
        am = AMNodes()
        am.read_and_set()
        am.clean_up()
        self.df_noncloud = self.subset_data(what='non-cloud')
        self.shape_noncloud = self.df_noncloud.shape
        self.__display_noncloud_count()
        self.df_noncloud = am.map_account_managers(self.df_noncloud)
        self.__validate_am_mapping(self.df_noncloud.shape)
        Progress.elapsed(tic, "Mapping Account Managers")
        return

    def __display_noncloud_count(self):
        print("With cloud data had %d row(s) %d col(s)" % self.shape_removecus)
        print("Non cloud mapping data has %d row(s) %d col(s)" % self.shape_noncloud)
        return

    def __validate_am_mapping(self, shape):
        """Validates Account Manager mapping"""
        self.__display_account_manager_mapping(shape)
        if self.shape_noncloud[0] == shape[0]:
            print("[Pass]: Account Manager mapping went well, Congratulations!")
        else:
            raise AMMappingOverflowError("[Failed]: Account Manager mapping")
        return
            
    def __validate_unique_name_mapping(self, shape, txt):
        """Validates whether the unique name  mapping went well"""
        self.__display_unique_name_mapping(shape, txt)
        if self.shape_removecus[0] == shape[0]:
            print("[Pass]: Unique %s Names mapping went well!" % txt)
        else:
            raise UniqueNameMappingOverflowError("[Failed]: %s Name mapping!" % txt)
        return
            
    def __validate_restatement(self):
        """Validates the restatement process and raises Error if it did not go well"""
        self.__display_restatement()
        if self.shape_init[0] == self.shape_restate[0]:
            print("[Pass]: Restatement went well!")
        else:
            raise RestatementError("[Failed]: Restatement caused a problem")
        return

    def __validate_techmap(self, err_df):
        """Validates the technology mapping and raises Error if it did not go well"""
        self.__display_techmap()
        if self.shape_init[0] == self.shape_techmap[0]:
            print("[Pass]: Technology Mapping went well!")
        else:
            err_df.to_excel(CleanBookingDump.tech_err_file, index=False)
            msg = "[Info]: Please refer %s file for more detail!" % CleanBookingDump.tech_err_file
            raise TechMappingError(msg)
        return

    def verbose_display(self):
        """Shows all preset metrics for the process"""
        self.__display_beginners()
        self.__display_data_readings()
        self.__display_restatement()
        return

    def __display_restatement(self):
        """Displays the details of restatement"""
        print("Before restatement data had %d row(s) %d column(s)" % self.shape_init)
        print("After restatement data has %d row(s) %d column(s)" % self.shape_restate)
        return

    def __display_unique_name_mapping(self, shp, txt):
        """Displays the details of Uniquename Mapping"""
        before_info = (txt, self.shape_removecus[0], self.shape_removecus[1])
        print("Before %s mapping data had %d row(s) %d col(s)" % before_info)
        print("After %s mapping data has %d row(s) %d col(s)" % (txt, shp[0], shp[1]))
        return

    def __display_account_manager_mapping(self, shp):
        """Displays the details of Account Managers mapping"""
        before_info = (self.shape_removecus[0], self.shape_removecus[1])
        print("Before AM mapping data had %d row(s) %d col(s)" % before_info)
        print("After AM mapping data has %d row(s) %d col(s)" % (shp[0], shp[1]))
        return

    def __display_techmap(self):
        """Displays the details of Technology Mapping"""
        print("Before Tech mapping data had %d row(s) %d col(s)" % self.shape_init)
        print("After Tech mapping data has %d row(s) %d col(s)" % self.shape_techmap)
        return

    def __display_data_readings(self):
        """Displays the details of the data read"""
        print("Current Year data has %d row(s) %d column(s)" % self.df_cur.shape)
        print("Previous Year data has %d row(s) %d column(s)" % self.df_prev.shape)
        print("All data has %d row(s) %d column(s)" % self.df.shape)
        return

    def __display_beginners(self):
        """Displays the initial set up data datails"""
        print('Latest Financial Year: %s' % self.lyear)
        print('Latest Financial Quarter: %s' % self.lquarter)
        pprint(self.lweeks)
        print('Previous years')
        print(self.prev_years)
        print('Previous years weeks')
        pprint(self.prev_years_weeks)
        return


        
class RestatementError(Exception):
    def __init__(self, msg):
        Exception.__init__(self, msg)


class TechMappingError(Exception):
    def __init__(self, msg):
        print("[Failed]: Technology mapping caused a problem")
        Exception.__init__(self, msg)


class UniqueNameMappingOverflowError(Exception):
    def __init__(self, msg):
        print("[Failed]: UniqueName mapping caused a problem")
        Exception.__init__(self, msg)


class AMMappingOverflowError(Exception):
    def __init__(self, msg):
        print("[Failed]: Serious Error=> DataFrame's row count increased ")
        Exception.__init__(self, msg)


class NullAMMappingFoundError(Exception):
    pass


