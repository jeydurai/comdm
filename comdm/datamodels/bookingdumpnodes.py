# bookingdumpnodes.py
from datamodels.bookingdump import BookingDump
from utils.queryutils import MongoMatch as AggMatch
from utils.queryutils import MongoGroup as AggGroup
from utils.queryutils import MongoProject as AggProj
from collections import namedtuple


class BookingDumpSL3(BookingDump):
    """Sub Class of BookingDump to define Sales_Level_3 specific 
    queries for Booking Dump"""

    def __init__(self, node=None, **kwargs):
        super().__init__(**kwargs)
        self.sensitive = 0        
        self.node_qry = AggMatch(node, self._get_node_fields()).get_node_obj()
        self.match_qry = { **self.match_qry, **self.period_qry, **self.node_qry }
        self.__set_fin_query()

    def __str__(self):
        """Overriding __str__ method on class"""
        if not self.period_qry:
            return self.__print_self_all()
        return self.__print_self_specific()

    def _generate_fin_grp_proj(self):
        """Generates Group & Project objects for Finance metrics aggregation.
        In the configuration, values are actual columns/fields in MongoDB collection
        """
        uni_fld_config = { 'sl4': 'sales_level_4' }
        val_fld_config = { 'booking': 'booking_net' }
        if self.sensitive < 2:
            val_fld_config['base_list'] = 'tms_sales_allocated_bookings_base_list'
        if self.sensitive < 1:
            val_fld_config['std_cost'] = 'standard_cost'
        grp = AggGroup(uni_fld_config, val_fld_config)
        proj = AggProj(grp)
        return grp.sum(), proj.obj()

    def __set_fin_query(self):
        """Makes the final MongoDB aggregate query"""
        self.fin_agg_qry = [{'$match': self.match_qry}]
        self.fin_grp_qry, self.fin_proj_qry = self._generate_fin_grp_proj()
        self.fin_agg_qry.append({ '$group': self.fin_grp_qry })
        self.fin_agg_qry.append({ '$project': self.fin_proj_qry })
        return
    
    def get_fin_data(self):
        """Runs the MongoDB Query for Financial Metrics and returns as Pandas DataFrame"""
        pass
    
    def _get_node_fields(self):
        """Creates and returns Field namedtuple object with nodes' MongoDB 
        field names"""
        Field = namedtuple('Field', ['sl3', 'sl4', 'sl5', 'sl6', 'sa'])
        return Field('sales_level_3', 'sales_level_4', 'sales_level_5',
                     'sales_level_6', 'tbm')

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
        Node Object:
        %s
        Match Object:
        %s
        Group Object:
        %s
        Project Object:
        %s
        Aggregate Query Object:
        %s
        =========================================================================
        """ % (self.__class__.__name__, self.latest.year, self.latest.quarter,
               self.latest.month, self.latest.week, self.period_qry, self.node_qry,
               self.match_qry, self.fin_grp_qry, self.fin_proj_qry, self.fin_agg_qry)

    def __print_self_specific(self):
        """Prints the 'self' object with specific attributes"""
        return """
        =========================================================================
        %s has =>
        Query Objects
        =============
        Period Object:
        %s
        Node Object:
        %s
        Match Object:
        %s
        Group Object:
        %s
        Project Object:
        %s
        Aggregate Query Object:
        %s
        =========================================================================
        """ % (self.__class__.__name__, self.period_qry, self.node_qry, self.match_qry,
               self.fin_grp_qry, self.fin_proj_qry, self.fin_agg_qry)
