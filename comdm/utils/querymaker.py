class SalesNode:
    """Generates all sales level nodes query object"""

    def __init__(self, sl3, sl4, sl5, sl6, sa, onlycom):
        self.onlycom = onlycom
        self.sl3 = self.__gen_sl3(sl3)
        self.sl4 = self.__gen_sl4(sl4)
        self.sl5 = self.__gen_sl5(sl5)
        self.sl6 = self.__gen_sl6(sl6)
        self.salesagent = self.__gen_sagent(sa)
        self.nodes = self.__nodes()

    def __gen_sl3(self, node):
        """Makes a query object for sales_level_3"""
        if self.onlycom:
            return { 'sales_level_3': 'INDIA_COMM_1' }
        if not node:
            return {}
        return { 'sales_level_3': { '$regex': node, '$options': '-i' } }

    def __gen_sl4(self, node):
        """Makes a query object for sales_level_4"""
        if not node:
            return {}
        return { 'sales_level_4': { '$regex': node, '$options': '-i' } }

    def __gen_sl5(self, node):
        """Makes a query object for sales_level_5"""
        if not node:
            return {}
        return { 'sales_level_5': { '$regex': node, '$options': '-i' } }

    def __gen_sl6(self, node):
        """Makes a query object for sales_level_6"""
        if not node:
            return {}
        return { 'sales_level_6': { '$regex': node, '$options': '-i' } }

    def __gen_sagent(self, node):
        """Makes a query object for tbm"""
        if not node:
            return {}
        return { 'tbm': { '$regex': node, '$options': '-i' } }

    def __nodes(self):
        """Assembles all nodes and makes as one 'Node' query object"""
        return {
            **self.sl3, **self.sl4, **self.sl5, **self.sl6, **self.salesagent}


class PeriodNode:
    """Generates query object for the financial periods"""

    def __init__(self, year, quarter, month, week):
        self.year = self.__gen_year(year)
        self.quarter = self.__gen_quarter(quarter)
        self.month = self.__gen_month(month)
        self.week = self.__gen_week(week)
        self.periods = self.__gen_periods()

    def __gen_year(self, period):
        """Generates query object for year based on 'fiscal_quarter_id'"""
        if not period:
            return {}
        return { 'fiscal_quarter_id': { '$regex': "^" + period } }

    def __gen_quarter(self, period):
        """Generates query object for quarter based on 'fiscal_quarter_id'"""
        if not period:
            return {}
        return { 'fiscal_quarter_id': { '$regex': period, '$options': '-i' } }

    def __gen_month(self, period):
        """Generates query object for month based on 'fiscal_period_id'"""
        if not period:
            return {}
        return { 'fiscal_period_id': period }

    def __gen_week(self, period):
        """Generates query object for week based on 'fiscal_week_id'"""
        if not period:
            return {}
        return { 'fiscal_week_id': period }

    def __gen_periods(self):
        """Assembles all period nodes and returns as one period query obejct"""
        return { **self.year, **self.quarter, **self.month, **self.week }


class MiscNode:
    """Generates Miscellaneous node query objects"""

    def __init__(self, servindi=None):
        self.servindi = self.__gen_serv_indi(servindi)
        self.nodes = self.__gen_mis_nodes()

    def __gen_serv_indi(self, node):
        """Generates Services Indicator node in the query object form"""
        if not node or node is None:
            return {}
        return { 'services_indicator': node }

    def __gen_mis_nodes(self):
        """Generates unified query object assembling all misc nodes"""
        return { **self.servindi }


class MongoTools:
    """Contains class methods to provide others tools in generating query"""


    @classmethod
    def hide_fields(cls, sensitivity):
        """Based on sensitivity/admin rights, it generates hideable fields query"""
        fields = { '_id': 0 }
        if sensitivity == '1':
            sense_fields = { 'standard_cost': 0 }
            fields = { **fields,  **sense_fields }
        elif sensitivity == '2':
            sense_fields = { 
                'standard_cost': 0,
                'tms_sales_allocated_bookings_base_list': 0 
            }
            fields = { **fields,  **sense_fields }
        return fields

    @classmethod
    def qry_aggregation(cls, field, match={}, based='',):
        """Aggregation query generator for returning scalar data"""
        if not based:
            return []
        if not based in ['max', 'min', 'sum', 'avg']:
            return []
        optr = '$' + based
        grp = {  '_id': None, 'result': { optr: '$' + field } }
        proj = { '_id': 0, 'result': '$result' }
        return [ { '$match': match  }, { '$group': grp }, { '$project': proj } ]

    @classmethod
    def qry_groupby(cls, field, match={}, asc=True):
        """Aggregation query generator for returning set of data"""
        from bson.son import SON
        grp = {  '_id': { 'result': '$' + field  } }
        proj = { '_id': 0, 'result': '$_id.result' }
        order = { '$sort': SON([('result', 1)])  }
        if not asc:
            order = { '$sort': SON([('result', -1)])  }
        return [ { '$match': match  }, { '$group': grp }, { '$project': proj }, order ]

    
class Assembler:
    """Combines all nodes and produces only single query object"""

    def __init__(self, sl3, sl4, sl5, sl6, salesagent, onlycom, year, quarter, month,
                 week, servindi):
        self.sales_node = SalesNode(sl3, sl4, sl5, sl6, salesagent, onlycom).nodes
        self.period_node = PeriodNode(year, quarter, month, week).periods
        self.misc_node = MiscNode(servindi).nodes
        self.query = self.__assemble()

    def __assemble(self):
        """Assembles all nodes query objects into one"""
        return { **self.sales_node, **self.period_node, **self.misc_node }


