# queryutils.py

class MongoMatch:
    """Contains functionalities of converting into Mongo match query object"""

    def __init__(self, param, field):
        self.param = param
        self.field = field

    def get_period_obj(self):
        """Validates and Returns the query object"""
        if self.param.year:
            return { self.field.year: { '$regex': '^'+self.param.year} }
        if self.param.quarter:
            return { self.field.quarter: self.param.quarter }
        if self.param.month:
            return { self.field.month: self.param.month }
        if self.param.week:
            return { self.field.week: self.param.week }
        return {}

    def get_node_obj(self):
        """Makes and returns the Node query object"""
        if self.param.sl3:
            return { self.field.sl3: self.param.sl3 }
        if self.param.sl4:
            return { self.field.sl4: self.param.sl4 }
        if self.param.sl5:
            return { self.field.sl5: self.param.sl5 }
        if self.param.sl6:
            return { self.field.sl6: self.param.sl6 }
        if self.param.sa:
            return { self.field.sa: self.param.sa }
        return {}


class MongoGroup:
    """Class to supply the MongoDB Group object for aggregate query"""

    def __init__(self, unique_fields=None, value_fields=None):
        self.uni_fields = unique_fields
        self.val_fields = value_fields

    def sum(self):
        """Makes Group Query object with sum value fields included"""
        return self.__get_grp_by_obj('sum')

    def avg(self):
        """Makes Group Query object with average value fields included"""
        return self.__get_grp_by_obj('avg')

    def max(self):
        """Makes Group Query object with max value fields included"""
        return self.__get_grp_by_obj('max')

    def min(self):
        """Makes Group Query object with min value fields included"""
        return self.__get_grp_by_obj('min')

    def unique(self):
        """Makes Group Query object with unique fields included"""
        return self.__get_grp_by_obj()

    def __get_grp_by_obj(self, opn=None):
        """Makes the final Group object by constructing fields and values"""
        if opn is None:
            return self.fields()
        flds = self.fields()
        vals = self.values(opn)
        return { **flds, **vals }

    def values(self, opn):
        """Makes Group Query object"""
        obj = {}
        for k, v in self.val_fields.items():
            obj[k] = { '$'+opn: '$'+v }
        return obj

    def fields(self):
        """Makes Group Query object"""
        obj = {}
        for k, v in self.uni_fields.items():
            obj[k] = '$' + v
        return { '_id': obj }


class MongoProject:
    """Class to make project query objects for MongoDB aggregate query"""

    def __init__(self, grp_obj):
        self.grp_obj = grp_obj

    def obj(self):
        """Makes and returns MongoDB aggregate project object"""
        hide_fields = { '_id': 0 }
        flds = {}
        vals = {}
        for k in self.grp_obj.uni_fields.keys():
            flds[k] = '$_id.'+ k
        for k in self.grp_obj.val_fields.keys():
            vals[k] = '$_id.' + k
        return { **hide_fields, **flds, **vals }
        
