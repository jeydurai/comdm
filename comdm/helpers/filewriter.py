from pymongo import MongoClient
from helpers.filereader import MongoReader
from helpers.loophelper import Progress
import timeit


class MongoWriter:
    """All MongoDB writing methods"""

    def __init__(self, coll_name, db_name='ccsdm', host='localhost',
                 port=27017):
        self.db_name = db_name
        self.coll_name = coll_name
        self.client = MongoClient(host, port)
        self.db = self.client[self.db_name]

    def collection_exists(self):
        """Check if the given collection exists in the database"""
        return self.coll_name in self.db.collection_names()

    def set_data(self, df):
        """Setter method to set the dataframe"""
        self.df = df
        return

    def write(self, rows):
        """Write into MongoDB"""
        print('Writing data...')
        p = Progress(rows, timeit.default_timer())
        coll = self.db[self.coll_name]
        for idx, row in self.df.iterrows():
            coll.insert(dict(row))
            p.show(idx)
        doc_count = MongoReader(self.coll_name).get_doc_count()
        print("\n%s collection has now %d doc(s)" % (self.coll_name, doc_count))
        return

    def cleanup_existing_data(self):
        """Removes existing documents -based on week_id for 'ent_dump_from_finance' 
        and removes all for others"""
        qry = {}
        delete = False
        if self.coll_name == 'ent_dump_from_finance':
            field = 'fiscal_week_id'
            weeks = self.df[field].unique().tolist()
            or_qry = [{field: week} for week in weeks]
            qry = {'$or': or_qry}
            delete = True
        else:
            delete = self._get_user_confirmation()
        self._clean(delete, qry)

    def _clean(self, delete, qry):
        """Executes MongoDB remove operation on condition"""
        doc_count1 = MongoReader(self.coll_name).get_doc_count()
        print('%d doc(s) existed before deletion' % doc_count1)
        if delete:
            print('Deletion on progress...')
            self.db[self.coll_name].remove(qry, multi=True)
        doc_count2 = MongoReader(self.coll_name).get_doc_count()
        print('%d doc(s) existing untouched' % doc_count2)
        print('%d doc(s) have been deleted' % (doc_count1-doc_count2))
        return

    def _get_user_confirmation(self):
        """Show which collection to be delete and get confirmation from user"""
        print('Collection considered: %s' % self.coll_name)
        print("""Please confirm whether to delete the All Existing 
        documents/specific documents Or else new documents will get 
        appended at the end of the existing documents""")
        delete = input('Should all documents be deleted (y/yes): ')
        if delete.lower() == 'y' or delete.lower() == 'yes':
            return True
        else:
            print('New documents will get appended!')
            return False

        

    
