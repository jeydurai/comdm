from ..helpers.reader import ExcelReader
from ..helpers.writer import MongoWriter
import os

class Upload:
    """Databases upload functionalities"""

    def __init__(self, source_path, source_name, desti, sheetname):
        self.source = os.path.join(source_path,
                        self._make_valid_xl_file(source_name))
        self.desti = desti
        self.xl_reader = ExcelReader(self.source, sheetname)
        self.m_writer = MongoWriter(self.desti)

    def _make_valid_xl_file(self, source):
        """Checks if the source' extension is 'xlsx', otherwise
        postpends 'xlsx' word to it"""
        if not source.endswith('.xlsx'):
            return source + '.xlsx'
        return source

    def validate(self):
        """Validates whether both source xl file and mongodb 
        collection do exist"""
        if not self.xl_reader.file_exists():
            # Checks whether file in the given path exists
            err = "Error: %s file does NOT exist!" % self.filepath
            return False, err
        if not self.m_writer.collection_exists():
            # Checks whether mongodb collection given exists
            err = "Error: %s collection does NOT exist!" % self.desti
            return False, err
        msg = "Info: Both %s file and %s collection exist" % (self.source, 
                self.desti)
        return True, msg

    def read_xl_upload_mongo(self):
        """Does execute the read the data from excel and upload 
        into MongoDB"""
        self.xl_reader.read()
        self.xl_reader.set_standard_cols(self.desti)
        self.m_writer.set_data(self.xl_reader.df)
        self.m_writer.cleanup_existing_data()
        self.m_writer.write(self.xl_reader.rows)
