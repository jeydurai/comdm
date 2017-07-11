import os


class Restore:
    """Class to hold all database restoring methods"""

    def __init__(self, source_dir):
        print('Initializing...')
        self.source = source_dir

    def run(self):
        """Executes the restore process"""
        print('Restoration starting now...')
        cmd = 'mongorestore --drop %s' % self.source
        os.system(cmd)
