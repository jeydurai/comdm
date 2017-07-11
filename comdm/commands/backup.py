import os


class Backup:
    """Backup class that holds the backup methods"""

    def __init__(self, out):
        print('Initializing...')
        self.out = out
        self.dump_dir = self.create_dump_dir('dump')

    def create_dump_dir(self, fol_name):
        """Removes the existing dump folde, recreates it and returns
        the full path
        """
        cmd = 'rmdir %s%s%s /s /q' % (self.out, '\\', fol_name)
        cmd2 = 'mkdir %s%s%s' % (self.out, '\\', fol_name)
        print('Removing %s folder...' % fol_name)
        os.system(cmd)
        print('Creating %s folder...' % fol_name)
        os.system(cmd2)
        return self.out + '\\' + fol_name

    def run(self):
        """Executes the backup command"""
        print('Backup starting now...')
        cmd = 'mongodump --out %s' % self.dump_dir
        print('Executing "%s"' % cmd)
        os.system(cmd)
        print('Backup is complete now!')
        
