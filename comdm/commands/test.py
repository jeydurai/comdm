from ..datamodels.uniquenames import UniqueNames


class TestComDM:


    def __init__(self):
        self.obj = UniqueNames()

    def run(self):
        """Runs the test"""
        self.obj.read_and_set()
        print("Data contains %d row(s) %d column(s)" % self.obj.shape)
        print(self.obj.df.columns)
        return
