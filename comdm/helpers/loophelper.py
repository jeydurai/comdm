import timeit
import sys

class Progress:
    """Helps to show the looping progress"""

    def __init__(self, rows, tic):
        self.rows = rows
        self.tic = tic
        self.timer = Timer(self.tic)

    def show(self, idx):
        """Displays the progress attribites"""
        self.timer.set_attr()
        self.idx = idx+1
        self.completed = (float(self.idx)/float(self.rows))*100
        sys.stdout.write(
            "Processed %.2f%% - (%d)/(%d) doc(s) %d hr(s) %d min(s) %d sec(s)...\r" \
            % (self.completed, self.idx, self.rows, self.timer.hrs, self.timer.mins, self.timer.secs)
        )
        sys.stdout.flush()
        return


class Timer:
    """Time Object to have convenient methods to handle time attributes"""

    def __init__(self, tic):
        self.tic = tic

    def set_attr(self):
        """Sets the time elapsed in time units"""
        self.toc = timeit.default_timer()
        self.elapsed = self.toc - self.tic
        self.hrs = int(float(self.elapsed)/float(60*60))
        self.mins = int(float(self.elapsed)/float(60))
        self.secs = self.elapsed % (60*60)
        return

