import timeit
import sys

class Progress:
    """Helps to show the looping progress"""

    @staticmethod
    def elapsed(tic, txt=''):
        """Prints the elpsed time"""
        toc = timeit.default_timer()
        time_diff = toc - tic
        hrs = int(time_diff / (60 * 60))
        mins = int(time_diff / 60)
        secs = time_diff % 60
        print("%s Elapsed %d hr(s) %d min(s) %d sec(s)." % (txt, hrs, mins, secs))




