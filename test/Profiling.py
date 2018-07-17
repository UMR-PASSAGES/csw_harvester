import os
import cProfile
import pstats
import sys
sys.path.append(os.path.abspath('../'))
from src import Main


def profiling_harvester():
    os.chdir("../src")
    cProfile.run("Main.start()", "profile_results.prof_file")
    p = pstats.Stats('profile_results.prof_file')
    p.sort_stats('time').print_stats(30)
    os.system('snakeviz profile_results.prof_file')


if __name__ == "__main__":
    profiling_harvester()
