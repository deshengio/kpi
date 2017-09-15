from pprint import pprint
import os,sys
import pg8000 as dbapi
import argparse
#from multiprocessing import Process
import threading
from datetime import datetime
import time

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from . import configurator
from . import db
from . import normalizer
from . import controller
from . import helpers


if __name__ == '__main__':
    args = helpers.parseCommandLine()
    kpiconfig = configurator.KpiConfiguration(args.optionFile)
    if args.continueRun:
        controller.loopProcess(kpiconfig)
    else:
        controller.startProcess(kpiconfig)
