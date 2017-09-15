import os
import sys
import argparse

PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

def parseCommandLine():
    parser = argparse.ArgumentParser("KPI Calculation Options")
    parser.add_argument('-o', action="store", dest="optionFile", required=False,
                        help="option file", default = None)
    parser.add_argument('-c', action="store_true", dest="continueRun", required=False,
                        help="it should continue running with interval setting or just one time.",
                        default = False)

    args = parser.parse_args()

    if not (args.optionFile):
        args.optionFile = os.path.join(SCRIPT_DIR,"../config/config.json")

    if not os.path.exists(args.optionFile) or not os.path.isfile(args.optionFile):
        print("Option file doesn't exist or is not a file:{}".format(args.optionFile))
        exit(1)

    return args