import warnings

import urllib3
from numba import NumbaDeprecationWarning

warnings.simplefilter(action="ignore", category=NumbaDeprecationWarning)
urllib3.disable_warnings()

import logging

logging.getLogger("requests").setLevel(logging.NOTSET)
logging.getLogger("urllib3").setLevel(logging.NOTSET)
