'''Verify the Python Interpreter version is in range required by this package'''

min_python = (3, 7)
max_python = (3, 8)

import sys

if (sys.version_info[:len(min_python)] < min_python) or (sys.version_info[:len(max_python)] > max_python):

    text_have = '.'.join("%s" % n for n in sys.version_info)
    text_min  = '.'.join("%d" % n for n in min_python) if min_python else None
    text_max  = '.'.join("%d" % n for n in max_python) if max_python else None

    text_cmd_min = 'python' + text_min + '  ' + "  ".join("'%s'" % a for a in sys.argv) if min_python              else None
    text_cmd_max = 'python' + text_max + '  ' + "  ".join("'%s'" % a for a in sys.argv) if max_python > min_python else None

    sys.stderr.write("Using Python version: " + text_have + "\n")

    if min_python:  sys.stderr.write(" - Min required: " + text_min + "\n")
    if max_python:  sys.stderr.write(" - Max allowed : " + text_max + "\n")
    sys.stderr.write("\n")

    sys.stderr.write("Consider running as:\n\n")
    if text_cmd_min:  sys.stderr.write(text_cmd_min + "\n")
    if text_cmd_max:  sys.stderr.write(text_cmd_max + "\n")
    sys.stderr.write("\n")

    sys.exit(9)

from retentioneering.core.config import config, init_config
from retentioneering import datasets
from .version import __version__
