import sys

py3k = sys.version_info >= (3, 0)

if py3k:
    basestring = str
else:
    basestring = basestring