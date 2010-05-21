import os
import sys

DIR = os.path.dirname(__file__)
# Giant hack.  TODO: cleanup
__LOCAL_MODULES = os.path.abspath(os.path.join(DIR, '../pkgs/lib64/python/pymongo-1.6_-py2.6-linux-x86_64.egg/'))
if __LOCAL_MODULES not in sys.path:
    sys.path.insert(0, __LOCAL_MODULES)
