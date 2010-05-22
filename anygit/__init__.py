import glob
import os
import sys

DIR = os.path.abspath(os.path.dirname(__file__))

matches = []
for mongo_dir in ['../pkgs/lib/python', '../pkgs/lib64/python']:
    matches += glob.glob(os.path.join(DIR, mongo_dir, 'pymongo-1.6_-py2.6-*'))

# Giant hack.  TODO: cleanup
sys.path[0:0] = matches
del matches
