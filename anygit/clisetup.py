import logging.config
import os
from paste.deploy import loadapp
import sys

DIR = os.path.abspath(os.path.dirname(__file__))
conf = os.path.join(DIR, '../conf/anygit.ini')
logging.config.fileConfig(conf)
application = loadapp('config:%s' % conf, relative_to='/')
app = loadapp('config:%s' % conf,relative_to=os.getcwd())

