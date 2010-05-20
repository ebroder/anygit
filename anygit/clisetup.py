import logging.config
import os
from paste.deploy import loadapp
import sys

conf = '/mit/gdb/web_scripts/anygit/anygit.ini'
app = loadapp('config:%s' % conf,relative_to=os.getcwd())
logging.config.fileConfig(conf)
