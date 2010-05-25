#!/usr/bin/python
import django.utils.autoreload
import logging.config
import os
import sys
import threading
import time

DIR = os.path.abspath(os.path.dirname(__file__))
conf = os.path.join(DIR, '../conf/anygit.ini')

sys.path.append('/mit/anygit/Scripts/anygit')

log = open('/mit/anygit/Scripts/anygit/web.log', 'a', 0)
os.dup2(log.fileno(), 1)
os.dup2(log.fileno(), 2)
log.close()

print '---- REQUEST FROM %s' % os.environ

logging.config.fileConfig(conf)

from flup.server.fcgi import WSGIServer
from paste.deploy import loadapp

application = loadapp('config:%s' % conf, relative_to='/')

def reloader_thread():
  while True:
    if django.utils.autoreload.code_changed():
      os._exit(3)
    time.sleep(1)
t = threading.Thread(target=reloader_thread)
t.daemon = True
t.start()

if __name__ == '__main__':
   WSGIServer(application).run()
