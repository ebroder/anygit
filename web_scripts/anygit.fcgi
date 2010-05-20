#!/usr/bin/python
import django.utils.autoreload
import os
import sys
import threading
import time

sys.path.append('/mit/gdb/Scripts/anygit')

from flup.server.fcgi import WSGIServer
from paste.deploy import loadapp

DIR = os.path.abspath(os.path.dirname(__file__))
conf = os.path.join(DIR, '../conf/anygit.ini')
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
