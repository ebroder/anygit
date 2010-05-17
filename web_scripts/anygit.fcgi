#!/usr/bin/python
import django.utils.autoreload
import os
import sys
import threading
import time

sys.path.append('/mit/gdb/Scripts/anygit')

from flup.server.fcgi import WSGIServer
from paste.deploy import loadapp

application = loadapp('config:anygit.ini', relative_to=os.getcwd())

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
