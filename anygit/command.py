import logging
import optparse
import os
import sys

from paste.deploy import loadapp
from paste.script import command

from anygit.data import crawl, physical_git, cleanup

logger = logging.getLogger('anygit.command')

class AnygitCommand(command.Command):
    max_args = 3
    min_args = 2

    usage = "CONFIG_FILE (index|dedup) [url]"
    summary = "Operate on the anygit index"
    group_name = "anygit"

    parser = optparse.OptionParser()
    parser.add_option("-a", "--no-add", action="store_false",
                      default=True, dest="add")
    parser.add_option("-f", "--no-fetch", action="store_false",
                      default=True, dest="fetch")
    parser.add_option("-i", "--no-index", action="store_false",
                      default=True, dest="index")

    def command(self):
        if len(self.args) < 2:
            raise command.BadCommand('Must provide a config file and a command')
            return 1

        config = self.args[0]
        if not config.startswith('config:'):
            config = 'config:%s' % config
        app = loadapp(config, relative_to=os.getcwd())

        action = self.args[1]
        if action == 'index':
            if len(self.args) < 3:
                raise command.BadCommand('Must provide a url')
                return 1
            url = self.args[2]
            logger.debug('Starting to index: %s' % url)
            if self.options.add:
                crawl.add_repo(url)
            if self.options.fetch:
                crawl.fetch_repo(url)
            if self.options.index:
                crawl.index_repo(url)
        elif action == 'dedup':
            logger.debug('Deduping')
            cleanup.dedup()
        elif action == 'delete_all':
            logger.debug('Deleting all')
            if raw_input('Are you sure? [yN] ').lower().startswith('y'):
                cleanup.delete_all()
        return 0
