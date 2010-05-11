"""Setup the anygit application"""
import logging

from anygit.config.environment import load_environment
from anygit import models

log = logging.getLogger(__name__)

def setup_app(command, conf, vars):
    """Place any commands to setup anygit here"""
    load_environment(conf.global_conf, conf.local_conf)

    # Create the tables if they don't already exist
    models.create_schema()
