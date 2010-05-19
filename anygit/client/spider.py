import logging
import os
import pycurl
import StringIO
import subprocess
import time
import traceback
import urllib2
import yaml

from anygit import models

logger = logging.getLogger(__name__)
users = set()
pending_users = set()
servers = [None, 'BEES-KNEES.MIT.EDU', 'CATS-WHISKERS.MIT.EDU', 'PANCAKE-BUNNY.MIT.EDU',
           'REAL-MCCOY.MIT.EDU', 'BUSY-BEAVER.MIT.EDU']
i = 0

def get_next_server():
    global i
    server = servers[i]
    i = (i + 1) % len(servers)
    return server

def run(args):
    stdout, _ = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return stdout

def yaml_curl(url):
    t = 10
    result = None
    while not result:
        server = get_next_server()
        if server:
            cmd = ['ssh', server, 'curl', url]
        else:
            cmd = ['curl', url]
        value = run(cmd)
        try:
            result = yaml.load(value)
        except Exception, e:
            logger.error('Tripped over content:\n%s\n%s' % (value, traceback.format_exc()))
            result = {'error' : '(Manual)'}

        if result.get('error') == [{'error': 'too many requests'}]:
            logger.error('Rate limited!  (%s)  Sleeping for %d seconds.' % (result, t))
            time.sleep(t)
            t *= 2
            result = None
    if 'error' in result:
        logger.warning('Experienced an error: %s' % result)
        result = None
    return result

def get_repos(user):
    repos = []
    result = yaml_curl('http://github.com/api/v2/yaml/repos/show/%s' % urllib2.quote(user))
    if result:
        repos.extend(result['repositories'])
    result = yaml_curl('http://github.com/api/v2/yaml/repos/watched/%s' % urllib2.quote(user))
    if result:
        repos.extend(result['repositories'])
    return repos

def get_collaborators(user, repo):
    users = set()
    result = yaml_curl('http://github.com/api/v2/yaml/repos/show/%s/%s/collaborators'
                       % (urllib2.quote(user), urllib2.quote(repo)))
    print user, repo, result
    if result:
        users.update(result['collaborators'])

    result = yaml_curl('http://github.com/api/v2/yaml/repos/show/%s/%s/contributors'
                       % (urllib2.quote(user), urllib2.quote(repo)))

    print user, repo, result
    if result:
        users.update([c[0] for c in result['contributors']])
    return users

def dump_state(name):
    tmp = '%s~' % name
    f = open(tmp, 'w')
    f.write(yaml.dump({'users' : list(users), 'pending_users' : list(pending_users)}))
    os.rename(tmp, name)

def load_state(name):
    global users, pending_users
    try:
        loaded = yaml.load(open(name))
        users = set(loaded['users'])
        pending_users = set(loaded['pending_users'])
    except IOError:
        logger.warning('Could not load state, continuing.')

def record_user(new_user):
    if new_user not in users and new_user not in pending_users:
        logger.info('Adding new user %s' % new_user)
        pending_users.add(new_user)

def spider(user):
    state_file ='state.yml'
    load_state(state_file)
    record_user(user)

    while True:
        try:
            user = pending_users.pop()
        except KeyError:
            break

        users.add(user)
        repos = get_repos(user)
        logger.info('Beginning spider for %s with %d pending users (%s).  Found %d repos' %
                    (user, len(pending_users), pending_users, len(repos)))
        for repo in repos:
            r = models.Repository.get_or_create(url=repo[':url'])
            r.approved = True
            r.save()
            for new_user in get_collaborators(user, repo[':name']):
                # Don't repeat people
                record_user(new_user)

        dump_state(state_file)
    logger.info('All done.')
