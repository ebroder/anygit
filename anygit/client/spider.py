import logging
import os
import pycurl
import re
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
proxies = []
start_port = 6000
i = 0

### Github methods

def fetch(url, proxy=None):
    time.sleep(1)
    c = pycurl.Curl()
    if proxy:
        c.setopt(pycurl.PROXY, proxy)
        c.setopt(pycurl.PROXYTYPE, pycurl.PROXYTYPE_SOCKS5)
    c.setopt(pycurl.URL, url)
    b = StringIO.StringIO()
    c.setopt(pycurl.WRITEFUNCTION, b.write)
    c.setopt(pycurl.FOLLOWLOCATION, 1)
    c.setopt(pycurl.MAXREDIRS, 5)
    c.perform()
    c.close()
    return b.getvalue()

def setup_proxies():
    port = start_port
    for server in servers:
        if server:
            subprocess.Popen(['ssh', '-D', str(port), server], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            proxies.append((server, 'localhost:%d' % port))
            port += 1
        else:
            proxies.append((server, None))

def get_next_proxy():
    global i
    proxy = proxies[i][1]
    i = (i + 1) % len(proxies)
    return proxy

def run(args):
    stdout, _ = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
    return stdout

def yaml_curl(url):
    t = 10
    result = None
    while not result:
        proxy = get_next_proxy()
        value = fetch(url, proxy=proxy)
        try:
            result = yaml.load(value)
        except Exception, e:
            logger.error('Tripped over content:\n%s\n%s' % (value, traceback.format_exc()))
            result = {'error' : '(Manual)'}

        if result.get('error') == [{'error': 'too many requests'}]:
            logger.error('Rate limited on proxy %s!  (%s)  Sleeping for %d seconds.' % (proxy,
                                                                                        result,
                                                                                        t))
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

def github_spider():
    state_file ='state.yml'
    load_state(state_file)
    if not pending_users:
        user = raw_input('Please enter in a GitHub user to bootstrap from: ').strip()
        pending_users.add(user)
    setup_proxies()

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
            url = 'git://%s.git' % repo[':url'].strip('http://')
            r = models.Repository.get_or_create(url=url)
            r.approved = 'spidered'
            r.save()
            for new_user in get_collaborators(user, repo[':name']):
                # Don't repeat people
                record_user(new_user)

        dump_state(state_file)
    logger.info('All done.')

# git.kernel.org spider

def kernel_spider():
    content = fetch('http://git.kernel.org/')
    repo_extractor = re.compile('git://[^\s<>]+\.git')
    for match in repo_extractor.finditer(content):
        logger.info('Adding repo %s' % match.group(0))
        r = models.Repository.get_or_create(url=match.group(0))
        r.approved = 'spidered'
        r.save()
