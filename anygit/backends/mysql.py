import datetime
import logging
import MySQLdb
import random
import re
import subprocess

from pylons import config

from anygit.backends import common
from anygit.data import exceptions

logger = logging.getLogger(__name__)

connection = None
collection_to_class = {}

sha1_re = re.compile('^[a-f0-9]*')

## Exported functions

def create_schema():
    print 'Huhh??'

def flush():
    pass

def init_model(connection):
    """Call me before using any of the tables or classes in the model."""
    db = connection

    for obj in globals().itervalues():
        if type(obj) == type and issubclass(obj, Model) and hasattr(obj, '__tablename__'):
            tablename = getattr(obj, '__tablename__')
            obj._object_store = Domain(db, tablename)
            collection_to_class[obj._object_store] = obj

def setup():
    """
    Sets up the database session
    """
    global connection
    connection = MySQLdb.connect(host=config.get('mysql.host'),
                                 user=config.get('mysql.user'),
                                 passwd=config.get('mysql.password'),
                                 db=config.get('mysql.db'),
                                 ssl={'ca' : config.get('mysql.cert')})
    init_model(connection)

def destroy_session():
    global connection
    connection = None

## Internal functions
class Query(object):
    def __init__(self, domain, query, is_full_query=None):
        self.is_full_query = is_full_query
        self._limit = None
        self._skip = None
        self._order = None
        self.domain = domain
        if isinstance(query, dict):
            items = []
            for k, v in query.iteritems():
                if isinstance(v, list):
                    items.append('`%s` IN (%s)' % (k, ','.join(self.domain._encode(val) for val in v)))
                elif isinstance(v, dict):
                    if '$lt' in v:
                        items.append('`%s` < %s' % (k, self.domain._encode(v['$lt'])))
                    elif '$in' in v:
                        if v['$in']:
                            items.append('`%s` IN (%s)' %
                                         (k, ','.join(self.domain._encode(val) for val in v['$in'])))
                        else:
                            items.append('1 = 0')
                    else:
                        raise ValueError('Unrecognized query modifier %s' % v)
                else:
                    items.append('`%s` = %s' % (k, self.domain._encode(v)))
            query = ' and '.join(items)
        self.query = query
        self._iterator = None

    def _get_iterator(self):
        if not self._iterator:
            self._iterator = iter(self.domain.select(self._get_select()))
        return self._iterator

    def _get_order(self):
        if self._order:
            return ' ORDER BY `%s` %s' % self._order
        else:
            return ''

    def _get_select(self):
        # TODO: select a subset of attributes
        if self.is_full_query:
            return self.query

        if self.query:
            full_query = 'select * from `%s` where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select * from `%s`' % self.domain.name
        return full_query + self._get_order() + self._get_limit()

    def _get_count(self):
        if self.query:
            full_query = 'select count(*) as count from `%s` where %s' % (self.domain.name, self.query)
        else:
            full_query = 'select count(*) from `%s`' % self.domain.name
        return full_query

    def _get_limit(self):
        clause = []
        if self._limit is not None:
            clause.append('LIMIT %d' % self._limit)
        if self._skip is not None:
            clause.append('OFFSET %d' % self._skip)
        if clause:
            return ' %s' % ' '.join(clause)
        else:
            return ''

    def __iter__(self):
        return iter(self.transform_outgoing(i) for i in self._get_iterator())

    def count(self):
        return int(self.domain.select(self._get_count()).next()['count'])

    def next(self):
        return self.transform_outgoing(self._get_iterator().next())

    def limit(self, limit):
        self._limit = limit
        return self

    def skip(self, skip):
        self._skip = skip
        return self

    def order(self, column, type):
        """Order the results.  type should be ASC or DESC"""
        self._order = (column, type)
        return self

    def transform_outgoing(self, son):
        """Transform an object retrieved from the database"""
        if 'type' in son:
            klass = classify(son['type'])
            return klass.demongofy(son)
        else:
            try:
                return collection_to_class[self.domain].demongofy(son)
            except KeyError:
                return son


class Domain(object):    
    def __init__(self, connection, name):
        self.connection = connection
        self.name = name

    def find(self, kwargs='', is_full_query=None):
        return Query(self, kwargs, is_full_query=is_full_query)

    def find_one(self, kwargs):
        result = self.find(kwargs)
        return result.next()

    def find_prefix(self, attr, value):
        # TODO: Perhaps do actual escaping here
        if not sha1_re.search(value):
            raise ValueError('Invalid sha1 prefix %s' % value)
        return Query(self, '%s LIKE "%s%%"' % (attr, value))

    def _encode(self, value):
        if isinstance(value, bool):
            if value:
                return '1'
            else:
                return '0'
        else:
            return repr(unicode(value)).lstrip('u')

    def _prepare_params(self, id, attributes):
        keys = []
        values = []
        # TODO: escape
        if id is not None:
            keys.append('`_id`')
            values.append(self._encode(id))
        for k, v in attributes.iteritems():
            keys.append('`%s`' % k)
            values.append(self._encode(v))
        return keys, values

    def insert(self, attributes, delayed=True):
        keys, values = self._prepare_params(None, attributes)
        if delayed:
            delayed_statement = ' DELAYED'
        else:
            delayed_statement = ''
        query = 'INSERT%s IGNORE INTO `%s` (%s) VALUES (%s)' % (delayed_statement,
                                                                self.name,
                                                                ', '.join(keys),
                                                                ', '.join(values))
        self._execute(query)

    def update(self, id, attributes):
        keys, values = self._prepare_params(None, attributes)
        # Mutable
        args = ', '.join('%s=%s' % (k, v) for k, v in zip(keys, values))
        query = 'UPDATE `%s` SET %s WHERE `id` = %s' % (self.name, args, self._encode(id))
        self._execute(query)

    def select(self, query_string):
        cursor = self.connection.cursor(MySQLdb.cursors.DictCursor)
        self._execute(query_string, cursor=cursor)
        return iter(cursor)

    def drop(self):
        self._execute('DROP TABLE `%s`' % self.name)

    def _execute(self, query_string, cursor=None):
        if not cursor:
            cursor = self.connection.cursor()
        return cursor.execute(query_string)


class Model(object):
    pass


class GitObject(Model, common.GitObject):
    pass


class Blob(Model, common.Blob):
    pass


class Tree(Model, common.Tree):
    pass


class Tag(Model, common.Tag):
    pass


class Commit(Model, common.Commit):
    pass


class Repository(Model, common.Repository):
    pass


class BlobTag(Model, common.BlobTag):
    pass


class BlobTree(Model, common.BlobTree):
    pass


class TreeParentTree(Model, common.TreeParentTree):
    pass


class TreeCommit(Model, common.TreeCommit):
    pass


class TreeTag(Model, common.Tree):
    pass


class CommitParentCommit(Model, common.CommitParentCommit):
    pass


class CommitTree(Model, common.CommitTree):
    pass


class CommitTag(Model, common.CommitTag):
    pass


class TagParentTag(Model, common.TagParentTag):
    pass


class Aggregate(Model, common.Aggregate):
    pass
