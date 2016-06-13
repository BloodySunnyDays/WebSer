# -*- coding:utf-8 -*-


__author__ = 'Djj'

import functools
import threading
import logging
import time

# 数据库引擎对象:
class _Engine(object):
    def __init__(self, connect):
        self._connect = connect
    def connect(self):
        return self._connect()

engine = None

# 持有数据库连接的上下文对象:
class _DbCtx(threading.local):
    def __init__(self):
        self.connection = None
        self.transactions = 0

    def is_init(self):
        return not self.connection is None

    def init(self):
        self.connection = _LasyConnection()
        self.transactions = 0

    def cleanup(self):
        self.connection.cleanup()
        self.connection = None

    def cursor(self):
        return self.connection.cursor()

_db_ctx = _DbCtx()

# 有了这两个全局变量，我们继续实现数据库连接的上下文，目的是自动获取和释放连接：
class _ConnectionCtx(object):
    def __enter__(self):
        global _db_ctx
        self.should_cleanup = False
        if not _db_ctx.is_init():
            _db_ctx.init()
            self.should_cleanup = True
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        if self.should_cleanup:
            _db_ctx.cleanup()

def connection():
    return _ConnectionCtx()

class _LasyConnection(object):

    def __init__(self):
        self.connection = None

    def cursor(self):
        if self.connection is None:
            connection = engine.connect()
            logging.info('open connection <%s>...' % hex(id(connection)))
            self.connection = connection
        return self.connection.cursor()

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.connection.rollback()

    def cleanup(self):
        if self.connection:
            connection = self.connection
            self.connection = None
            logging.info('close connection <%s>...' % hex(id(connection)))
            connection.close()

# pymssql.connect(host='.',user='sa',password='sa',database='local')
def create_engine(user, password, database, host='127.0.0.1', port=3306, **kw):
    import pymssql
    global engine
    if engine is not None:
        raise DBError('Engine is already initialized.')

    engine = _Engine(lambda: pymssql.connect(host=host,user=user,password=password,database=database))
    # test connection...
    logging.info('Init mysql engine <%s> ok.' % hex(id(engine)))

class DBError(Exception):
    pass


def with_connection(func):

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        with _ConnectionCtx():
            return func(*args, **kw)
    return _wrapper

@with_connection
def select(sql, *args):

    return _select(sql, False, *args)

def _select(sql, first, *args):
    ' execute select SQL and return unique result or list results.'
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        names = []
        y = ''
        if cursor.description:
            for x in cursor.description:
                y= str(x[0])
                names.append(y)
                # names = [x[0] for x in cursor.description]
        if first:
            values = cursor.fetchone()
            if not values:
                return None
            return Dict(names, str(values))

        return [Dict(names, x) for x in cursor.fetchall()]
    finally:
        if cursor:
            cursor.close()

@with_connection
def _update(sql, *args):
    global _db_ctx
    cursor = None
    sql = sql.replace('?', '%s')
    logging.info('SQL: %s, ARGS: %s' % (sql, args))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql, args)
        r = cursor.rowcount
        if _db_ctx.transactions==0:
            # no transaction enviroment:
            logging.info('auto commit')
            _db_ctx.connection.commit()
        return r
    finally:
        if cursor:
            cursor.close()


def _execsp(sql):
    global _db_ctx
    cursor = None
    logging.info('SQL: %s ' % (sql))
    try:
        cursor = _db_ctx.connection.cursor()
        cursor.execute(sql)
        names = []
        y = ''
        if cursor.description:
            for x in cursor.description:
                y= str(x[0])
                names.append(y)

        # values = cursor.fetchone()
        # if not values:
        #     return None
        # return Dict(names, str(values))
        TD = [Dict(names, x) for x in cursor.fetchall()]
        _db_ctx.connection.commit()
        return TD
    finally:
        if cursor:
            cursor.close()

@with_connection
def exec_sp(sql):
    return _execsp(sql)


def insert(table, **kw):
    '''
    Execute insert SQL.

    >>> u1 = dict(id=2000, name='Bob', email='bob@test.org', passwd='bobobob', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 2000)
    >>> u2.name
    u'Bob'
    >>> insert('user', **u2)
    Traceback (most recent call last):
      ...
    IntegrityError: 1062 (23000): Duplicate entry '2000' for key 'PRIMARY'
    '''
    cols, args = zip(*kw.iteritems())
    sql = 'insert into %s (%s) values (%s)' % (table, ','.join(['%s' % col for col in cols]), ','.join(['?' for i in range(len(cols))]))
    sql = sql
    return _update(sql, *args)


def update(sql, *args):
    r'''
    Execute update SQL.

    >>> u1 = dict(id=1000, name='Michael', email='michael@test.org', passwd='123456', last_modified=time.time())
    >>> insert('user', **u1)
    1
    >>> u2 = select_one('select * from user where id=?', 1000)
    >>> u2.email
    u'michael@test.org'
    >>> u2.passwd
    u'123456'
    >>> update('update user set email=?, passwd=? where id=?', 'michael@example.org', '654321', 1000)
    1
    >>> u3 = select_one('select * from user where id=?', 1000)
    >>> u3.email
    u'michael@example.org'
    >>> u3.passwd
    u'654321'
    >>> update('update user set passwd=? where id=?', '***', '123\' or id=\'456')
    0
    '''
    return _update(sql, *args)

@with_connection
def select_one(sql, *args):

    return _select(sql, True, *args)

class Dict(dict):

    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        try:
            return self[key.upper()]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        self[key.upper()] = value

class _TransactionCtx(object):
    '''
    _TransactionCtx object that can handle transactions.

    with _TransactionCtx():
        pass
    '''

    def __enter__(self):
        global _db_ctx
        self.should_close_conn = False
        if not _db_ctx.is_init():
            # needs open a connection first:
            _db_ctx.init()
            self.should_close_conn = True
        _db_ctx.transactions = _db_ctx.transactions + 1
        logging.info('begin transaction...' if _db_ctx.transactions==1 else 'join current transaction...')
        return self

    def __exit__(self, exctype, excvalue, traceback):
        global _db_ctx
        _db_ctx.transactions = _db_ctx.transactions - 1
        try:
            if _db_ctx.transactions==0:
                if exctype is None:
                    self.commit()
                else:
                    self.rollback()
        finally:
            if self.should_close_conn:
                _db_ctx.cleanup()

    def commit(self):
        global _db_ctx
        logging.info('commit transaction...')
        try:
            _db_ctx.connection.commit()
            logging.info('commit ok.')
        except:
            logging.warning('commit failed. try rollback...')
            _db_ctx.connection.rollback()
            logging.warning('rollback ok.')
            raise

    def rollback(self):
        global _db_ctx
        logging.warning('rollback transaction...')
        _db_ctx.connection.rollback()
        logging.info('rollback ok.')

def _profiling(start, sql=''):
    t = time.time() - start
    if t > 0.1:
        logging.warning('[PROFILING] [DB] %s: %s' % (t, sql))
    else:
        logging.info('[PROFILING] [DB] %s: %s' % (t, sql))

def with_transaction(func):

    @functools.wraps(func)
    def _wrapper(*args, **kw):
        _start = time.time()
        with _TransactionCtx():
            return func(*args, **kw)
        _profiling(_start)
    return _wrapper





# create_engine(user='sa', password='data-setpass', database='SDETicket2009DB_SZYL', host='192.168.0.28')
# #
# user =  select('select 	AccName,CertNo,BeginDate,InvaliDate,RegDate from IC_AccInfo where CertNo = ?','330501198612099010')
# #
# #
# #
# #
# # print  '\xbb\xa2\xc7\xf0'
# #
# # #
# AccName = user[0]['AccName'].encode('raw_unicode_escape')

# RegDate = str(user[0]['RegDate'])
# BeginDate = str(user[0]['BeginDate'])
# InvaliDate = str(user[0]['InvaliDate'])
#
# user[0]['RegDate'] = RegDate
# user[0]['BeginDate'] = BeginDate
# user[0]['InvaliDate'] = InvaliDate
# user[0]['AccName'] = AccName
# print user[0]
# data_string = json.dumps(user[0])
# print "JSON:",data_string
# # #
# print user[0]['TicketShortName'].encode('raw_unicode_escape')

