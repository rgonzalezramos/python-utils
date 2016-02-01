"""
Small wrapper around mysql-python to provide auto-closable cursors and transactions inside a context
object.

Requires:
    - mysql-python
"""

import logging
import traceback

import MySQLdb
import MySQLdb.cursors


class Database(object):
    def __init__(self, settings):
        self._settings = settings

    def __enter__(self):
        self._db = self._connect(self._settings)
        self._db.autocommit(False)  # Enable transactions
        self._open_resources = [self._db]
        return self

    def __exit__(self, some, other, args):
        while self._open_resources:
            resource = self._open_resources.pop()
            self._safe_close(resource)

    def cursor(self):
        cursor = self._db.cursor()
        self._open_resources.append(cursor)
        return cursor

    def commit(self):
        self._db.commit()

    def rollback(self):
        self._db.rollback()

    @staticmethod
    def _connect(settings):
        logging.debug('Opening connection...')
        return MySQLdb.connect(
            host=settings.DB_HOST,
            db=settings.DB_NAME,
            user=settings.DB_USER,
            passwd=settings.DB_PASS,
            cursorclass=MySQLdb.cursors.DictCursor)

    @staticmethod
    def _safe_close(resource):
        logging.debug('Closing %s' % resource)
        try:
            resource.close()
        except Exception as safely_ignored:
            logging.exception('While trying to close open resource')
