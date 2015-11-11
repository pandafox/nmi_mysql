"""
    Custom mysql wrapper for pymysql
    Usefull for raw queries and scripting
"""

import re
import logging
import pymysql.cursors


class DB():

    def __init__(self, conf, autoconnect=False):
        self.logger = logging.getLogger('database')
        self.host = conf['host']
        self.user = conf['user']
        self.password = conf['password']
        self.db_conn = conf['db']
        self.port = int(conf['port'])
        self.handle = None
        self.connected = False

        if autoconnect:
            self.connect()

    def __del__(self):
        self.close()

    def connect(self):

        self.logger.info('Trying to connect to mysql database')
        try:
            con = pymysql.connect(host=self.host, user=self.user, password=self.password,
                                  db=self.db_conn, port=self.port, charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)

        except Exception as err:
            self.logger.error('Failed to connect to db')
            self.logger.warn('Error:')
            self.logger.info(err)
            return None

        self.logger.info('Connection to mysql')
        self.connected = True
        self.handle = con
        return True

    def close(self):
        try:
            if self.connected:
                self.handle.close()
                self.connected = False
                self.handle = None
                self.logger.warn('Disconnecting to db, closing connection')

        except Exception as err:
            self.logger.warn('Failed to close connection')
            self.logger.warn(err)

        return None

    def query(self, _query, _params=None):
        """
            self.handle holds the connection
            _query is the query
            _params holds the variables need by the query
        """

        result = None
        query = _query

        if _params:
            query = self.generate_query(_query, _params)

        try:
            with self.handle.cursor() as cursor:
                cursor.execute(query, ())

                if 'insert' in query.lower() or 'update' in query.lower():
                    result = {
                        'affected_rows': cursor.rowcount
                    }

                else:
                    result = list(cursor.fetchall())

        except Exception as err:
            self.logger.warn(err)
            return None

        self.handle.commit()

        return result

    def generate_query(self, _query, _params):
        """
            replace all instances of ? to %s la :D YOLO programming best!
        """

        query = re.sub('\?', '%s', _query)

        if not isinstance(_params, list):
            return query % self.to_string(_params)

        params = []
        values = []
        for param in _params:
            if isinstance(param, tuple):
                values.append('(' + self.to_string(param) + ')')

            else:
                params.append(self.to_string(param))

        if values:
            params = ', '.join(values)
            query = query % params[1:-1]

        else:
            query = query % tuple(params)

        return query

    def to_string(self, temp):
        if isinstance(temp, (list, tuple)):
            tmp = []
            for item in temp:
                if isinstance(item, str):
                    item = item.replace('%', '%%')
                tmp.append(self.handle.escape(item))

            return ', '.join(tmp)

        elif isinstance(temp, dict):
            tmp = []
            for key in temp:
                if isinstance(temp[key], str):
                    temp[key] = temp[key].replace('%', '%%')
                tmp.append(key + ' = ' + self.handle.escape(temp[key]))

            return ', '.join(tmp)

        elif isinstance(temp, str):
            return self.handle.escape(temp.replace('%', '%%'))

        else:
            return self.handle.escape(temp)
