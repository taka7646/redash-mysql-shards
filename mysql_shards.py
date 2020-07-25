  # -*- coding: utf-8 -*-

from redash.query_runner import *
from redash.query_runner.mysql import Mysql, types_map
import MySQLdb
import threading
import logging
from redash.utils import json_dumps, json_loads

logger = logging.getLogger(__name__)

class Result(object):
    def __init__(self):
        self.rows = []
        self.columns = []
        self.error = None
        self.thread = None
        self.sql_thread_id = None
        pass

class MysqlShards(Mysql):
    def __init__(self, configuration):
        super().__init__(configuration)
        self._parsed = False
        self._param1 = []
        self._param2 = []
        self._param3 = []
        self._param4 = []
        self._shard_count = 0

    @classmethod
    def name(cls):
        return "MySQL (Sharding)"

    @classmethod
    def type(cls):
        return 'mysql_shards'

    @classmethod
    def enabled(cls):
        return True

    @classmethod
    def configuration_schema(cls):
        schema = {
            'type': 'object',
            'properties': {
                'user': {
                    'type': 'string'
                },
                'passwd': {
                    'type': 'string',
                    'title': 'Password'
                },
                'host': {
                    'type': 'string',
                    'title': 'host'
                },
                'db': {
                    'type': 'string',
                    'default': 'db_{param1}',
                },
                'port': {
                    'type': 'string',
                    'default': '3306',
                },
                'param1': {
                    'type': 'string',
                    'title': 'shard parameter1 replace {param1}',
                    'default': 'shard1, shard2, shard3, shard4'
                },
                'param2': {
                    'type': 'string',
                    'title': 'shard parameter2 replace {param2}',
                    'default': '',
                },
                'param3': {
                    'type': 'string',
                    'title': 'shard parameter3 replace {param3}',
                    'default': '',
                },
                'param4': {
                    'type': 'string',
                    'title': 'shard parameter4 replace {param4}',
                    'default': '',
                }
            }
        }
        return schema

    def _parse_param(self):
        if self._parsed:
            return
        self._param1 = [n.strip() for n in self.configuration.get("param1", "").split(',')]
        self._param2 = [n.strip() for n in self.configuration.get("param2", "").split(',')]
        self._param3 = [n.strip() for n in self.configuration.get("param3", "").split(',')]
        self._param4 = [n.strip() for n in self.configuration.get("param4", "").split(',')]
        self._parsed = True
        self._shard_count = len(self._param1)

    def _get_config(self, name, default_value, index):
        v = self.configuration.get(name, default_value)
        if type(v) is not str:
            return v
        if len(self._param1) > index:
            v = v.replace('{param1}', self._param1[index])
        if len(self._param2) > index:
            v = v.replace('{param2}', self._param2[index])
        if len(self._param3) > index:
            v = v.replace('{param3}', self._param3[index])
        if len(self._param4) > index:
            v = v.replace('{param4}', self._param4[index])
        return v

    def _connection(self, index):
        params = dict(
            host=self._get_config("host", "", index),
            user=self._get_config("user", "", index),
            passwd=self._get_config("passwd", "", index),
            db=self._get_config("db", "", index),
            port=self._get_config("port", 3306, index),
            charset="utf8",
            use_unicode=True,
            connect_timeout=60,
        )

        logger.debug("connection params: %s", json_dumps(params))
        connection = MySQLdb.connect(**params)
        return connection

    def _get_tables(self, schema):
        query = """
        SELECT col.table_schema as table_schema,
               col.table_name as table_name,
               col.column_name as column_name
        FROM `information_schema`.`columns` col
        WHERE col.table_schema NOT IN ('information_schema', 'performance_schema', 'mysql', 'sys');
        """

        self._parse_param()
        connection = self._connection(0)
        cursor = connection.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute(query)
        data = cursor.fetchall()

        for row in data:
            if row["table_schema"] != self.configuration["db"]:
                table_name = "{}.{}".format(row["table_schema"], row["table_name"])
            else:
                table_name = row["table_name"]

            if table_name not in schema:
                schema[table_name] = {"name": table_name, "columns": []}

            schema[table_name]["columns"].append(row["column_name"])

        return list(schema.values())

    def run_query(self, query, user):
        logger.info("run_query: ")
        self._parse_param()
        events = []
        results = []
        json_data = ""
        error = None
        logger.info("run_query: %d", self._shard_count)
        try:
            for i in range(self._shard_count):
                ev = threading.Event()
                events.append(ev)
                r = Result()
                results.append(r)
                connection = self._connection(i)
                t = threading.Thread(
                    target=self._run_query, args=(query, user, connection, r, ev, i)
                )
                r.thread = t
                r.sql_thread_id = connection.thread_id()
                t.start()
            while True:
                finished = 0
                for i in range(self._shard_count):
                    if events[i] is None:
                        finished += 1
                        continue
                    if events[i].wait(1):
                        events[i] = None
                        finished += 1
                if finished == self._shard_count:
                    break
            columns = []
            rows = []
            errors = []
            for r in results:
                if len(columns) == 0:
                    columns = r.columns
                rows.extend(r.rows)
                if r.error is not None:
                    errors.append(r.error)
            data = {"columns": columns, "rows": rows}
            json_data = json_dumps(data)
            if len(errors) == 0:
                error = None
            else:
                error = ",".join(errors)
        except (KeyboardInterrupt, InterruptException, JobTimeoutException):
            for r in results:
                if r.sql_thread_id:
                    self._cancel(r.sql_thread_id)
                    r.sql_thread_id = None
                if r.thread:
                    r.thread.join()
                    r.thread = None
            raise
        return json_data, error


    def _run_query(self, query, user, connection, r, ev, shard_index):
        try:
            cursor = connection.cursor()
            logger.debug("ShardingMysql running query: %s", query)
            cursor.execute(query)

            data = cursor.fetchall()
            desc = cursor.description

            while cursor.nextset():
                if cursor.description is not None:
                    data = cursor.fetchall()
                    desc = cursor.description
            # TODO - very similar to pg.py
            if desc is not None:
                columns = self.fetch_columns(
                    [(i[0], types_map.get(i[1], None)) for i in desc]
                )
                r.columns.append({'name':'SHARD_ID','friendly_name':'SHARD_ID', 'type':'integer'})
                r.columns.extend(columns)
                rows = []
                for row in data:
                    rd = {'SHARD_ID': shard_index}
                    for i, c in enumerate(columns):
                        rd[c['name']] = row[i]
                    rows.append(rd)
                r.rows = rows
            cursor.close()
        except MySQLdb.Error as e:
            if cursor:
                cursor.close()
            r.error = e.args[1]
        finally:
            ev.set()
            if connection:
                connection.close()
            r.sql_thread_id = None
            r.thread = None



register(MysqlShards)

