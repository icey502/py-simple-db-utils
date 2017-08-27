import petl
import pymysql

"""
DB Utils is a collection of database utilities that are meant to help abstract
away from the mechanics of database operations and help you manage things at
a slightly higher level.
"""
class SimpleDictWriter(object):
    """ A convenience class for writing dicts to a database.
    Example usage:
    writer = SimpleDictWriter(db)
    writer.write_dict("myTable", {'id':1234,'value':'mydata'}, commit=True)
    """
    def __init__(self, db, verbose=False):
        self.name = "SimpleDictWriter"
        if not db:
            raise ValueError('SimpleDictWriter requires a db object, which is a dict with your database connection fields.')
        self.db = db
        self.verbose = verbose

    def _out(self, message):
        """ Handles giving output in a consistent manner. """
        if self.verbose:
            print ("%s:%s" % (self.name, message))

    def _get_connection(self):
        return pymysql.connect(host=self.db["DB_HOST"], port=self.db["DB_PORT"], user=self.db["DB_USER"], passwd=self.db["DB_PASS"], db=self.db["DB"], charset='utf8')

    def _generate_dict_insert_sql(self, table, d):
        placeholders = ', '.join(['%s'] * len(d))
        columns = ', '.join(d.keys())
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
        return sql

    def _generate_dict_update_sql(self, table, d, id_field_name):
        set_str = ""
        for key, value in d.iteritems():
            if key != id_field_name:
                set_str += "%s='%s'," % (key, value)
        sql = "UPDATE %s SET %s WHERE %s='%s'" % (table, set_str[:-1], id_field_name, d[id_field_name]) # the "-1" is to take off the last comma
        return sql

    def _write_dict_row(self, conn, cursor, table_name, data, commit=True):
        """ write a dict to a table.  Note that the dict keys must match with your database schema. """
        sql = self._generate_dict_insert_sql(table_name, data)
        self._out(data)
        cursor.execute(sql, data.values())
        if commit:
            conn.commit()

    def _update_dict_row(self, conn, cursor, table_name, data, id_field_name, commit=True):
        """ update a dict to a table.  Note that the dict keys must match with your database schema. """
        sql = self._generate_dict_update_sql(table_name, data, id_field_name)
        self._out(sql)
        cursor.execute(sql)
        if commit:
            conn.commit()

    def update_dict(self, table_name, data, id_field_name, commit=True):
        """ write a dict to a table.  Note that the dict keys must match with your database schema. """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SET SQL_MODE=ANSI_QUOTES')
        sql = self._generate_dict_update_sql(table_name, data, id_field_name)
        self._out(sql)
        cursor.execute(sql)
        if commit:
            conn.commit()

    def update_list_of_dicts(self, table_name, data, id_field_name):
        """ write a list of dicts to a table.  Note that the dict keys must match with your database schema.
        Commits will be done in chunks. """
        self._out("updating list of data...")
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SET SQL_MODE=ANSI_QUOTES')
        count = 0
        commit = True if (len(data) == 1) else False
        for row in data:
            self._update_dict_row(conn, cursor, table_name, row, id_field_name, commit=commit)
            count += 1
            if count == len(data) - 1:
                commit = True # commit on the last row...
            elif count % 500 == 0:
                commit = True # or in 500-element chunks
                self._out("updated %d values" % count)
            else:
                commit = False
        self._out("done, updated %d values" % count)

    def write_dict(self, table_name, data, commit=True):
        """ write a dict to a table.  Note that the dict keys must match with your database schema. """
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SET SQL_MODE=ANSI_QUOTES')
        self._write_dict_row(conn, cursor, table_name, data, commit=commit)

    def write_list_of_dicts(self, table_name, data):
        """ write a list of dicts to a table.  Note that the dict keys must match with your database schema.
        Commits will be done in chunks. """
        self._out("writing list of data...")
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('SET SQL_MODE=ANSI_QUOTES')
        count = 0
        commit = True if (len(data) == 1) else False
        for row in data:
            self._write_dict_row(conn, cursor, table_name, row, commit=commit)
            count += 1
            if count == len(data) - 1:
                commit = True # commit on the last row...
            elif count % 500 == 0:
                commit = True # or in 500-element chunks
                self._out("wrote %d values" % count)
            else:
                commit = False
        self._out("done, wrote %d values" % count)

    def run_sql(self, sql, commit=True):
        """ Run sql on a table.  Be careful with this, because it is not checking for deletes, truncates, drops, etc! """
        if sql:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            if commit:
                conn.commit()

class SimpleDictReader(object):
    """ A convenience class for writing dicts to a database.
    Example usage:
    reader = SimpleDictReader(db)
    reader.read_dict("table_name")
    """
    def __init__(self, db, verbose=False):
        self.name = "SimpleDictReader"
        if not db:
            raise ValueError('SimpleDictReader requires a db object, which is a dict with your database connection fields.')
        self.db = db
        self.verbose = verbose

    def _out(self, message):
        """ Handles giving output in a consistent manner. """
        if self.verbose:
            print ("%s:%s" % (self.name, message))

    def _get_connection(self):
        return pymysql.connect(host=self.db["DB_HOST"], port=self.db["DB_PORT"], user=self.db["DB_USER"], passwd=self.db["DB_PASS"], db=self.db["DB"], use_unicode=True, charset="utf8")

    def read_table(self, table_name, query=None):
        """ Reads a table and returns a list of dicts """
        conn = self._get_connection()
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        if query:
            cursor.execute(query)
        else:
            cursor.execute("SELECT * FROM %s" % table_name)
        return cursor.fetchall()

    def read_table_as_o1_dict(self, table_name, key_name, query=None):
        """ If you know your table has a PK, this will run your query and load it as a dict with the PK as an index """
        data = self.read_table(table_name, query)
        # transpose into a dict
        d = {}
        for row in data:
            if key_name in row:
                d[row[key_name]] = row
        return d
