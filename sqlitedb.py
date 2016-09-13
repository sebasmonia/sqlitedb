"""Helper to work with SQLite databases: create tables from sequences, query, etc."""

import sqlite3 as _sqlite3
import helperfunctions as _hf
import os as _os
import csv as _csv
import pathlib as _pathlib
from collections import namedtuple as _namedtuple
from contextlib import closing as _closing
from enum import IntEnum as _IntEnum


def _resolve_default_location():
    home_path = _pathlib.Path(_os.path.expanduser("~"))
    folder_name = _pathlib.Path(".sqlitedb")
    default_location = home_path / folder_name
    default_location.mkdir(exist_ok=True)
    return str(default_location)

default_db_location = _resolve_default_location()


def _load_known_dbs():
    as_dict = {db.replace(".db", ""): db for db in _os.listdir(
        default_db_location) if db.endswith(".db")}
    ctor = _namedtuple("dbs", as_dict.keys())
    return ctor(**as_dict)


class OutputType(_IntEnum):
    """Enum used in DB for the type of return value from the queries."""
    namedtuple = 1
    dict = 2
    tuple = 3


class DB():
    """
    Open a database for use. "path" can be either a file or a full path.
    If it's a single file, it is searched in the current directory, then
    in the default DB location.
    If it doesn't exists, the DB is created using the same rules: create
    at specific location for full paths; if it's a single file name,
    create in the default directory.
    """
    known_dbs = _load_known_dbs()

    def __init__(self, path):
        self._db_file_path = str(DB._resolve_filename(path))
        self.default_return_type = OutputType.namedtuple
        self.connection = _sqlite3.connect(self._db_file_path)

    def query(self, sql, return_type=None):
        """
        Run the query specified in "sql". The "return_type" argument lets you
        specify the type of object to return per row: namedtuple (default),
        tuple, or dictionary.
        """
        if not return_type:
            return_type = self.default_return_type
        results = []
        with _closing(self.connection.cursor()) as cursor:
            exec_result = cursor.execute(sql)
            columns = [column[0] for column in exec_result.description]
            if return_type == OutputType.tuple:
                results = list(exec_result)
            if return_type == OutputType.dict:
                results = [dict(zip(columns, row)) for row in exec_result]
            if return_type == OutputType.namedtuple:
                unique = DB._make_cols_unique(columns)
                builder = _namedtuple('r', unique)
                results = [builder._make(row) for row in exec_result]
        return results

    def __del__(self):
        self.close()

    def close(self):
        """Close the connection to the DB. This releases the file's lock."""
        self.connection.close()

    def get_all_rows(self, table_name):
        """Run a SELECT * on table_name."""
        return self.query("SELECT * FROM " + table_name)

    def rowcount(self, sql, params=None):
        results = -25
        with _closing(self.connection.cursor()) as cursor:
            if params:
                exec_result = cursor.executemany(sql, params)
            else:
                exec_result = cursor.execute(sql)
            results = exec_result.rowcount
            self.connection.commit()
        return results

    def table_from_namedtuple(self, table_name, stuff):
        """
        Create a table using a sequence of namedtuple to determine the columns
        and datatypes.
        """
        if not stuff:
            print("Collection is empty")
            return
        cols = DB._get_columns(stuff)
        statement = "CREATE TABLE {name} ({field_list})"
        field = "{name} {type}"
        with _closing(self.connection.cursor()) as cursor:
            field_list = ', '.join(field.format(name=f, type=t)
                                   for f, t in cols)
            prepared = statement.format(name=table_name, field_list=field_list)
            cursor.execute(prepared)
            self.connection.commit()
        return True

    def insert_namedtuple(self, table_name, stuff, create=False):
        """
        Insert a sequence of namedtuple into a table. The option parameter
        "create" (default False) allows us to create and insert in a single call.
        """
        if not stuff:
            print("Collection is empty")
            return
        cols = DB._get_columns(stuff)
        if create:
            self.table_from_namedtuple(table_name, stuff)
        statement = "INSERT INTO {name} ({fields}) VALUES ({to_fill})"
        fields = ','.join(x[0] for x in cols)
        to_fill = ','.join(":" + x[0] for x in cols)
        prepared = statement.format(
            name=table_name, fields=fields, to_fill=to_fill)
        as_dicts = (item._asdict() for item in stuff)
        with _closing(self.connection.cursor()) as cursor:
            exec_result = cursor.executemany(prepared, as_dicts)
            self.connection.commit()
            return exec_result.rowcount == len(stuff)

    def insert_csv(self, file_path):
        """
        Insert the contents of a CSV file into a table. Always creates a table
        with the name of the CSV file, replacing spaces with underscores.
        WARNING: If the table exists, it is dropped and recreated.
        """
        path = _pathlib.Path(file_path)
        table_name = path.stem.replace(" ", "_")
        csv_rows = _hf.read_csv_to_namedtuple(str(path))
        with _closing(self.connection.cursor()) as cursor:
            cursor.execute("DROP TABLE IF EXISTS " + table_name)
        self.insert_namedtuple(table_name, csv_rows, True)
        return table_name

    def insert_dicts(self, table_name, dict_list):
        """
        Insert a sequence of dicts in a table. The list if first "normalized"
        so that all dictionaries have the same keys.
        """
        def _normalize_dict(a_dict, keys):
            return {k: a_dict.get(k) for k in keys}

        all_keys = set()
        for d in dict_list:
            all_keys.update(d.keys())
        normalized = [_normalize_dict(d, all_keys) for d in dict_list]
        temp = _namedtuple('temp', all_keys)
        rows = [temp(**dn) for dn in normalized]
        self.insert_namedtuple(table_name, rows, True)

    def list_tables(self):
        """List all the tables in the database."""
        tables = self.query(
            "SELECT Name FROM sqlite_master WHERE type='table'", OutputType.tuple)
        return tuple(x[0] for x in tables)

    def list_columns(self, table_name):
        """List all the columns in table_name."""
        results = self.query(
            "PRAGMA table_info(" + table_name + ")", OutputType.tuple)
        return tuple((col[1], col[2]) for col in results)

    def _read_csv_to_namedtuple(path):
        with open(path, encoding='utf-8') as f:
            header_text = next(f).rstrip('\n')
            fields = [h.replace(' ', '_').lower() for h in header_text.split(',')]
            row_creator = namedtuple('row', fields)
            reader = _csv.reader(f)
            return list(map(row_creator._make, reader))

    @staticmethod
    def _resolve_filename(filename):
        original_path = _pathlib.Path(filename)
        default_dir_path = _pathlib.Path(default_db_location) / original_path
        if original_path.is_file():
            return original_path
        elif default_dir_path.is_file():
            return default_dir_path
        else:
            # so, it's a new DB
            if len(original_path.parts) == 1:
                # it's a single filename, no dir provided.
                # create the DB in the default dir
                return default_dir_path
            else:
                return original_path

    @staticmethod
    def _type_mapper(item, field_name):
        # SQLite natively supports the following types: NULL, INTEGER, REAL, TEXT,
        # BLOB.
        t = getattr(item, field_name)
        if isinstance(t, int):
            return "INT"
        if isinstance(t, float):
            return "REAL"
        if isinstance(t, bytes):
            return "BLOB"
        if isinstance(t, memoryview):
            return "BLOB"
        return "TEXT"  # for everything else, really...that's how SQLite rolls

    @staticmethod
    def _get_columns(stuff):
        item = stuff[0]
        col_types = [(f, DB._type_mapper(item, f)) for f in item._fields]
        # col_types.insert(0, ('ID', 'INTEGER PRIMARY KEY'))
        return col_types

    @staticmethod
    def _make_cols_unique(cols):
        seen = []
        for c in cols:
            counter = 1
            while c in seen:
                c = c + ("_" * counter)
                counter += 1
            seen.append(c)
        return seen
