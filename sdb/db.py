import glob
import os
import shutil
import struct
import tarfile
from dataclasses import asdict, dataclass
from functools import cache
from typing import Any, Dict, List, Set

import mmh3
import ujson

MAX_DB_SIZE = int(2**20)
BUCKET_SIZE = int(2**10)
NUMBER_OF_BUCKETS = int(MAX_DB_SIZE / BUCKET_SIZE)

MarkupStruct = struct.Struct("ii?")
MARKUP_STRUCT_SIZE = MarkupStruct.size


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    is_key: bool
    type: str


def hash_to_bucket(value, number_of_buckets=NUMBER_OF_BUCKETS):
    hashed = mmh3.hash128(str(value), seed=42)
    return hashed % NUMBER_OF_BUCKETS

class DB:
    def __init__(self, path):
        self.pk = 0
        self.path = os.path.abspath(path)

    def create_table(self, name, columns: List[ColumnInfo] = None):
        path = self.get_table_path(name)

        if os.path.exists(path):
            return

        os.makedirs(path)

        columns = columns or []

        # we need primary key for fast edit operations
        primary_key_column = ColumnInfo(name="pk", is_key=True, type="int")
        columns.append(primary_key_column)

        self.store_columns_info(name, columns)

        with open(self.get_markup_path(name), "wb"):
            pass

        with open(self.get_storage_path(name), "wb"):
            pass

        with open(self.get_tables_list_path(), "at") as tables:
            tables.write(f"{name}\n")

    def list_tables(self):
        tables = []

        if not os.path.isfile(self.get_tables_list_path()):
            return tables

        with open(self.get_tables_list_path(), "rt") as tables_list:
            for line in tables_list:
                tables.append(line.strip())

        return tables

    def delete_table(self, name):
        path = self.get_table_path(name)
        if not os.path.exists(path):
            return

        shutil.rmtree(path)

        with open(self.get_tables_list_path(), "rt+") as tables:
            data = tables.readlines()
            tables.seek(0)
            for line in data:
                line = line.strip()
                if line != name:
                    tables.write(f"{line}\n")
            tables.truncate()

    def store_record(self, name: str, values: Dict[str, Any]):
        keys = self.load_keys(name)

        if "pk" not in values:
            values["pk"] = self.get_pk(name)

        for key in keys:
            if self.is_key_exist(name, key, values[key]):
                return False

        start, length = self.do_store_record(name, values)

        offset = self.store_markup(name, start, length)

        for key in keys:
            if key in values:
                self.store_key(name, key, values[key], offset)

        return True

    def get_record(self, name, key, value):
        keys = self.load_keys(name)

        if key in keys:
            yield self.get_record_by_key(name, key, value)
        else:
            for record in self.get_record_by_non_key(name, key, value):
                yield record

    def list_records(self, name):
        with open(self.get_storage_path(name), "rb") as storage:
            for markup in self.list_markup_records(name):

                _, start, length, active = markup
                if not active:
                    continue

                storage.seek(start)
                data = storage.read(length)
                record = ujson.loads(data)
                yield record

    def delete_record(self, name, key, value):
        keys = self.load_keys(name)

        if key in keys:
            return self.delete_record_by_key(name, key, value)
        else:
            return self.delete_record_by_non_key(name, key, value)

    def edit_record(self, name, pk, key, value):
        record = self.get_record_by_key(name, "pk", pk)
        if record[key] == value:
            return
        self.delete_record_by_key(name, "pk", pk)
        record[key] = value

        self.store_record(name, record)

    def get_tables_list_path(self):
        return os.path.join(self.path, "tables.txt")

    def store_markup(self, name, begin, length):
        with open(self.get_markup_path(name), "ab") as markup:
            offset = int(markup.tell() / MARKUP_STRUCT_SIZE)
            markup.write(MarkupStruct.pack(begin, length, True))
            return offset

    def list_markup_records(self, name):
        with open(self.get_markup_path(name), "rb") as markup:
            while True:
                offset = int(markup.tell() / MARKUP_STRUCT_SIZE)

                data = markup.read(MARKUP_STRUCT_SIZE)
                if len(data) == 0:
                    return

                record = MarkupStruct.unpack(data)
                yield offset, *record

    def to_csv(self, name):
        columns = [column.name for column in self.load_columns_info(name)]
        header = ",".join(columns)
        yield f"{header}\n"
        for record in self.list_records(name):
            record = ",".join(str(record[column]) for column in columns)
            yield f"{record}\n"

    def backup(self, name, output_file):
        with tarfile.open(output_file, "w|gz") as archive:
            files = glob.glob(os.path.join(self.get_table_path(name), "*"))
            for fn in files:
                archive.add(fn, os.path.basename(fn))

    def restore(self, name, archive):
        self.delete_table(name)
        self.create_table(name)
        with tarfile.open(archive, "r|gz") as archive:
            archive.extractall(path=self.get_table_path(name))

    def read_markup_record(self, name, offset):
        with open(self.get_markup_path(name), "rb") as markup:
            markup.seek(int(offset * MARKUP_STRUCT_SIZE))

            data = markup.read(MARKUP_STRUCT_SIZE)
            if len(data) == 0:
                return

            return MarkupStruct.unpack(data)

    def markup_set_inactive(self, name, offset):
        with open(self.get_markup_path(name), "rb+") as markup:
            markup.seek(int(offset * MARKUP_STRUCT_SIZE))

            data = markup.read(MARKUP_STRUCT_SIZE)
            if len(data) == 0:
                return

            start, length, active = MarkupStruct.unpack(data)
            if not active:
                return

            markup.seek(int(offset * MARKUP_STRUCT_SIZE))
            data = MarkupStruct.pack(start, length, False)
            markup.write(data)

    def store_columns_info(self, name, columns):
        path = os.path.join(self.get_table_path(name), "info.txt")
        with open(path, "wt") as info:
            for column in columns:
                data = ujson.dumps(asdict(column))
                info.write(f"{data}\n")

    @cache
    def load_columns_info(self, name):
        path = os.path.join(self.get_table_path(name), "info.txt")
        columns = []
        with open(path, "rt") as info:
            for line in info:
                column = ColumnInfo(**ujson.loads(line))
                columns.append(column)

        return columns

    @cache
    def load_keys(self, name):
        columns = self.load_columns_info(name)
        return set(column.name for column in columns if column.is_key)

    @cache
    def get_table_path(self, name):
        return os.path.join(self.path, name)

    def get_storage_path(self, name: str):
        return os.path.join(self.get_table_path(name), "storage.json")

    def get_markup_path(self, name: str):
        return os.path.join(self.get_table_path(name), "markup.bin")

    def get_pk_path(self, name):
        return os.path.join(self.get_table_path(name), "pk.txt")

    def do_store_record(self, name: str, values: Dict[str, Any]):
        data = ujson.dumps(values)
        with open(self.get_storage_path(name), "at") as storage:
            start = storage.tell()
            storage.write(data)
            length = len(data)

        return start, length

    def get_bucket_name(self, name, key, value):
        bucket = hash_to_bucket(value)
        return os.path.join(self.get_table_path(name), f"{key}_{bucket}.json")

    def is_key_exist(self, name, key, value):
        path = self.get_bucket_name(name, key, value)
        if not os.path.isfile(path):
            return False
        with open(path, "rt") as bucket:
            for line in bucket:
                record = ujson.loads(line)
                if record[key] == value:
                    # record with such key already exists
                    return True

        return False

    def store_key(self, name, key, value, offset):
        path = self.get_bucket_name(name, key, value)
        with open(path, "at") as bucket:
            record = {
                key: value,
                "offset": offset,
            }

            bucket.write(f"{ujson.dumps(record)}\n")

    def cleanup_bucket(self, name, key, value):
        bucket = hash_to_bucket(value)
        bucket_file = self.get_bucket_name(name, key, value)

        bucket_content = []
        with open(bucket_file, "rt") as bucket:
            bucket_content = [ujson.loads(line) for line in bucket]

        bucket_content = [
            record for record in bucket_content if record[key] != value
        ]

        with open(bucket_file, "wt") as bucket_file:
            for line in bucket_content:
                bucket_file.write(f"{ujson.dumps(line)}\n")

    def delete_record_by_key(self, name, key, value):
        bucket = hash_to_bucket(value)
        bucket_file = self.get_bucket_name(name, key, value)

        if not os.path.isfile(bucket_file):
            return None

        record = self.get_record_by_key(name, key, value)

        if not record:
            return

        with open(bucket_file, "rt") as bucket:
            for line in bucket:
                bucket_record = ujson.loads(line)

                if bucket_record[key] == value:
                    offset = bucket_record["offset"]
                    self.markup_set_inactive(name, offset)
                    break

        keys = self.load_keys(name)
        for key in keys:
            self.cleanup_bucket(name, key, record[key])

    def delete_record_by_non_key(self, name, key, value):
        keys = self.load_keys(name)
        with open(self.get_storage_path(name), "rb") as storage:
            for markup in self.list_markup_records(name):
                offset, start, length, active = markup
                if not active:
                    continue

                storage.seek(start)
                data = storage.read(length)
                record = ujson.loads(data)

                if record[key] != value:
                    continue

                self.markup_set_inactive(name, offset)

                if record[key] == value:
                    for _key in keys:
                        self.cleanup_bucket(name, _key, record[_key])

    def get_record_by_key(self, name, key, value):
        bucket = hash_to_bucket(value)
        bucket = self.get_bucket_name(name, key, value)

        if not os.path.isfile(bucket):
            return None

        with open(bucket, "rt") as bucket:
            for line in bucket:
                bucket_record = ujson.loads(line)
                if bucket_record[key] == value:
                    offset = bucket_record["offset"]
                    start, length, active = self.read_markup_record(
                        name, offset)
                    if not active:
                        continue
                    return self.read_storage(name, start, length)

    def get_record_by_non_key(self, name, key, value):
        with open(self.get_storage_path(name), "rb") as storage:
            for markup in self.list_markup_records(name):
                _, start, length, active = markup

                if not active:
                    continue

                storage.seek(start)
                data = storage.read(length)
                record = ujson.loads(data)

                if record[key] == value:
                    yield record

    def read_storage(self, name, start, length):
        with open(self.get_storage_path(name), "rt") as storage:
            storage.seek(start)
            data = storage.read(length)
            record = ujson.loads(data)

            return record

    def get_pk(self, name):
        pk_file_path = self.get_pk_path(name)
        if not os.path.isfile(pk_file_path):
            with open(pk_file_path, "wt") as pk_file:
                pk_file.write(f"0")

        with open(pk_file_path, "rt") as pk_file:
            pk = int(pk_file.read().strip())

        _pk = pk
        pk += 1

        with open(pk_file_path, "wt") as pk_file:
            pk_file.write(f"{pk}")

        return _pk

    def cast_key(self, name, key, value):
        map_types = {
            'str': str,
            'int': int,
            'float': float,
        }

        columns = self.load_columns_info(name)

        for column in columns:
            if column.name == key:
                return map_types[column.type](value)


if __name__ == '__main__':
    db = DB("test")
    columns = [
        ColumnInfo(name="name", is_key=False, type='str'),
        ColumnInfo(name="number", is_key=True, type='str'),
        ColumnInfo(name="age", is_key=False, type='int'),
    ]
    # db.create_table("addressbook", columns=columns)
    # db.create_table("addressbook2", columns=columns)

    # db.store_record("addressbook", {
    #     "name": "John Doe",
    #     "number": "+7912323271",
    #     "age": 54
    # })
    # db.store_record("addressbook", {
    #     "name": "John Doe11",
    #     "number": "+7912323271t3",
    #     "age": 54
    # })

    # db.store_record("addressbook", {
    #     "name": "John Doe11",
    #     "number": "+79123232711efsdf",
    #     "age": 54
    # })

    # print(db.get_record_by_key("addressbook", "number", "+7912323271t3"))
    # print(db.get_record_by_key("addressbook", "number", "+8245"))
    # print(list(db.get_record("addressbook", "number", "+7912323271")))
    # print(list(db.get_record("addressbook", "name", "John Doe11")))
    print(list(db.list_records("addressbook")))
    # db.delete_record("addressbook", "name", "John Doe11")
