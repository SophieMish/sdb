import math

import perfplot
from faker import Faker

from .db import DB, ColumnInfo

fake = Faker()
db = DB("test")
from functools import cache
import random


@cache
def build_records(count):
    return [{
        "name": fake.name(),
        "job": fake.job(),
        "number": fake.phone_number()
    } for _ in range(count)]

    return records


def perf_add():
    name = "test_add"
    def setup(size: int):
        db.delete_table(name)

        columns = [
            ColumnInfo(name="name", is_key=False, type='str'),
            ColumnInfo(name="job", is_key=False, type='str'),
            ColumnInfo(name="number", is_key=True, type='str'),
        ]

        db.create_table(name, columns=columns)

        return build_records(size)

    def kernel(record):
        for record in record:
            db.store_record(name, record)

    perfplot.show(
        setup=setup,
        kernels=[
            kernel,
        ],
        labels=["add"],
        n_range=[1000 * k for k in range(1, 20)],
        xlabel="number of db records",
        # More optional arguments with their default values:
        logx=False,  # set to True or False to force scaling
        logy=False,
        # equality_check=np.allclose,  # set to None to disable "correctness" assertion
        # show_progress=True,
        # target_time_per_measurement=1.0,
        # max_time=None,  # maximum time per measurement
        # time_unit="s",  # set to one of ("auto", "s", "ms", "us", or "ns") to force plot units
        # relative_to=1,  # plot the timings relative to one of the measurements
        # flops=lambda n: 3*n,  # FLOPS plots
    )

def perf_search():
    name = "test_search_by_key"
    def setup(size: int):
        db.delete_table(name)

        columns = [
            ColumnInfo(name="name", is_key=False, type='str'),
            ColumnInfo(name="job", is_key=False, type='str'),
            ColumnInfo(name="number", is_key=True, type='str'),
        ]

        db.create_table(name, columns=columns)

        records = build_records(size)
        for record in records:
            db.store_record(name, record)

        return records

    def search_by_key(records):
        searched_record = random.choice(records)

        key = "number"
        records = list(db.get_record(name, key=key, value=searched_record[key]))
        assert len(records) > 0

    def search_by_non_key(records):
        searched_record = random.choice(records)

        key = "name"
        records = list(db.get_record(name, key=key, value=searched_record[key]))
        assert len(records) > 0


    perfplot.show(
        setup=setup,
        kernels=[
            search_by_key,
            # search_by_non_key,
        ],
        labels=[
            "by key",
            # "by non-key"
        ],
        n_range=[1000 * k for k in range(1, 5)],
        xlabel="number of db records",
        # More optional arguments with their default values:
        logx=False,  # set to True or False to force scaling
        logy=False,
        equality_check=None,  # set to None to disable "correctness" assertion
        # show_progress=True,
        # target_time_per_measurement=1.0,
        # max_time=None,  # maximum time per measurement
        time_unit="us",  # set to one of ("auto", "s", "ms", "us", or "ns") to force plot units
        # relative_to=1,  # plot the timings relative to one of the measurements
        # flops=lambda n: 3*n,  # FLOPS plots
    )

def perf_del():
    name = "del_search_by_key"
    def setup(size: int):
        db.delete_table(name)

        columns = [
            ColumnInfo(name="name", is_key=False, type='str'),
            ColumnInfo(name="job", is_key=False, type='str'),
            ColumnInfo(name="number", is_key=True, type='str'),
        ]

        db.create_table(name, columns=columns)

        records = build_records(size)
        for record in records:
            db.store_record(name, record)

        return records

    def del_by_key(records):
        removed_record = random.choice(records)

        key = "number"
        db.delete_record(name, key=key, value=removed_record[key])

    def del_by_non_key(records):
        removed_record = random.choice(records)

        key = "name"
        db.delete_record(name, key=key, value=removed_record[key])


    perfplot.show(
        setup=setup,
        kernels=[
            del_by_key,
            # del_by_non_key,
        ],
        labels=[
            "by key",
            # "by non-key"
        ],
        n_range=[1000 * k for k in range(1, 5)],
        xlabel="number of db records",
        # More optional arguments with their default values:
        logx=False,  # set to True or False to force scaling
        logy=False,
        equality_check=None,  # set to None to disable "correctness" assertion
        # show_progress=True,
        # target_time_per_measurement=1.0,
        # max_time=None,  # maximum time per measurement
        time_unit="ms",  # set to one of ("auto", "s", "ms", "us", or "ns") to force plot units
        # relative_to=1,  # plot the timings relative to one of the measurements
        # flops=lambda n: 3*n,  # FLOPS plots
    )
