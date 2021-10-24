import os
import tempfile

from flask import (Flask, flash, redirect, render_template, request, send_file,
                   url_for)

from .db import DB, ColumnInfo

app = Flask(__name__)
app.config['SECRET_KEY'] = 'my secret key'
db = DB("prod")


def format_columns_info(columns):
    fmt = []
    for column in columns:
        if column.is_key:
            fmt.append(f"<b>{column.name}</b>")
        else:
            fmt.append(f"{column.name}")

    return ",".join(fmt)


def tables():
    tables_info = [{
        "name": table,
        "columns": format_columns_info(db.load_columns_info(table))
    } for table in db.list_tables()]

    return render_template("tables.html", tables_info=tables_info)


@app.route('/', methods=['GET'])
def index():
    return tables()


@app.route('/delete', methods=['POST'])
def delete_table():
    name = request.form.get('table_name')
    db.delete_table(name)
    return redirect("/")


def get_records(name, filter_column, filter_value):
    if filter_column is not None and filter_value is not None:
        filter_value = db.cast_key(name, filter_column, filter_value)
        if filter_value is None:
            return []

    if filter_column is not None and filter_value is not None:
        return list(db.get_record(name, key=filter_column, value=filter_value))
    else:
        return list(db.list_records(name))


@app.route('/table/<name>/csv', methods=['GET'])
def table_csv(name):
    return app.response_class(db.to_csv(name), mimetype='text/csv')


@app.route('/backup/<name>/', methods=['GET', 'POST'])
def backup(name):
    filename = os.path.join(os.getcwd(), f"{name}_backup.tar.gz")
    if request.method == "GET":
        db.backup(name, filename)
        return send_file(filename, mimetype='application/gzip')
    else:
        request.files.get('file').save(filename)
        db.restore(name, filename)
        return 'file uploaded successfully'


@app.route('/table/<name>', methods=['GET', 'DELETE', 'POST', 'PATCH'])
def table(name):
    map_column_types_to_html = {
        "int": "number",
        "float": "number",
        "str": "text",
    }
    columns = [
        column.name for column in db.load_columns_info(name)
        if column.name != "pk"
    ]
    column_types = {
        column.name: map_column_types_to_html[column.type]
        for column in db.load_columns_info(name)
    }

    limit = request.args.get('limit', 10)
    key = request.args.get('key')
    value = request.args.get('value')
    filter_value = request.args.get('filter_value')
    filter_column = request.args.get('filter_column')

    if request.method == 'GET':
        pass
    elif request.method == 'DELETE':
        filter_value = db.cast_key(name, filter_column, filter_value)
        db.delete_record(name, key=filter_column, value=filter_value)
    elif request.method == 'POST':
        values = {
            key: db.cast_key(name, key, value)
            for key, value in request.json.items()
        }
        db.store_record(name, values)
    elif request.method == 'PATCH':
        pk = int(request.form.get("pk"))
        key = request.form.get("name")
        value = request.form.get("value")
        value = db.cast_key(name, key, value)

        db.edit_record(name, pk, key, value)

    rows = get_records(name, filter_column, filter_value)

    return render_template("table.html",
                           table_name=name,
                           columns=columns,
                           rows=rows,
                           filter_value=filter_value,
                           filter_column=filter_column,
                           column_types=column_types)


@app.route('/create', methods=['GET', 'POST'])
def create():
    if request.method == "GET":
        return render_template("create.html")
    else:
        data = request.json
        name = data["name"]
        fields = [ColumnInfo(**field) for field in data["columns"]]
        db.create_table(name, fields)
        return redirect("/")


if __name__ == '__main__':
    main()


def main():
    app.run()
