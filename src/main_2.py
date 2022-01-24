from asyncio import FastChildWatcher
from audioop import add
from calendar import day_abbr
from distutils.dep_util import newer_pairwise
import queue
import sys
import itertools
from functools import reduce
import operator
from tokenize import group
from moz_sql_parser import parse
import sys
import os
import re


schema = {}
aggregates = ["max", "min", "average", "sum", "count"]
where_operations = {'lt': operator.lt, 'gt': operator.gt, 'lte': operator.le,
                    'gte': operator.ge, 'eq': operator.eq, 'neq': operator.ne}
aggregate_functions = {"sum": lambda x: sum(x), "average": lambda x: sum(
    x) * (1/len(x)), "max": lambda x: max(x), "min": lambda x: min(x), "count": lambda x: len(x)}


def print_error(variable, error):
    print("ERROR: {} => {}".format(variable, error))
    exit()


def load_metadata(file):
    # check if file exists
    if not os.path.isfile(file):
        print_error(file, "File {} does not exist".format(file))
    table = []
    # open file
    file = open(file, "r")
    if not file:
        print_error(file, "File cannot be read")

    for line in file:
        line = line.rstrip("\n").strip()
        if line.startswith("#"):
            continue
        if line == "<begin_table>":
            table = []
        elif line == "<end_table>":
            schema[table[0]] = table[1:]
        else:
            table.append(line.lower().strip())
    file.close()


def load_table(table_name):
    file = open(table_name+'.csv', "r")
    if not file:
        print_error(table_name, "File cannot be read")
    table = []
    for line in file:
        parts = line.split(",")
        row = []
        for part in parts:
            row.append(int(part))
        table.append(row)
    file.close()
    table = list(map(list, zip(*table)))
    columns = schema[table_name]
    table = {columns[i]: table[i] for i in range(len(columns))}
    return table


def join(tables, from_tables):
    table_names = list(tables.keys())
    column_names = []
    formatted_tables = []
    for table in from_tables:
        columns = from_tables[table]
        cols = []
        for column in columns:
            column_names.append(column)
            cols.append(tables[table][column])
        formatted_tables.append(list(map(list, zip(*cols))))

    current_table = formatted_tables[0]
    for table in formatted_tables[1:]:
        next_table = table
        temp_table = []
        for row in current_table:
            for next_row in next_table:
                temp_table.append(row + next_row)
        current_table = temp_table
    return current_table, column_names


def filter_conditions(table, columns, conditions, final_condition=None):
    new_table = []
    for row in table:
        status = []
        for condition in conditions:
            # print("condition: {}".format(condition))
            op = list(condition.keys())[0]
            lhs = condition[op][0]
            rhs = condition[op][1]
            op = where_operations[op]
            if (isinstance(lhs, str) and isinstance(rhs, str)):
                lhs = row[columns.index(lhs)]
                rhs = row[columns.index(rhs)]
            elif (isinstance(lhs, str)):
                lhs = row[columns.index(lhs)]
            elif (isinstance(rhs, str)):
                rhs = row[columns.index(rhs)]
            status.append(op(lhs, rhs))

        if final_condition:
            res = status[0]
            for i in range(1, len(status)):
                if final_condition == "or":
                    res = res or status[i]
                else:
                    res = res and status[i]
            if res:
                new_table.append(row)
        else:
            if status[0]:
                new_table.append(row)
    return new_table


def order_by(product, columns, orderby):
    print("Order by {}".format(orderby))
    # check ascending or descending
    rows = []
    reverse = False
    if orderby.get("sort") == "desc":
        reverse = True
    if isinstance(orderby["value"], dict):
        col = list(list(orderby['value'].items())[0])
        orderby["value"] = col[1]
    order = orderby["value"]
    rows = sorted(
        product, key=lambda x: x[columns.index(order)], reverse=reverse)
    # print("rows: {}".format(rows))
    return rows


def distinct(rows):
    return [list(row) for row in set(tuple(row) for row in rows)]


def aggregate(groups, column_names, aggregates, groupby=[]):
    product = []
    for grp in groups:
        columns = list(zip(*groups[grp]))
        agg_grp = []
        for i, column in enumerate(columns):
            if groupby and i in groupby:
                agg_grp.append(grp)
                continue
            agg = aggregates[i]
            if agg[1] == "count":
                agg_grp.append(len(column))
            elif agg[1] == "sum":
                agg_grp.append(sum(column))
            elif agg[1] == "avg":
                agg_grp.append((sum(column)*(1/len(column))))
            elif agg[1] == "min":
                agg_grp.append(min(column))
            elif agg[1] == "max":
                agg_grp.append(max(column))
        product.append(agg_grp)
    return product


def output(product, header, keep_columns):
    new_product = []
    for row in product:
        new_row = []
        for i in range(len(keep_columns)):
            if keep_columns[i]:
                new_row.append(row[i])
        new_product.append(new_row)
    product = new_product
    new_header = []
    for i in range(len(keep_columns)):
        if keep_columns[i]:
            new_header.append(header[i])
    header = new_header

    print(",".join(header))
    for row in product:
        print(",".join(map(str, row)))


def parse_select(obj):
    if obj["select"] == "*":
        for table in schema:
            for column in schema[table]:
                obj["columns"].append(column)
    else:
        # possible types : dict, list
        if isinstance(obj["select"], dict) and isinstance(obj["select"]["value"], dict):
            if "distinct" in obj["select"]["value"]:
                # print("Distinct")
                obj["distinct"] = True
                obj["select"] = obj["select"]["value"]["distinct"]
        if not isinstance(obj["select"], list):
            obj["select"] = [obj["select"]]
        for column_object in obj["select"]:
            # print("column: {}".format(column_object))
            func = None
            if isinstance(column_object["value"], dict):
                # aggregate
                func = list(column_object["value"].keys())[0]
                if func not in aggregates:
                    print_error(
                        query, "Invalid aggregate function {}".format(func))
                col = column_object["value"][func]
            else:
                col = column_object["value"]
            if col == "*":
                for table in schema:
                    obj["columns"].extend(cols for cols in schema[table])
                if func:
                    for table in schema:
                        obj["aggregate"].extend([(col, func)
                                                 for col in schema[table]])
            else:
                obj["columns"].append(col)
                if func:
                    obj["aggregate"].append((col, func))


def parse_where(obj, columns, conditions):
    if 'where' in obj:
        if list(obj["where"].keys())[0] in ['and', 'or']:
            conditions = list(obj["where"].values())[0]
        else:
            conditions = [obj["where"]]
        # print("Conditions: {}".format(conditions))
        for condition in conditions:
            # print("Condition: {}".format(condition))
            operations = list(condition.keys())[0]
            for col in condition[operations]:
                if isinstance(col, str):
                    columns.append(col)


def parse_from(obj):
    obj["from_tables"] = {}
    for column in obj["columns"]:
        table = None
        for t in schema:
            if column in schema[t]:
                table = t
                break
        if table is None:
            print_error(query, "Column {} does not exist".format(column))
        if table not in obj["from_tables"]:
            obj["from_tables"][table] = []
        if column not in obj["from_tables"][table]:
            obj["from_tables"][table].append(column)


def parse_orderby(obj, columns):
    if "orderby" in obj:
        if isinstance(obj["orderby"]["value"], dict):
            column = list(obj["orderby"]["value"].items())[0]
            column = (column[1], column[0])
            if column not in obj["aggregate"]:
                if "groupby" not in obj:
                    print_error(
                        column[0], " Orderby aggregates must be grouped")
                else:
                    for col in obj["aggregate"]:
                        if column[0] == col[0]:
                            print_error(
                                column[0], " Different aggregate in orderby and groupby")
                    obj["aggregate"].append(column)
                columns.append(column[0])
            pass
        else:
            columns.append(obj["orderby"]["value"])


def check_query():
    if len(sys.argv) != 2:
        print("Usage: python3 main.py <sql_file>")
        exit(0)
    query = sys.argv[1].lower().strip()

    # check ; at the end
    if query[-1] != ';':
        print_error(query, "Expected ; at the end of query")
    query = query[:-1]
    # check empty query
    if query == "":
        print_error(query, "Empty query")
    return query


if __name__ == '__main__':
    query = check_query()
    try:
        obj = parse(query)
    except Exception as e:
        print_error(query, "Invalid query")

    # print("Object: {}".format(obj))

    if "select" not in obj:
        print_error(query, "No SELECT in the query")
    if "from" not in obj:
        print_error(query, "No FROM in the query")

    load_metadata("metadata.txt")
    # print("Schema {}".format(schema))

    if isinstance(obj["from"], str):
        obj["from"] = [obj["from"]]

    table_names = list(schema.keys())
    for table in table_names:
        if table not in obj["from"]:
            del schema[table]

    tables = {}
    for table in obj["from"]:
        if table not in table_names:
            print_error(query, "Table {} does not exist".format(table))
        tables[table] = load_table(table)
    # print("Tables: {}".format(tables))

    obj["columns"] = []
    obj["aggregate"] = []
    obj["distinct"] = False

    parse_select(obj)
    # print("obj: {}".format(obj))
    parse_from(obj)
    # print("From tables: {}".format(obj["from_tables"]))

    required_columns = []
    conditions = []

    parse_where(obj, required_columns, conditions)

    if "groupby" in obj:
        # Code to parse multiple columsn in groupby clause
        # if isinstance(obj["groupby"], list):
        #     columns = [list(x.values())[0] for x in obj["groupby"]]
        #     obj["groupby"] = {"value": columns}
        # else:
        # obj["groupby"]["value"] = [obj["groupby"]["value"]]
        # required_columns.extend(obj["groupby"]["value"])

        required_columns.append(obj["groupby"]["value"])

    parse_orderby(obj, required_columns)
    required_columns = [
        col for col in required_columns if col not in obj["columns"]]

    for column in required_columns:
        table = None
        for t in schema:
            if column in schema[t]:
                table = t
                break
        if table is None:
            print_error(column, "Column {} does not exist".format(column))
        if table not in obj["from_tables"]:
            obj["from_tables"][table] = []
        if column not in obj["from_tables"][table]:
            obj["from_tables"][table].append(column)

    factor = 1
    for table in tables:
        if table not in obj["from_tables"]:
            factor *= len(tables[table][list(tables[table].keys())[0]])
    # print("Factor: {}".format(factor))

    product, column_names = join(tables, obj["from_tables"])

    if "where" in obj:
        if list(obj["where"].keys())[0] in ['and', 'or']:
            product = filter_conditions(
                product, column_names, conditions, list(obj['where'].keys())[0])
        else:
            product = filter_conditions(
                product, column_names, conditions)

    new_product = []
    for _ in range(factor):
        new_product.extend(product)
    product = new_product
    # print("Product: {}".format(product))

    if "groupby" in obj:
        index = column_names.index(obj["groupby"]["value"])
        groups = {}
        for row in product:
            if row[index] not in groups:
                groups[row[index]] = []
            groups[row[index]].append(row)
        if obj["aggregate"]:
            product = aggregate(groups, column_names,
                                obj["aggregate"], [index])
        else:
            # TODO : understand
            product = sorted(product, key=lambda row: row[index])
            obj["distinct"] = True
    elif obj["aggregate"]:
        product = {1: product}
        product = aggregate(product, column_names, obj["aggregate"])
        # print("Product: {}".format(product))
        pass

    if "orderby" in obj:
        product = order_by(
            product, column_names, obj["orderby"])

    # print("Object: {}".format(obj))
    if obj["distinct"]:
        product = distinct(product)

    # TODO print header
    header = []
    for table in obj["from_tables"]:
        columns = obj["from_tables"][table]
        header.extend(["{}.{}".format(table, col) for col in columns])
    keep_columns = [True for _ in range(len(header))]
    for column in required_columns:
        keep_columns[column_names.index(column)] = False
    for col, func in obj["aggregate"]:
        header[column_names.index(col)] = "{}({})".format(
            func, header[column_names.index(col)])

    # remove row[i] from product if keep_columns[i] is False
    output(product, header, keep_columns)
