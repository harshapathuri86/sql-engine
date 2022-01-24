from ast import operator
import os
import re
import sys
import csv
import numpy as np
from soupsieve import select

schema = {}
METADATA = "../files/metadata.txt"
DIR = "../files"
AGGREGATE_OPS = ["sum", "avg", "min", "max", "count", "distinct"]
COMPARISON_OPS = ["=", "<", ">", "<=", ">="]


def get_operation(condition):
    for op in COMPARISON_OPS:
        if op in condition:
            return op
    return None


def parse(query):
    query = query.lower().strip().split()
    if query[0] != "select":
        print("ERROR: Invalid query. Only select is supported")
        exit(-1)
    select_index = [i for i, x in enumerate(query) if x == "select"]
    from_index = [i for i, x in enumerate(query) if x == "from"]
    where_index = [i for i, x in enumerate(query) if x == "where"]
    print("select_index: {}".format(select_index))
    print("from_index: {}".format(from_index))
    print("where_index: {}".format(where_index))

    if len(select_index) != 1:
        print("ERROR: Invalid query. Only one select is supported")
        exit(-1)
    if len(from_index) != 1:
        print("ERROR: Invalid query. Only one from is supported")
        exit(-1)
    if len(where_index) > 1:
        print("ERROR: Invalid query. Only one where is supported")
        exit(-1)

    select_index = select_index[0]
    from_index = from_index[0]

    if from_index <= select_index:
        print("ERROR: Invalid query. select must come before from")
        exit(-1)

    if len(where_index) == 1:
        where_index = where_index[0]
    else:
        where_index = None

    if where_index:
        if where_index <= from_index:
            print("ERROR: Invalid query. where must come before from")
            exit(-1)

    columns = query[select_index + 1:from_index]
    if where_index:
        tables = query[from_index + 1:where_index]
        conditions = query[where_index + 1:]
    else:
        tables = query[from_index + 1:]
        conditions = []

    if len(tables) == 0:
        print("ERROR: Invalid query. At least one table is required")
        exit(-1)
    if where_index and len(conditions) == 0:
        print("ERROR: Invalid query. At least one condition is required")
        exit(-1)

    print("columns: {}".format(columns))
    print("tables: {}".format(tables))
    print("conditions: {}".format(conditions))

    # Parse tables
    tables = " ".join(tables).split(",")
    parsed_tables = []
    print("raw_tables: {}".format(tables))
    for table in tables:
        print("rt: {}".format(table))
        t = table.split()
        print("t: {}".format(t))
        # check table as alaias format
        if len(t) == 1:
            parsed_tables.append({"table": t[0], "alias": t[0]})
        elif len(t) == 3 and t[1] == "as":
            parsed_tables.append({"table": t[0], "alias": t[2]})
        else:
            print("ERROR: Invalid query. Invalid table format")
            exit(-1)
    print("parsed_tables: {}".format(parsed_tables))

    # check table existence in schema
    for table in parsed_tables:
        if table["table"] not in schema:
            print("ERROR: Invalid query. Table {} does not exist".format(
                table["table"]))
            exit(-1)
    # check table alias uniqueness
    if len(parsed_tables) != len(set([table["alias"] for table in parsed_tables])):
        print("ERROR: Invalid query. Table aliases must be unique")
        exit(-1)

    # Parse columns
    columns = " ".join(columns).split(",")
    parsed_columns = []
    for column in columns:

        match = re.match("(.+)\((.+)\)", column)
        if match:
            aggregation, column = match.groups()
        else:
            aggregation = None
            if re.search(" distinct ", column):
                # TODO: check if distinct is at the beginning or somewhere else : is it a valid query?
                column = column.replace("distinct", "")
                aggregation = "distinct"
            column = column.strip()

        print("aggregation: {}".format(aggregation))
        print("column: {}".format(column))

        table = None
        alias = None
        if "." in column:
            if len(column.split(".")) == 2:
                table, column = column.split(".")
                if table not in [t["alias"] for t in parsed_tables]:
                    print("ERROR: Invalid query. {} does not exist".format(column))
                    exit(-1)
                # get table["table"] from parsed_tables
                alias = table
                table = [t["table"]
                         for t in parsed_tables if t["alias"] == table][0]
                print("table: {}".format(table))
                parsed_columns.append(
                    {"table": table, "column": column, "aggregation": aggregation})
            else:
                print("ERROR: Invalid query. Invalid column format")
                exit(-1)
        else:
            # list of tables that contain column
            tables_with_column = []
            if column != "*":
                for table in parsed_tables:
                    if column in schema[table["table"]]:
                        tables_with_column.append(table["table"])
                if len(tables_with_column) == 0:
                    print(
                        "ERROR: Invalid query. Column {} does not exist".format(column))
                    exit(-1)
                elif len(tables_with_column) > 1:
                    print("ERROR: Invalid query. Column {} is ambiguous".format(column))
                    exit(-1)
                else:
                    table = tables_with_column[0]
                    alias = [t["alias"]
                             for t in parsed_tables if t["table"] == table][0]
                    print("table: {}".format(table))
                    print("alias: {}".format(alias))

                    parsed_columns.append(
                        {"table": table, "column": column, "aggregation": aggregation})
            else:
                if aggregation:
                    print("ERROR: Invalid query. * cannot be used with aggregation")
                    exit(-1)
                if table:
                    parsed_columns.append(
                        {"table": table, "column": column, "aggregation": aggregation})
                else:
                    for table in parsed_tables:
                        parsed_columns.extend([
                            {"table": table["table"], "column": column, "aggregation": aggregation}] for column in schema[table["table"]])

    aggregations = [c["aggregation"]
                    for c in parsed_columns if c["aggregation"]]
    if len(aggregations) > 1:
        print("ERROR: Invalid query. Only one aggregation is supported")
        exit(-1)

    print("parsed_columns: {}".format(parsed_columns))

    # Parse conditions
    parsed_conditions = []
    conditions = " ".join(conditions)

    if " and " in conditions and " or " in conditions:
        print("ERROR: Invalid query. Both AND and OR cannot be used")
        exit(-1)
    if " not " in conditions:
        print("ERROR: Invalid query. NOT cannot be used")
        exit(-1)

    condition_and = re.search(" and ", conditions)
    condition_or = re.search(" or ", conditions)
    condition_not = re.search(" not ", conditions)

    if condition_not:
        print("ERROR: Invalid query. NOT cannot be used")
        exit(-1)
    if condition_and and condition_or:
        print("ERROR: Invalid query. Both AND and OR cannot be used")
        exit(-1)
    if conditions.split(" ").count("and") > 1:
        print("ERROR: Invalid query. AND cannot be used more than once")
        exit(-1)
    if conditions.split(" ").count("or") > 1:
        print("ERROR: Invalid query. OR cannot be used more than once")
        exit(-1)
    if condition_and:
        conditions = conditions.split(" and ")
        operator = "and"
    elif condition_or:
        conditions = conditions.split(" or ")
        operator = "or"
    else:
        conditions = [conditions]
        operator = "and"

    print("conditions: {}".format(conditions))
    print("operator: {}".format(operator))

    parsed_conditions = []
    for condition in conditions:
        value = None
        print("condition: {}".format(condition))

        operation = get_operation(condition)
        if operation is None:
            print("ERROR: Invalid query. Invalid condition format")
            exit(-1)

        parts = condition.split(operation)
        for part in parts:
            if re.match("^[0-9]+$", part):
                value = part

        for part in parts:
            part = part.strip()

            if re.match("^[0-9]+$", part):
                continue

            if "." in part:
                if len(part.split(".")) == 2:
                    table, column = part.split(".")
                    if table not in [t["alias"] for t in parsed_tables]:
                        print(
                            "ERROR: Invalid query. {} does not exist".format(column))
                        exit(-1)
                    # get table["table"] from parsed_tables
                    alias = table
                    table = [t["table"]
                             for t in parsed_tables if t["alias"] == table][0]
                    print("table: {}".format(table))
                    parsed_conditions.append(
                        {"table": table, "column": column, "value": value, "operation": operation})
                else:
                    print("ERROR: Invalid query. Invalid column format")
                    exit(-1)
            else:
                # list of tables that contain column
                tables_with_column = []

                for table in parsed_tables:
                    if part in schema[table["table"]]:
                        tables_with_column.append(table["table"])
                if len(tables_with_column) == 0:
                    print(
                        "ERROR: Invalid query. Column {} does not exist".format(part))
                    exit(-1)
                elif len(tables_with_column) > 1:
                    print(
                        "ERROR: Invalid query. Column {} is ambiguous".format(part))
                    exit(-1)
                else:
                    table = tables_with_column[0]
                    alias = [t["alias"]
                             for t in parsed_tables if t["table"] == table][0]
                    print("table: {}".format(table))
                    print("alias: {}".format(alias))
                    parsed_conditions.append(
                        {"table": table, "column": part, "value": value, "operation": operation})

    print("parsed_conditions: {}".format(parsed_conditions))

    return parsed_tables, parsed_columns, parsed_conditions


def run_query(parsed_tables, parsed_columns, parsed_conditions):
    pass


def init_engine():
    table_name = None
    with open(METADATA, "r") as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith("#"):
                continue
            line = line.strip()
            if line == "":
                continue
            line = line.lower()
            if line == "<begin_table>":
                table_name = None
            elif line == "<end_table>":
                pass
            elif not table_name:
                table_name = line
                schema[table_name] = []
            else:
                schema[table_name].append(line)
    pass


def load_table(table_name):
    table_path = os.path.join(DIR, table_name + ".csv")
    return np.genfromtxt(table_path, delimiter=",", dtype=int)


def print_output(header, rows):
    print(",".join(map(str, header)))
    for row in rows:
        print(",".join(map(str, row)))


def main():
    init_engine()
    print("Schema: {}".format(schema))
    if len(sys.argv) != 2:
        # ERROR
        print("ERROR: Invalid arguments")
        print("Usage: python {} '<sql_query>'".format(sys.argv[0]))
        exit(-1)
    query = sys.argv[1]
    print("Query: {}".format(query))
    print_output(run_query(parse(query)))
    # print_output(output)


if __name__ == '__main__':
    main()
