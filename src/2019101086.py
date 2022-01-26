from cmath import inf
from concurrent.futures.process import _threads_wakeups
import operator
from tkinter.messagebox import NO

schema = {}
aggregates = ["max", "min", "avg", "sum", "count"]
where_operations = {'lt': operator.lt, 'gt': operator.gt, 'lte': operator.le,
                    'gte': operator.ge, 'eq': operator.eq, 'neq': operator.ne}


def print_error(variable, error):
    if variable is None:
        import sys
        variable = sys.argv[1].strip()
    print("ERROR: {} => {}".format(variable, error))
    exit()


def load_file(path):
    try:
        file = open(path, "r")
    except FileNotFoundError:
        try:
            file = open("files/" + path, "r")
        except FileNotFoundError:
            try:
                file = open("../files/" + path, "r")
            except FileNotFoundError:
                print_error(
                    path, "File does not exist")
    return file


def load_metadata():
    file = load_file("metadata.txt")
    for line in file:
        line = line.rstrip("\n").strip()
        if line.startswith("#"):
            continue
        if line == "<begin_table>":
            table = []
        elif line == "<end_table>":
            schema[table[0]] = table[1:]
        else:
            table.append(line.strip())
    file.close()


def load_table(table_name):
    file = load_file(table_name + ".csv")
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
    rows = []
    reverse = False
    if orderby.get("sort") == "desc":
        reverse = True
    if isinstance(orderby["value"], dict):
        col = list(list(orderby['value'].items())[0])
        col = (col[1], col[0])
        orderby["value"] = col
    else:
        orderby["value"] = (orderby["value"], None)
    order = orderby["value"]
    # print("columns: ", columns)
    # print("order: {}".format(order))
    # print("index: {}".format(columns.index(order)))
    # print("product: ", product)
    rows = sorted(
        product, key=lambda x: x[columns.index(order)], reverse=reverse)
    return rows


def distinct(rows):
    distinct_rows = []
    for row in rows:
        if tuple(row) not in distinct_rows:
            distinct_rows.append(tuple(row))
    return [list(row) for row in distinct_rows]


def aggregate(groups, column_names, obj_columns, aggregates, groupby=[]):
    product = []
    # print("obj cols", obj_columns)
    import numpy as np
    agg_keys = np.array([x[0] for x in aggregates])
    for grp in groups:
        columns = list(zip(*groups[grp]))
        agg_grp = [float(-inf) for _ in range(len(obj_columns))]
        for i, col in enumerate(obj_columns):
            column_name = col[0]
            agg = col[1]
            index = column_names.index(column_name)
            column = columns[index]
            # print("col name: ", col[0])
            # print("index: ", index)
            # print("column: ", column)
            # print("agg: ", agg)
            # print("i: ", i)
            if agg is None:
                # print("grp by", groupby)
                if groupby and column_name in groupby:
                    agg_grp[i] = column[0]
                elif groupby == []:
                    agg_grp[i] = column[0]
                continue

            if agg == "count":
                agg_grp[i] = len(column)
            elif agg == "sum":
                agg_grp[i] = sum(column)
            elif agg == "avg":
                agg_grp[i] = sum(column) / len(column)
            elif agg == "min":
                agg_grp[i] = min(column)
            elif agg == "max":
                agg_grp[i] = max(column)
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
                obj["columns"].append((column, None))
    else:
        # possible types : dict, list
        if isinstance(obj["select"], dict) and isinstance(obj["select"]["value"], dict):
            if "distinct" in obj["select"]["value"]:
                obj["distinct"] = True
                obj["select"] = obj["select"]["value"]["distinct"]
        if not isinstance(obj["select"], list):
            obj["select"] = [obj["select"]]
        for column_object in obj["select"]:
            func = None
            if isinstance(column_object["value"], dict):
                # aggregate
                func = list(column_object["value"].keys())[0]
                if func not in aggregates:
                    print_error(
                        None, "Invalid aggregate function {}".format(func))
                col = column_object["value"][func]
            else:
                col = column_object["value"]
            if col == "*":
                for table in schema:
                    obj["columns"].extend((cols, func)
                                          for cols in schema[table])
                if func:
                    for table in schema:
                        obj["aggregate"].extend([(col, func)
                                                 for col in schema[table]])
            else:
                obj["columns"].append((col, func))
                if func:
                    obj["aggregate"].append((col, func))


def parse_where(obj, columns):
    conditions = []
    if 'where' in obj:
        if list(obj["where"].keys())[0] in ['and', 'or']:
            conditions = list(obj["where"].values())[0]
        else:
            conditions = [obj["where"]]
        for condition in conditions:
            operations = list(condition.keys())[0]
            if operations not in where_operations:
                print_error(
                    operations, "Not a valid operation in where clause")
            for col in condition[operations]:
                if isinstance(col, str):
                    columns.append((col, None))
    return conditions


def parse_from(obj):
    obj["from_tables"] = {}
    for column, _ in obj["columns"]:
        table = None
        for t in schema:
            if column in schema[t]:
                table = t
                break
        if table is None:
            print_error(column, "Column does not exist")
        if table not in obj["from_tables"]:
            obj["from_tables"][table] = []
        if column not in obj["from_tables"][table]:
            obj["from_tables"][table].append(column)


def parse_orderby(obj, columns):
    if "orderby" in obj:
        if isinstance(obj["orderby"], list):
            print_error(None, "Multiple columns for order by is not supported")
        elif isinstance(obj["orderby"]["value"], dict):
            column = list(obj["orderby"]["value"].items())[0]
            column = (column[1], column[0])
            if column not in obj["aggregate"]:
                if "groupby" not in obj:
                    print_error(
                        column[0], "Orderby aggregates must be grouped")
                else:
                    for col in obj["aggregate"]:
                        if column[0] == col[0]:
                            if (column[1] is None or col[1] is None) and not (column[1] is None and col[1] is None):
                                print_error(
                                    column[0], "Different aggregates for same columns")
                    obj["aggregate"].append(column)
                columns.append(column)
        else:
            columns.append((obj["orderby"]["value"], None))


def check_query():
    import sys
    if len(sys.argv) != 2:
        print("Usage: python3 main.py <sql_file>")
        exit(0)
    query = sys.argv[1].strip()
    if query[-1] != ';':
        print_error(None, "Expected ; at the end of query")
    query = query[:-1]
    if query == "":
        print_error(None, "Empty query")
    return query


def sqlEngine():
    query = check_query()
    try:
        from moz_sql_parser import parse
        obj = parse(query)
    except Exception as e:
        print_error(query, "Invalid query")

    if "select" not in obj:
        print_error(query, "No SELECT in the query")
    if "from" not in obj:
        print_error(query, "No FROM in the query")

    load_metadata()

    if isinstance(obj["from"], str):
        obj["from"] = [obj["from"]]

    table_names = list(schema.keys())
    for table in table_names:
        if table not in obj["from"]:
            del schema[table]

    tables = {}
    for table in obj["from"]:
        if table not in table_names:
            print_error(table, "Table does not exist")
        tables[table] = load_table(table)

    obj["columns"] = []
    obj["aggregate"] = []
    obj["distinct"] = False

    parse_select(obj)
    parse_from(obj)

    required_columns = []

    conditions = parse_where(obj, required_columns)

    if "groupby" in obj:
        required_columns.append((obj["groupby"]["value"], None))

    parse_orderby(obj, required_columns)
    required_columns = [
        col for col in required_columns if col not in obj["columns"]]

    for column, func in required_columns:
        table = None
        for t in schema:
            if column in schema[t]:
                table = t
                break
        if table is None:
            print_error(column, "Column does not exist")
        if table not in obj["from_tables"]:
            obj["from_tables"][table] = []
        if column not in obj["from_tables"][table]:
            obj["from_tables"][table].append(column)

    factor = 1
    for table in tables:
        if table not in obj["from_tables"]:
            factor *= len(tables[table][list(tables[table].keys())[0]])

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
    # print("product hifi :", product)
    if "groupby" in obj:
        # print("OBJ ", obj["columns"], required_columns)
        index = column_names.index(obj["groupby"]["value"])
        groups = {}
        for row in product:
            if row[index] not in groups:
                groups[row[index]] = []
            groups[row[index]].append(row)
        if obj["aggregate"]:
            # print("col names", column_names)
            # product = aggregate(groups, column_names,
            #                     obj["aggregate"], [index])
            product = aggregate(groups, column_names, obj["columns"]+required_columns,
                                obj["aggregate"], [obj["groupby"]["value"]])
        else:
            # group by a column
            product = sorted(product, key=lambda row: row[index])
            obj["distinct"] = True
    elif obj["aggregate"]:
        product = {1: product}
        # product = aggregate(product, column_names, obj["aggregate"])
        product = aggregate(product, column_names,
                            obj["columns"]+required_columns, obj["aggregate"])
        pass

    # print("product lol: ", product)
    header = []
    formated_columns = []
    for table in obj["from_tables"]:
        columns = obj["from_tables"][table]
        formated_columns.extend(["{}.{}".format(table, col)
                                for col in columns])
    keep_columns = [True for _ in range(
        len(obj["columns"]))]
    keep_columns.extend([False for _ in range(len(required_columns))])

    if "orderby" in obj:
        product = order_by(
            product, obj["columns"]+required_columns, obj["orderby"])

    if obj["distinct"]:
        product = distinct(product)

    for col, func in obj["columns"]:
        if func is None:
            header.append("{}".format(
                formated_columns[column_names.index(col)]))
        else:
            header.append("{}({})".format(
                func, formated_columns[column_names.index(col)]))

    output(product, header, keep_columns)


if __name__ == '__main__':
    try:
        sqlEngine()
    except FileNotFoundError as e:
        print_error(e.filename, "File not found")
    except PermissionError as e:
        print_error(e.filename, "Permission denied")
    except Exception as e:
        print_error(None, "Invalid query")
