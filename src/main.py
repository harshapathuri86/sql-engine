from ast import keyword
from asyncio import current_task
from tokenize import group
from matplotlib.pyplot import table
from numpy import column_stack
import sqlparse
import sys
import os
import re

relational_operators = ["=", "<", ">", "<=", ">="]
aggregates = ["max", "min", "average", "sum", "count"]
schema = {}


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


def valid_table(table):
    if table not in schema:
        print("Table {} does not exist".format(table))
        return False
    return True


def valid_column(column, table=None):
    if table is None:
        for t in schema:
            if column in schema[t]:
                return True
        # print("Column {} does not exist".format(column))
        return False
    if column in schema[table]:
        return True
    # print("Column {} does not exist in table {}".format(column, table))
    return False


def get_column_name(column):

    # if wildcard is present
    if column.find("*") != -1:
        return None, "*"

    if "." in column:
        if valid_column(column=column.split(".")[1], table=column.split(".")[0]):
            return column.split(".")[0], column.split(".")[1]
        else:
            return None, None
    else:
        if valid_column(column=column):
            for t in schema:
                if column in schema[t]:
                    return t, column
        else:
            return None, None


def parse_column(column):
    column = str(column)
    match = re.match("(.+)\((.+)\)", column)
    if match:
        agg = match.group(1)
        column = match.group(2)
        table, column = get_column_name(column)
        if agg == "count":
            return table, column, lambda x: len(x), agg
        elif agg == "max":
            return table, column, lambda x: max(x), agg
        elif agg == "min":
            return table, column, lambda x: min(x), agg
        elif agg == "average":
            return table, column, lambda x: sum(x) * 1.0 / len(x), agg
        elif agg == "sum":
            return table, column, lambda x: sum(x), agg
        else:
            print_error(agg, "Aggregate {} is not supported".format(agg))
    else:
        table, column = get_column_name(column)
        return table, column, lambda x: x, None


def parse_order_by(token, available_columns):
    tokens = token.value.split()
    if len(tokens) != 2:
        print_error(token, "Invalid order by statement")
    if tokens[1].lower() == "asc":
        for i, column in enumerate(available_columns):
            if tokens[0] == column:
                return (lambda s: s[i]), False
    if tokens[1].lower() == "desc":
        for i, column in enumerate(available_columns):
            if tokens[0] == column:
                return (lambda s: s[i]), True
    print_error(token, "Invalid order by statement")


def parse_expression(s):
    # IMP order of queries is important first <=, then <, then >, then >=, then =
    s = re.sub(r'where ', '', s)
    eq = re.compile("(.*)=(.*)")
    lt = re.compile("(.*)<(.*)")
    gt = re.compile("(.*)>(.*)")
    le = re.compile("(.*)<=(.*)")
    ge = re.compile("(.*)>=(.*)")

    if le.match(s) is not None and len(le.match(s).groups()) == 2:
        try:
            p = int(le.match(s).groups()[1].strip())
            return (le.match(s).groups()[0].strip(), lambda lhs: lhs <= p, None)
        except:
            return (le.match(s).groups()[0].strip(), lambda lhs, rhs: lhs <= rhs, le.match(s).groups()[1].strip())
    if lt.match(s) is not None and len(lt.match(s).groups()) == 2:
        try:
            p = int(lt.match(s).groups()[1].strip())
            return (lt.match(s).groups()[0].strip(), lambda lhs: lhs < p, None)
        except:
            return (lt.match(s).groups()[0].strip(), lambda lhs, rhs: lhs < rhs, lt.match(s).groups()[1].strip())
    if ge.match(s) is not None and len(ge.match(s).groups()) == 2:
        try:
            p = int(ge.match(s).groups()[1].strip())
            return (ge.match(s).groups()[0].strip(), lambda lhs: lhs >= p, None)
        except:
            return (ge.match(s).groups()[0].strip(), lambda lhs, rhs: lhs >= rhs, ge.match(s).groups()[1].strip())
    if gt.match(s) is not None and len(gt.match(s).groups()) == 2:
        try:
            p = int(gt.match(s).groups()[1].strip())
            return (gt.match(s).groups()[0].strip(), lambda lhs: lhs > p, None)
        except:
            return (gt.match(s).groups()[0].strip(), lambda lhs, rhs: lhs > rhs, gt.match(s).groups()[1].strip())
    if eq.match(s) is not None and len(eq.match(s).groups()) == 2:
        try:
            p = int(eq.match(s).groups()[1].strip())
            return (eq.match(s).groups()[0].strip(), lambda lhs: lhs == p, None)
        except:
            return (eq.match(s).groups()[0].strip(), lambda lhs, rhs: lhs == rhs, eq.match(s).groups()[1].strip())

    print_error(s, "Invalid expression")


def modify_columns(exp, available_columns):
    idx = None
    if exp[2] != None:
        for i, v in enumerate(available_columns):
            if exp[2] == v:
                idx = i
        if idx == None:
            # TODO test case "select distinct A,C from table1,table2 where A=640 and D="
            print_error(exp[2], "Column {} does not exist".format(exp[2]))
    for i, v in enumerate(available_columns):
        if exp[0] == v and exp[2] == None:
            return (lambda s: exp[1](s[i]))
        elif exp[0] == v and exp[2] != None:
            return (lambda s: exp[1](s[i], s[idx]))
        print_error(exp[0], "Column {} does not exist".format(exp[0]))
    exit(0)


def load_table(table_name):
    file = open('../files/'+table_name+'.csv', "r")
    if not file:
        print_error(table_name, "File cannot be read")
    table = []
    for line in file:
        parts = line.split(",")
        col = []
        for part in parts:
            col.append(int(part))
        table.append(col)
    file.close()
    return table


def join(tables):
    if len(tables) == 1:
        return load_table(tables[0])
    elif len(tables) == 0:
        print("No tables to join")
        exit()
    else:
        current_table = load_table(tables[0])
        for table in tables[1:]:
            next_table = load_table(table)
            temp_table = []
            for row in current_table:
                for next_row in next_table:
                    temp_table.append(row + next_row)
            current_table = temp_table
        return current_table


def execute(query):

    table_names = []
    available_columns = []

    # print("tokens: ", query.tokens)

    # process from clause
    for i, token in enumerate(query.tokens):
        # identify from clause
        if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == "from":

            for j in range(i+1, len(query.tokens)):

                if query.tokens[j].ttype == sqlparse.tokens.Keyword:
                    print_error(
                        query.tokens[j].value, "Keyword not allowed in FROM clause")

                if isinstance(query.tokens[j], sqlparse.sql.Identifier):
                    table_names.append(query.tokens[j].get_name())
                    print("table: {}".format(query.tokens[j].get_name()))
                    available_columns = schema[query.tokens[j].get_name()]
                    if available_columns == None:
                        print_error(
                            query.tokens[j].get_name(), "Table does not exist")
                    break

                if isinstance(query.tokens[j], sqlparse.sql.IdentifierList):
                    for identifier in query.tokens[j].get_identifiers():
                        if not isinstance(identifier, sqlparse.sql.Identifier):
                            print_error(identifier, "Invalid table name")
                        if not valid_table(identifier.get_name()):
                            print_error(identifier.value,
                                        "Table does not exist")
                        table_names.append(identifier.get_name())
                        table_columns = schema[identifier.get_name()]
                        available_columns = available_columns + table_columns
                    break

                if query.tokens[j].ttype == sqlparse.tokens.Wildcard:
                    print_error(query.tokens[j].value,
                                "Wildcard not allowed in FROM clause")
                continue
            break


# ----------------------------------------------------------------------------------------------------------------------
    # require query, table_names, available_columns, and schema

    print("table_names: {}".format(table_names))
    print("columns: {}".format(available_columns))
    if available_columns == []:
        print_error(query, "No columns available")
    data = join(table_names)

    distinct = False
    for token in query.tokens:
        if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == "distinct":
            distinct = True

    print("tokens: {}".format(query.tokens))

    group_by = []
    for i, token in enumerate(query.tokens):
        if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == "group by":
            for j in range(i+1, len(query.tokens)):
                if query.tokens[j].ttype == sqlparse.tokens.Keyword:
                    print_error(query.tokens[j].value,
                                "Keyword not allowed in GROUP BY clause")

                if isinstance(query.tokens[j], sqlparse.sql.IdentifierList):
                    for identifier in query.tokens[j].get_identifiers():
                        # if identifier is neither  Identifier nor Function exit
                        if not (isinstance(identifier.value, sqlparse.sql.Function) or isinstance(identifier, sqlparse.sql.Identifier)):
                            print_error(identifier, "Invalid column name")

                        table, column, agg, agg_tag = parse_column(identifier)
                        if column is None:
                            print_error(identifier.value,
                                        "Invalid column name")
                        group_by.append((table, column, agg, agg_tag))
                    break

                if isinstance(query.tokens[j], sqlparse.sql.Identifier) or isinstance(query.tokens[j], sqlparse.sql.Function):
                    table, column, agg, agg_tag = parse_column(query.tokens[j])
                    if column is None:
                        print_error(query.tokens[j].value,
                                    "Invalid column name")
                    group_by.append((table, column, agg, agg_tag))
                    break

                if query.tokens[j].ttype == sqlparse.tokens.Wildcard:
                    print_error(query.tokens[j].value,
                                "Wildcard not allowed in GROUP BY clause")
                continue
            break
    print("group_by: {}".format(group_by))

# ----------------------------------------------------------------------------------------------------------------------

    required_columns = []
    for i, token in enumerate(query.tokens):
        if token.ttype == sqlparse.tokens.DML:
            for j in range(i+1, len(query.tokens)):
                print("token: {}".format(query.tokens[j]), j)
                if query.tokens[j].ttype == sqlparse.tokens.Keyword and query.tokens[j].value.lower() == "from":
                    print_error(query.tokens[j].value,
                                "No columns provided for select")

                if isinstance(query.tokens[j], sqlparse.sql.Identifier) or isinstance(query.tokens[j], sqlparse.sql.Function):
                    # TODO handle aggregation(*) case
                    print("Identifier: {}".format(query.tokens[j]))
                    _, column, _, _ = parse_column(query.tokens[j])
                    print("column: {}".format(column))
                    if column is None:
                        print_error(query.tokens[j].value,
                                    "Invalid column name")
                    required_columns.append(parse_column(query.tokens[j]))
                    break

                if isinstance(query.tokens[j], sqlparse.sql.IdentifierList):
                    for identifier in query.tokens[j].get_identifiers():
                        # if identifier is not an sql.Identifier, throw error
                        if not (isinstance(identifier, sqlparse.sql.Identifier) or isinstance(identifier, sqlparse.sql.Function)):
                            print_error(identifier.value,
                                        "Invalid column name")
                        required_columns.append(parse_column(identifier))
                    break

                if query.tokens[j].ttype == sqlparse.tokens.Wildcard:
                    required_columns = []
                    for col in available_columns:
                        required_columns.append(parse_column(col))
                    break

                if query.tokens[j].ttype == sqlparse.tokens.Keyword and query.tokens[j].value.lower() == "distinct":
                    continue

                # TODO check if left anything
                # https://github.com/swetanjal/Mini-SQL-Engine/blob/master/main.py#L304

            break
    print("required_columns: {}".format(required_columns))

    if group_by != []:
        group_by_cols = [col[1] for col in group_by]
        # TODO handle agg(*) case
        for col in required_columns:
            if col not in group_by_cols and col[3] == None:
                # TODO is this true?
                print_error(
                    col[1], "Column without aggregation in query with GROUP BY clause is not allowed")

    aggregation = False
    no_aggregation = False
    for col in required_columns:
        if col[3] is not None:
            aggregation = True
        else:
            no_aggregation = True
    if aggregation and no_aggregation:
        print_error(
            query, "Aggregation and non-aggregation columns in same query")

    def filter(x): return True
    for token in query.tokens:
        if isinstance(token, sqlparse.sql.Where):
            token = str(token)
            if token.find(" and ") != -1 and token.find(" or ") != -1:
                print_error(token, "AND and OR not allowed in WHERE clause")
            if token.find(" and ") != -1:
                tokens = token.split(" and ")
                exp1 = parse_expression(tokens[0])
                exp2 = parse_expression(tokens[1])
                exp1 = modify_columns(exp1, available_columns)
                exp2 = modify_columns(exp2, available_columns)

                def filter(x): return exp1(x) and exp2(x)
            elif token.find(" or ") != -1:
                tokens = token.split(" or ")
                exp1 = parse_expression(tokens[0])
                exp2 = parse_expression(tokens[1])
                exp1 = modify_columns(exp1, available_columns)
                exp2 = modify_columns(exp2, available_columns)

                def filter(x): return exp1(x) or exp2(x)
            else:
                exp = parse_expression(token)
                exp = modify_columns(exp, available_columns)

                def filter(x): return exp(x)
            break

    filtered_data = []
    for row in data:
        if filter(row):
            filtered_data.append(row)

    print("filtered_data:")
    for f in filtered_data:
        print(f)

    for i, token in enumerate(query.tokens):
        if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == "order by":
            for j in range(i+1, len(query.tokens)):
                if query.tokens[j].ttype == sqlparse.tokens.Keyword:
                    print_error(
                        query.tokens[j].value, "Expected column name for ORDER BY clause")
                if isinstance(query.tokens[j], sqlparse.sql.Identifier):
                    key, order = parse_order_by(
                        query.tokens[j], available_columns)
                    filtered_data.sort(key=key, reverse=order)
                    break

    # print("ordered filtered_data:")
    # for f in filtered_data:
    #     print(f)

    if group_by != []:
        tables = {}
        keys = []
        for row in filtered_data:
            key = []
            for col in group_by:
                idx = 0
                for i, c in enumerate(available_columns):
                    if c == col[1]:
                        idx = i
                        break
                key.append(row[idx])
            key = tuple(key)
            if key not in tables:
                tables[key] = []
            tables[key].append(row)
            keys.append(key)

        for col in required_columns:
            # print("table: {}".format(tables), end="\n\n")
            # print("\n col: {}".format(col), end="\n\n")
            for key in tables.keys():
                tables[key] = col[2](tables[key])
            # print()
            # print("table: {}".format(tables), end="\n\n")

        selected_data = []
        for key in keys:
            print("table[key]: {}".format(tables[key]))
            selected_data.append([tables[key]])
        print(len(selected_data))
        # exit()
    else:
        selected_data = []
        for data in filtered_data:
            temp = []
            for column in required_columns:
                temp.append(data[available_columns.index(column[1])])
            selected_data.append(temp)

        # transpose selected_data
        selected_data = list(map(list, zip(*selected_data)))

        # aggregate selected_data
        for i, col in enumerate(required_columns):
            selected_data[i] = col[2](selected_data[i])

        selected_data = list(map(list, zip(*selected_data)))
        selected_data = [tuple(row) for row in selected_data]

    print("selected_data:", selected_data)
    for s in selected_data:
        for d in s:
            print(d, "\t\t", end="")
        print()

    if distinct:
        final_data = []
        for row in selected_data:
            if row not in final_data:
                final_data.append(row)
        selected_data = final_data

    # transpose selected_data
    return [col[0]+"."+col[1] for col in required_columns], selected_data


def print_output(columns, output):
    for column in columns:
        print(column, "\t", end="")
    print()
    for data in output:
        for i in range(len(data)):
            if i != len(data)-1:
                print(data[i], "\t\t", end="")
            else:
                print(data[i])


if __name__ == '__main__':
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

    query = sqlparse.format(query, strip_comments=True)
    query = sqlparse.parse(query)[0]

    # check if query is SELECT
    if query.get_type() != 'SELECT':
        print("Query must be SELECT")
        exit(0)

    # load metadata
    load_metadata("../files/metadata.txt")
    print("\nSchema: {}".format(schema), end="\n\n")

    columns, output = execute(query)

    print("\nOutput: {}".format(output), end="\n\n")

    print_output(columns, output)
