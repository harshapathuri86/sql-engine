import sqlparse
import sys
import re


def load_table(table):
    arr = []
    f = open('../files/' + table + '.csv')
    lines = f.readlines()
    for line in lines:
        toks = line.split(',')
        tmp = []
        for t in toks:
            try:
                tmp.append(int(t))
            except:
                t = t.strip()
                tmp.append(int(t[1:len(t)-1]))
        arr.append(tmp)
    return arr


def perform_join(tables):
    curr = []
    for i, table in enumerate(tables):
        if i == 0:
            curr = load_table(table)
            continue
        data = load_table(table)
        new_curr = []
        for left_element in curr:
            for right_element in data:
                new_curr.append(left_element + right_element)
        curr = new_curr
    return curr


def get_columns(table):
    f = open('../files/`metadata`.txt')
    lines = f.readlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if line == '<begin_table>':
            table_name = lines[i + 1].strip().lower()
            cols = []
            j = i + 2
            while lines[j].strip() != '<end_table>':
                cols.append(lines[j].strip().lower())
                j = j + 1
            if table_name == table:
                return cols
    return None


def get_table(col):
    f = open('../files/metadata.txt')
    lines = f.readlines()
    for i, line in enumerate(lines):
        line = line.strip()
        if line == '<begin_table>':
            table_name = lines[i + 1].strip().lower()
            j = i + 2
            while lines[j].strip() != '<end_table>':
                if lines[j].strip().lower() == col:
                    return table_name
                j = j + 1


def parse_column(s):
    global is_aggregate
    global is_non_aggregate
    s = str(s)
    find_max = re.compile("max\((.*)\)")
    find_min = re.compile("min\((.*)\)")
    find_mean = re.compile("average\((.*)\)")
    find_sum = re.compile("sum\((.*)\)")
    find_count = re.compile("count\((.*)\)")
    if find_max.match(s) is not None:
        is_aggregate = True
        return (find_max.match(s).groups()[0], lambda s: max(s), 'max')
    if find_min.match(s) is not None:
        is_aggregate = True
        return (find_min.match(s).groups()[0], lambda s: min(s), 'min')
    if find_mean.match(s) is not None:
        is_aggregate = True
        return (find_mean.match(s).groups()[0], lambda s: sum(s) * 1.0 / len(s), 'mean')
    if find_sum.match(s) is not None:
        is_aggregate = True
        return (find_sum.match(s).groups()[0], lambda s: sum(s), 'sum')
    if find_count.match(s) is not None:
        is_aggregate = True
        return (find_count.match(s).groups()[0], lambda s: len(s), 'count')
    is_non_aggregate = True
    return (s, lambda s: s, 'none')


def parse_expression(s):
    s = re.sub(r'where ', '', s)
    eq = re.compile("(.*) = (.*)")
    lt = re.compile("(.*) < (.*)")
    gt = re.compile("(.*) > (.*)")
    le = re.compile("(.*) <= (.*)")
    ge = re.compile("(.*) >= (.*)")
    if eq.match(s) is not None and len(eq.match(s).groups()) == 2:
        try:
            p = int(eq.match(s).groups()[1].strip())
            return (eq.match(s).groups()[0].strip(), lambda lhs: lhs == p, None)
        except:
            return (eq.match(s).groups()[0].strip(), lambda lhs, rhs: lhs == rhs, eq.match(s).groups()[1].strip())

    if lt.match(s) is not None and len(lt.match(s).groups()) == 2:
        try:
            p = int(lt.match(s).groups()[1].strip())
            return (lt.match(s).groups()[0].strip(), lambda lhs: lhs < p, None)
        except:
            return (lt.match(s).groups()[0].strip(), lambda lhs, rhs: lhs < rhs, lt.match(s).groups()[1].strip())
    if gt.match(s) is not None and len(gt.match(s).groups()) == 2:
        try:
            p = int(gt.match(s).groups()[1].strip())
            return (gt.match(s).groups()[0].strip(), lambda lhs: lhs > p, None)
        except:
            return (gt.match(s).groups()[0].strip(), lambda lhs, rhs: lhs > rhs, gt.match(s).groups()[1].strip())
    if le.match(s) is not None and len(le.match(s).groups()) == 2:
        try:
            p = int(le.match(s).groups()[1].strip())
            return (le.match(s).groups()[0].strip(), lambda lhs: lhs <= p, None)
        except:
            return (le.match(s).groups()[0].strip(), lambda lhs, rhs: lhs <= rhs, le.match(s).groups()[1].strip())
    if ge.match(s) is not None and len(ge.match(s).groups()) == 2:
        try:
            p = int(ge.match(s).groups()[1].strip())
            return (ge.match(s).groups()[0].strip(), lambda lhs: lhs >= p, None)
        except:
            return (ge.match(s).groups()[0].strip(), lambda lhs, rhs: lhs >= rhs, ge.match(s).groups()[1].strip())

    print("Syntax Error: Invalid expression", s)
    exit(0)


def transform_columns(exp, available_columns):
    idx = None
    if exp[2] != None:
        for i, v in enumerate(available_columns):
            if exp[2] == v:
                idx = i
        if idx == None:
            print("Semantic Error: Column does not exist!")
            exit(0)
    for i, v in enumerate(available_columns):
        if exp[0] == v and exp[2] == None:
            return (lambda s: exp[1](s[i]))
        elif exp[0] == v and exp[2] != None:
            return (lambda s: exp[1](s[i], s[idx]))
    print("Semantic Error: Column does not exist!")
    exit(0)


def parse_order_by(s, available_columns):
    tokens = s.split()
    if len(tokens) == 2 and tokens[1].upper() == 'ASC':
        for i, v in enumerate(available_columns):
            if tokens[0] == v:
                return ((lambda s: s[i]), False)
        print("Semantic Error: Column doesn't exist!")
        exit(0)
    if len(tokens) == 2 and tokens[1].upper() == 'DESC':
        for i, v in enumerate(available_columns):
            if tokens[0] == v:
                return ((lambda s: s[i]), True)
        print("Semantic Error: Column doesn't exist!")
        exit(0)
    print("Syntax Error: Expected column name followed by ASC|DESC")
    exit(0)


def select_distinct_data(data):
    DICT = {}
    res = []
    for d in data:
        if d in DICT.keys():
            continue
        res.append(d)
        DICT[d] = 1
    return res


if len(sys.argv) != 2:
    print("Incorrect Usage! Expected Usage: python3 main.py query")
    exit(0)
query = sys.argv[1].lower()

query = query.strip()
if(query[-1] != ";"):
    print("Syntax Error: Need to terminate statement with a semi-colon!")
    exit(0)

query = query[0: len(query) - 1]
query_tokens = sqlparse.parse(query)[0]

if query_tokens.get_type() == 'UNKNOWN':
    print("Query Syntax Error")
    exit(0)
elif query_tokens.get_type() == 'UPDATE':
    print("Query not supported")
    exit(0)
elif query_tokens.get_type() == 'CREATE':
    print("Query not supported")
    exit(0)
elif query_tokens.get_type() == 'DELETE':
    print("Query not supported")
    exit(0)


# Parse for valid table
available_columns = []
corresponding_tables = []
data = []
for i, token in enumerate(query_tokens.tokens):
    if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
        # Look for first wildcard/Identifier/IdentifierList
        for j in range(i + 1, len(query_tokens.tokens)):
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword:
                print("Syntax error: No columns to select from!")
                exit(0)
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                tb_name = query_tokens.tokens[j].get_name()
                available_columns = get_columns(tb_name)
                if available_columns == None:
                    print("Semantic Error: Invalid table name", tb_name)
                    exit(0)
                data = perform_join([tb_name])
                break
            if isinstance(query_tokens.tokens[j], sqlparse.sql.IdentifierList):
                tb_names = []
                for identifier in query_tokens.tokens[j].get_identifiers():
                    tb_names.append(identifier.get_name())
                    add_cols = get_columns(identifier.get_name())
                    if add_cols == None:
                        print("Semantic Error: Invalid table name",
                              (identifier.get_name()))
                        exit(0)
                    available_columns = available_columns + add_cols
                data = perform_join(tb_names)
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Wildcard:
                print("Semantic Error: Expected Table name but found wildcard!")
                exit(0)
            continue
        break

if available_columns == []:
    print("Syntax error: No table to select from!")
    exit(0)

# Parse for Distinct Selector
distinct_selector = False
for token in query_tokens.tokens:
    if token.ttype == sqlparse.tokens.Keyword and token.value.upper() == 'DISTINCT':
        distinct_selector = True

# Parse for Group By
group_by = None
for i, token in enumerate(query_tokens.tokens):
    if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == 'group by':
        for j in range(i + 1, len(query_tokens.tokens)):
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword:
                print("Syntax Error: Expected column name")
                exit(0)
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                group_by = str(query_tokens.tokens[j])
                break
        break

# Parse for columns
columns = []
is_aggregate = False
is_non_aggregate = False
for i, token in enumerate(query_tokens.tokens):

    if token.ttype == sqlparse.tokens.DML:
        # Look for first wildcard/Identifier/IdentifierList
        for j in range(i + 1, len(query_tokens.tokens)):
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword and query_tokens.tokens[j].value.upper() == 'FROM':
                print("Syntax error: No columns to select!")
                exit(0)
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                column_name, aggregate, tag = parse_column(
                    query_tokens.tokens[j])
                if not (column_name in available_columns):
                    print("Semantic Error: Invalid column name", column_name)
                    exit(0)
                columns.append([column_name, aggregate, tag])
                break
            if isinstance(query_tokens.tokens[j], sqlparse.sql.IdentifierList):
                for identifier in query_tokens.tokens[j].get_identifiers():
                    column_name, aggregate, tag = parse_column(identifier)

                    if not (column_name in available_columns):
                        print("Semantic Error: Invalid column name", column_name)
                        exit(0)
                    columns.append([column_name, aggregate, tag])
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Wildcard:
                tmp = []
                for a in available_columns:
                    tmp.append([a, lambda s: s, 'none'])
                columns = tmp
                break
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword and query_tokens.tokens[j].value.upper() == 'DISTINCT':
                continue

            if query_tokens.tokens[j].ttype == sqlparse.tokens.Text.Whitespace:
                continue

            column_name, aggregate, tag = parse_column(query_tokens.tokens[j])
            if not (column_name in available_columns):
                print("Semantic Error: Invalid column name", column_name)
                exit(0)
            columns.append([column_name, aggregate, tag])
            break
        break

if group_by != None:
    # Check if some other column other than group_by is 'none', then syntax error
    for c in columns:
        if c[2] == 'none' and group_by != c[0]:
            print(
                "Semantic Error: Cannot combine aggregate with non aggregate projection!")
            exit(0)


if is_aggregate and is_non_aggregate:
    # Check non aggregate is same as group by column. Only then this case is applicable.
    for c in columns:
        if c[2] == 'none' and group_by != c[0]:
            print(
                "Semantic Error: Cannot combine aggregate with non aggregate projection!")
            exit(0)


def filt(s): return True


for token in query_tokens.tokens:
    if isinstance(token, sqlparse.sql.Where):
        token = str(token)
        if token.find(" and ") != -1:
            tokens = token.split(" and ")
            exp1 = parse_expression(tokens[0])
            exp2 = parse_expression(tokens[1])
            exp1 = transform_columns(exp1, available_columns)
            exp2 = transform_columns(exp2, available_columns)
            def filt(s): return exp1(s) and exp2(s)
        elif token.find(" or ") != -1:
            tokens = token.split(" or ")
            exp1 = parse_expression(tokens[0])
            exp2 = parse_expression(tokens[1])
            exp1 = transform_columns(exp1, available_columns)
            exp2 = transform_columns(exp2, available_columns)
            def filt(s): return exp1(s) or exp2(s)
        else:
            exp1 = parse_expression(token)
            exp1 = transform_columns(exp1, available_columns)
            def filt(s): return exp1(s)
        break

filtered_data = []
for row in data:
    if filt(row):
        filtered_data.append(row)


for i, token in enumerate(query_tokens.tokens):
    if token.ttype == sqlparse.tokens.Keyword and token.value.lower() == 'order by':
        for j in range(i + 1, len(query_tokens.tokens)):
            if query_tokens.tokens[j].ttype == sqlparse.tokens.Keyword:
                print(
                    "Syntax Error: Expected Column name after order by but found Keyword!")
                exit(0)
            if isinstance(query_tokens.tokens[j], sqlparse.sql.Identifier):
                key, reverse = parse_order_by(
                    str(query_tokens.tokens[j]), available_columns)
                filtered_data.sort(key=key, reverse=reverse)
                break

for i in range(len(columns)):
    if i != len(columns) - 1:
        if columns[i][2] == 'none':
            print(get_table(columns[i][0]) + "." + columns[i][0] + ",", end='')
        else:
            print(columns[i][2] + "(" + get_table(columns[i][0]
                                                  ) + "." + columns[i][0] + ")" + ",", end='')
    else:
        if columns[i][2] == 'none':
            print(get_table(columns[i][0]) + "." + columns[i][0])
        else:
            print(columns[i][2] + "(" + get_table(columns[i]
                  [0]) + "." + columns[i][0] + ")")


if group_by != None:
    # Follow a different path to showing results
    idx = -1
    for i, a in enumerate(available_columns):
        if a == group_by:
            idx = i
            break
    selected_data = {}

    for d in filtered_data:
        tmp = []
        for i in range(len(columns)):
            for j in range(len(available_columns)):
                if available_columns[j] == columns[i][0]:
                    tmp.append(d[j])
                    break
        if not d[idx] in selected_data.keys():
            selected_data[d[idx]] = []
        selected_data[d[idx]].append(tmp)

    DATA = []
    for k in selected_data.keys():
        tmp = []
        for i in range(len(columns)):
            tmp2 = []
            for j in range(len(selected_data[k])):
                tmp2.append(selected_data[k][j][i])
            if columns[i][0] == group_by and columns[i][2] == 'none':
                tmp.append(tmp2[0])
            elif columns[i][0] == group_by:
                tmp.append(columns[i][1](tmp2))
            else:
                tmp.append(columns[i][1](tmp2))
        DATA.append(tmp)
    selected_data = DATA
    final_data = []
    for d in selected_data:
        final_data.append(tuple(d))

    if distinct_selector:
        final_data = select_distinct_data(final_data)

    for d in final_data:
        for j in range(len(d)):
            if j != len(d) - 1:
                print(str(d[j]) + ",", end='')
            else:
                print(str(d[j]))
    exit(0)


selected_data = []

for d in filtered_data:
    tmp = []
    for i in range(len(columns)):
        for j in range(len(available_columns)):
            if available_columns[j] == columns[i][0]:
                tmp.append(d[j])
                break
    selected_data.append(tmp)


if is_aggregate:
    tmp = []
    for i in range(len(columns)):
        tmp2 = []
        for j in range(len(selected_data)):
            tmp2.append(selected_data[j][i])
        tmp.append(columns[i][1](tmp2))
    selected_data = [tmp]


final_data = []
for d in selected_data:
    final_data.append(tuple(d))

if distinct_selector:
    final_data = select_distinct_data(final_data)

for d in final_data:
    for j in range(len(d)):
        if j != len(d) - 1:
            print(str(d[j]) + ",", end='')
        else:
            print(str(d[j]))
