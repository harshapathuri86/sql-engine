import sys
import itertools
from functools import reduce
import operator
from moz_sql_parser import parse

try:
    assert len(sys.argv) == 2, "Exactly 1 argument - SQL query should be provided"
    query = sys.argv[1].strip()
    assert query[-1] == ';', "Query doesn't end with ;, invalid"

    try:
        sql_obj = parse(query)
    except:
        raise AssertionError("Invalid SQL Syntax")

    assert 'select' in sql_obj, "SELECT Missing, invalid query"
    assert 'from' in sql_obj, "FROM Missing, invalid query"

    tables = {}
    table_col_order = {}
    # Read metadata (No error handling in format of metadata, assuming correct)
    with open('metadata.txt', 'r') as f:
        x = f.readline().strip()
        while x:
            if x == '<begin_table>':
                table_name = f.readline().strip()
            x = f.readline().strip()
            attributes = []
            while x != '<end_table>':
                attributes.append(x)
                x = f.readline().strip()
            tables[table_name] = {x: [] for x in attributes}
            table_col_order[table_name] = {
                i: x for (i, x) in enumerate(attributes)}
            x = f.readline().strip()

    # Tables to read from
    if isinstance(sql_obj['from'], str):
        sql_obj['from'] = [sql_obj['from']]

    # Ignore tables not in query
    all_tables = list(tables.keys())
    for x in all_tables:
        if x not in sql_obj['from']:
            del(tables[x])

    # Load tables from csv
    for tab in sql_obj['from']:
        if tab not in tables:
            raise AssertionError("Table not found in Database")
        with open(f'{tab}.csv', 'r') as f:
            x = f.readline().strip()
            while x:
                x = x.split(',')
                x = [k.strip() for k in x]
                x = [k[1:-1] if k[0] == '\"' or k[0] == "'" else k for k in x]
                for (i, v) in enumerate(x):
                    tables[tab][table_col_order[tab][i]].append(int(v))
                x = f.readline().strip()

    columns = []
    aggregates = []
    all_columns = []
    for tab in tables:
        all_columns.extend([f"{tab}.{x}" for x in tables[tab]])

    def fmt_column(column):
        # If Column is of not of type <table_name>.<col_name> and there is no ambiguity
        # on which table it belongs to, then return appropriate one else report its ambiguous
        if len(column.split('.')) == 1:
            presence = [tab for tab in tables if column in tables[tab]]
            assert len(
                presence) != 0, f"Column {column} not found in any of the tables"
            assert len(
                presence) == 1, f"Column {column} present in multiple tables, query ambiguous"
            column = f'{presence[0]}.{column}'
        elif len(column.split('.')) == 2:
            tab = tables.get(column.split('.')[0], False)
            assert tab and column.split(
                '.')[1] in tab, f"Column {column} not found"
        else:
            raise ValueError(
                f"Column {column} invlaid syntax, more than one `.`")
        return column

    # Select all columns
    if sql_obj['select'] == '*':
        columns.extend(all_columns)
    else:
        # Misplet `distinct` will be considered as column name because of optional AS syntax, no need to handle
        if isinstance(sql_obj['select'], dict) and isinstance(sql_obj['select']['value'], dict) and 'distinct' in sql_obj['select']['value']:
            sql_obj['distinct'] = True
            sql_obj['select'] = sql_obj['select']['value']['distinct']

        if not isinstance(sql_obj['select'], list):
            sql_obj['select'] = [sql_obj['select']]

        for col_dict in sql_obj['select']:
            agg_fn = None
            if isinstance(col_dict['value'], dict):
                agg_fn = list(col_dict['value'].keys())[0]
                assert agg_fn in ['sum', 'min', 'max', 'count',
                                  'avg'], "Error, unknown aggregate function provided"
                column = col_dict['value'][agg_fn]
            else:
                column = col_dict['value']

            if column == '*':
                # Handle distinct *, aggregate(*)
                columns.extend(all_columns)
                if agg_fn:
                    aggregates.extend([(c, agg_fn) for c in all_columns])
            else:
                column = fmt_column(column)
                columns.append(column)
                if agg_fn:
                    aggregates.append((column, agg_fn))

    result = []
    from_table = {}
    for col_full in columns:
        [tab, col] = col_full.split('.')
        if tab in from_table:
            if col not in from_table[tab]:
                from_table[tab].append(col)
        else:
            from_table[tab] = [col]

    # Take into account cols in 'orderby' and 'groupby', 'where'
    additional_ones = []
    if 'where' in sql_obj:
        if list(sql_obj['where'].keys())[0] in ['and', 'or']:
            for cond in list(sql_obj['where'].values())[0]:
                op_str = list(cond.keys())[0]
                for c in cond[op_str]:
                    if isinstance(c, str):
                        additional_ones.append(fmt_column(c))
        else:
            cond = sql_obj['where']
            op_str = list(sql_obj['where'].keys())[0]
            for c in cond[op_str]:
                if isinstance(c, str):
                    additional_ones.append(fmt_column(c))

    if 'groupby' in sql_obj:
        additional_ones.append(fmt_column(sql_obj['groupby']['value']))

    if 'orderby' in sql_obj:
        if isinstance(sql_obj['orderby']['value'], dict):

            check[1] = fmt_column(check[1])
            check = (check[1], check[0])
            if not (check in aggregates):
                if not ('groupby' in sql_obj):
                    raise AssertionError(
                        "Invalid query - Order by aggregate without Group by")
                else:
                    for x in aggregates:
                        if x[0] == check[0]:
                            raise AssertionError(
                                "Invalid query - Two different aggregates for one column")
                    aggregates.append(check)
            additional_ones.append(check[0])
        else:
            additional_ones.append(fmt_column(sql_obj['orderby']['value']))
    additional_ones = [c for c in additional_ones if c not in columns]

    for col_full in additional_ones:
        [tab, col] = col_full.split('.')
        if tab in from_table:
            if col not in from_table[tab]:
                from_table[tab].append(col)
        else:
            from_table[tab] = [col]

    # Handle "Select table1.A, table1.B from table1, table2;" i.e, we get multiple duplicates
    # due to the property of cartesian product
    mult_factor = 1
    for name, value in tables.items():
        if name not in from_table:
            mult_factor *= len(value[list(value.keys())[0]])

    headers = []
    to_product = []
    for tab, cols in from_table.items():
        headers.extend([f'{tab}.{c}' for c in cols])
        to_product.append(list(zip(*[tables[tab][col] for col in cols])))

    tupled = list(itertools.product(*to_product))
    rows = []
    for x in tupled:
        rows.append(list(reduce(lambda t1, t2: t1 + t2, x)))

    def get_tup_index(col):
        col = fmt_column(col)
        return headers.index(col)

    # Apply condition if Where clause present
    if 'where' in sql_obj:
        ops_map = {
            'lt': operator.lt, 'gt': operator.gt,
            'lte': operator.le, 'gte': operator.ge,
            'eq': operator.eq, 'neq': operator.ne
        }

        def cond_handler(row, conditions, final_op):
            status = []
            for cond in conditions:
                op_str = list(cond.keys())[0]
                if not (isinstance(cond[op_str][0], str) or isinstance(cond[op_str][1], str)):
                    status.append(ops_map[op_str](*cond[op_str]))
                elif isinstance(cond[op_str][0], str) and isinstance(cond[op_str][1], str):
                    one = get_tup_index(cond[op_str][0])
                    two = get_tup_index(cond[op_str][1])
                    status.append(ops_map[op_str](row[one], row[two]))
                elif isinstance(cond[op_str][0], str):
                    one = get_tup_index(cond[op_str][0])
                    status.append(ops_map[op_str](row[one], cond[op_str][1]))
                elif isinstance(cond[op_str][1], str):
                    two = get_tup_index(cond[op_str][1])
                    status.append(ops_map[op_str](cond[op_str][0], row[two]))
            if final_op:
                res = status[0]
                for i in range(1, len(status)):
                    if final_op == 'or':
                        res = res or status[i]
                    else:
                        res = res and status[i]
                return res
            else:
                return status[0]

        conditions = []
        final_op = None
        if list(sql_obj['where'].keys())[0] in ['and', 'or']:
            conditions.extend(list(sql_obj['where'].values())[0])
            final_op = list(sql_obj['where'].keys())[0]
        else:
            conditions.append(sql_obj['where'])
        rows = [keep for keep in rows if cond_handler(
            keep, conditions, final_op)]

    # Handle multiplication factor for cartesian product
    new_rows = []
    for _ in range(mult_factor):
        new_rows.extend(rows)
    rows = new_rows

    agg_ops = {
        'sum': [lambda a, b: a+b, 0],
        'count': [lambda a, b: a+1, 0],
        'avg': [lambda a, b: ((a[0]+b), a[1]+1), (0, 0)],
        'max': [lambda a, b: max(a, b), float('-inf')], 'min': [lambda a, b: min(a, b), float('inf')]
    }
    aggregate_map = {get_tup_index(
        col): agg_ops[agg_fn] for col, agg_fn in aggregates}

    # Group results by column if clause present else handle aggregation
    if 'groupby' in sql_obj:
        idx = get_tup_index(sql_obj['groupby']['value'])
        store = {}
        for row in rows:
            if row[idx] in store:
                store[row[idx]].append(row)
            else:
                store[row[idx]] = [row]
        if len(aggregates) != 0:
            new_rows = []
            for num, row_list in store.items():
                modded = list(zip(*row_list))
                new_entry = []
                for i, elems in enumerate(modded):
                    if i == idx:
                        new_entry.append(num)
                        continue
                    res = reduce(aggregate_map[i][0],
                                 elems, aggregate_map[i][1])
                    if isinstance(res, tuple):
                        res = res[0]/res[1]
                    new_entry.append(res)
                new_rows.append(new_entry)
            rows = new_rows
        else:
            rows = rows = sorted(rows, key=lambda x: x[idx])
            sql_obj['distinct'] = True
    elif len(aggregates) != 0:
        modded = list(zip(*rows))
        rows = [[]]
        for i, elems in enumerate(modded):
            res = reduce(aggregate_map[i][0], elems, aggregate_map[i][1])
            if isinstance(res, tuple):
                res = res[0]/res[1]
            rows[0].append(res)

    # Sort results by order if clause present
    if 'orderby' in sql_obj:
        desc = True if sql_obj['orderby'].get(
            'sort', False) == 'desc' else False
        if isinstance(sql_obj['orderby']['value'], dict):
            check = list(list(sql_obj['orderby']['value'].items())[0])
            check[1] = fmt_column(check[1])
            check = (check[1], check[0])
            assert check in aggregates, "Invalid column provided for order by"
            sql_obj['orderby']['value'] = check[0]
        order_col = fmt_column(sql_obj['orderby']['value'])
        rows = sorted(
            rows, key=lambda x: x[get_tup_index(order_col)], reverse=desc)

    keep_bool = [True for _ in range(len(headers))]
    for col_full in additional_ones:
        keep_bool[get_tup_index(col_full)] = False

    for col, agg_fn in aggregates:
        idx = get_tup_index(col)
        headers[idx] = f"{agg_fn}({headers[idx]})"

    headers = list(itertools.compress(headers, keep_bool))
    rows = [itertools.compress(r, keep_bool) for r in rows]

    # Distinct results
    if sql_obj.get('distinct', False):
        rows = [list(x) for x in set(tuple(r) for r in rows)]

    print(','.join(headers))
    for r in rows:
        print(','.join([str(x) for x in r]))
    print("length:", len(rows))

except AssertionError as err:
    print(err)

except KeyError as err:
    print(f"Invalid aggregate or group by operation")

except Exception as err:
    print("Error, invalid query")
