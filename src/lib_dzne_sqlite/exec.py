import sys

import pandas as pd

from . import cmd, fmt
from .Where import Where


def _cursor_decorator(oldfunc):
    def newfunc(*, cursor, **kwargs):
        command, values = oldfunc(**kwargs)
        _main(
            cursor=cursor,
            command=command,
            values=values,
        )
    return newfunc



def _where_decorator(oldfunc):
    def newfunc(*, where, **kwargs):
        where = Where(row=where)
        return oldfunc(where=where, **kwargs)
    return newfunc




def _main(*, cursor, command, values):
    command = str(command)
    values = list(values)
    try:
        cursor.execute(command, values)
    except:
        raise Exception(f"cursor.execute({command.__repr__()}, {values}) failed! ")    





def insert(*, cursor, table, row):
    row = dict(row)
    command = "INSERT INTO "
    command += fmt.name(table)
    command += "("
    command += ", ".join(fmt.name(k) for k in row.keys())
    command += ")"
    command += " VALUES "
    command += "("
    command += ", ".join('?' for v in row.values())
    command += ");"
    _main(
        cursor=cursor,
        command=command,
        values=row.values(),
    )




@_where_decorator
def update(*, cursor, table, row, where):
    #where = dict(where)
    row = dict(row)
    command = "UPDATE "
    command += fmt.name(table)
    command += " SET "
    command += ", ".join(f"{fmt.name(k)} = ?" for k in row.keys())
    command += " "
    command += where.command
    command += ";"
    #print('table', table, file=sys.stderr)
    #print('row', row, file=sys.stderr)
    #print('where', where, file=sys.stderr)
    #print(command, list(row.values()) + where.values, file=sys.stderr)
    #print(file=sys.stderr)
    _main(
        cursor=cursor,
        command=command,
        values=list(row.values()) + where.values,
    )




@_where_decorator
def select(*, cursor, table, columns, where={}):
    columns = list(columns)
    if len(columns) == 0:
        raise ValueError("The columns-list is not allowed to be empty! ")
    command = "SELECT "
    if '*' in columns:
        if len(columns) != 1:
            raise ValueError("The columns-list is invalid! ")
        command += '*'
    else:
        command += ", ".join(fmt.name(c) for c in columns)
    command += " FROM "
    command += fmt.name(table)
    command += " "
    command += where.command#cmd.where(where.keys())
    command += ";"
    _main(
        cursor=cursor,
        command=command,
        values=where.values,
    )
    if '*' in columns:
        columns = [descriptor[0] for descriptor in cursor.description]
    return pd.DataFrame(data=cursor.fetchall(), columns=columns)







def all_tables(*, cursor):
    df_master = select(
        cursor=cursor,
        table='sqlite_master',
        columns=['name'],
        where={'type':'table'},
    )
    ans = dict()
    for table in df_master['name']:
        ans[table] = select(
            cursor=cursor,
            table=table,
            columns=['*'],
            where={},
        )
    return ans








