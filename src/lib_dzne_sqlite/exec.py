import sys as _sys

import lib_dzne_sqlite.fmt as _fmt
import pandas as _pd

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
    command += _fmt.name(table)
    command += "("
    command += ", ".join(_fmt.name(k) for k in row.keys())
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
    command += _fmt.name(table)
    command += " SET "
    command += ", ".join(f"{_fmt.name(k)} = ?" for k in row.keys())
    command += " "
    command += where.command
    command += ";"
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
        command += ", ".join(_fmt.name(c) for c in columns)
    command += " FROM "
    command += _fmt.name(table)
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
    return _pd.DataFrame(data=cursor.fetchall(), columns=columns)







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








