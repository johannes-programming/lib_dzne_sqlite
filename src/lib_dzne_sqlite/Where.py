import sys as _sys

import lib_dzne_math.na as _na
import lib_dzne_sqlite.fmt as _fmt


class Where:
    def __str__(self):
        return self.command
    @property
    def command(self):
        return self._command
    @property
    def values(self):
        return list(self._values)
    def __init__(self, /, row, *, is_not_null=[]):
        self._command = ""
        self._values = list()
        row = dict(row)
        is_not_null = list(is_not_null)
        conditions = list()
        for k, v in row.items():
            condition = _fmt.name(k)
            if type(v) in (tuple, list, set, frozenset):
                options = list(set(v))
                condition += " IN ("
                condition += ', '.join(['?'] * len(options))
                condition += ")"
                self._values += options
            elif _na.isna(v):
                condition += " IS NULL"
            else:
                condition += " = ?"
                self._values.append(v)
            conditions.append(condition)
        for k in is_not_null:
            condition = _fmt.name(k)
            condition += " IS NOT NULL"
            conditions.append(condition)
        if len(conditions) == 0:
            self._command = "WHERE 1"
        elif len(conditions) == 1:
            self._command = f"WHERE ({conditions[0]})"
        else:
            self._command = "WHERE (("
            self._command += ") AND (".join(conditions)
            self._command += "))"
            
