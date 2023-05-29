from . import fmt


def where(equals_questionmark, *, is_null=[], is_not_null=[]):
    conditions = list()
    conditions += [f"{fmt.name(x)} = ?" for x in equals_questionmark]
    conditions += [f"{fmt.name(x)} IS NULL" for x in is_null]
    conditions += [f"{fmt.name(x)} IS NOT NULL" for x in is_not_null]
    if not len(conditions):
        return "WHERE 1"
    cmd = "WHERE (("
    cmd += ") AND (".join(conditions)
    cmd += "))"
    return cmd