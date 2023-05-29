import string


def name(s):
    if type(s) is not str:
        raise TypeError(f"A name must be a string; {s.__repr__()} is not! ")
    invalid_chars = set(s) - set(string.digits + string.ascii_lowercase + "_")
    if (len(invalid_chars) or (s[0] in string.digits)):
        raise ValueError(f"The value {s.__repr__()} is not a valid name! ")
    return s
def datatype(t):
    return {
        int: 'INTEGER',
        str: 'TEXT',
        float: 'REAL',
    }[t]

