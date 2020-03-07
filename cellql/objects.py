import re

__all__ = ['Attribute', 'Table', 'Operation', 'insert_alias']


class Attribute:
    def __init__(self, name, formula, is_pk):
        self.name = name
        self.formula = formula
        self.is_pk = is_pk

    def __str__(self):
        return self.name


class Table:
    def __init__(self, name, alias, join_condition=None):
        self.name = name
        self.alias = alias
        self.join_condition = join_condition or f'{name} {alias}'


class Operation:
    _priority = {'INNER JOIN': 4, 'LEFT JOIN': 4, 'FULL JOIN': 4, 'UNION': 3, 'UNION ALL': 3, 'DIFFERENCE': 2,
                 '(': 0, 'SEPARATOR': 1}

    def __init__(self, name):
        if name.startswith('INNER JOIN ON') or name.startswith('LEFT JOIN ON'):
            self.name, self.options = map(str.strip, name.split('ON', 1))
        else:
            self.name = name
            self.options = ''

    @classmethod
    def is_operation(cls, name: str):
        name = name.upper()
        return name in cls._priority or name.startswith('INNER JOIN') or name.startswith('LEFT JOIN')

    def __gt__(self, other):
        return self._priority[self.name] > self._priority[other.name]

    def __eq__(self, other):
        return self._priority[self.name] == self._priority[other.name]

    def __str__(self):
        return self.name


_re_table_attribute = re.compile(r"(?:(?P<table>[a-zA-Z][\w.]*)\s*\.\s*)?(?P<name>[a-zA-Z]\w*(?:\s*\()?)")
SQL_KEYWORDS = {'CASE', 'WHEN', 'THEN', 'END', 'ELSE', 'NULL', 'AND', 'OR',
                'BETWEEN', 'IN', 'IS', 'LIKE', 'NOT', 'WHERE', 'AS', 'INTEGER'}


def insert_alias(formula, tables):
    """
    Вставка alias таблиц в формулу, содержащую поля в виде ATTRIBUTE_NAME или TABLE_NAME.ATTRIBUTE_NAME на равне с
    ключевыми словами sql.
    При этом атрибут без указания таблицы считается принадлежащим первой таблице в списке tables
    :param formula: sql-expression
    :param tables: List['TABLE_NAME alias']
    :return: new formula
    """
    default = tables[0].alias
    tables = {
        (t.name.split('.', 1)[1] if '.' in t.name else t.name): t.alias
        for t in tables
    }
    tables[None] = default

    def replace(m):
        table, name = m.groups()
        if name.upper() in SQL_KEYWORDS or name.endswith('('):
            return name
        else:
            return f'{tables.get(table, table)}.{name}'

    formula = _re_table_attribute.sub(replace, formula)
    return formula
