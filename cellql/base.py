"""
Algorithm syntax:

# - inline comments
table definition starting at beginning of line
table modification starting with padding (1 or more spaces):
    .modification_name(attributes)
    .get_some_field() as TARGET_NAME
    SOME_FLG = 1
table definitions with no operation between them are separate parts.
Parts are not combined (by default) or combined by FULL JOIN (if join_parts=True)
supported operations: LEFT JOIN, INNER JOIN, FULL JOIN, UNION, UNION ALL
Unlike SQL UNION has lower priority then JOIN (Two tables will be unioned before joined)
Algorithm may contain sql snippets:
    table1 as (
        SELECT * FROM a
    )

Attributes syntax:
for each attribute there are should be:
    attribute target name
    primary key flag
    source schema (multiple values supported, see below)
    source table name (multiple values supported, see below)
    column‑expression for select each source table. column‑expression can be multiline, in that case indend applied
schema, table and column connected to triple "by rows". All values should have the same number of rows
if schema or table have multiple values separated by coma, then cross product wil be used to get triples.

Examples:
{
    "source_schema": "s1, s2",
    "source_table": "t1, t2",
    "source_field": "a"
}
is same as
{
    "source_schema": "s1, s2\ns1, s2",
    "source_table": "t1\nt2",
    "source_field": "a\na"
}
is same as
{
    "source_schema": "s1\ns1\ns2\ns2",
    "source_table": "t1\nt2\nt1\nt2",
    "source_field": "a\na\na\na"
}
"""
from collections import defaultdict
from .transforms import dispatcher
from .objects import *
from itertools import product
import re

_re_line_continuation = re.compile(r"(\n|\r|\n\r|\r\n) {4,}")
_re_blankline = re.compile(r"(\n|\r|\n\r|\r\n) *$")
PART_SEPARATOR = Operation('SEPARATOR')
BRACKET = Operation('(')


class PartedQuery:
    def __init__(self):
        self.attributes = []  # type: List[Attribute]
        self.tables = []  # type: List[Table]
        self.filters = []
        self.group = []
        self.subqueries = []
        self.name = ''

    @property
    def sql(self):
        if self.subqueries:
            sql = 'WITH\n' + ',\n'.join(
                f"{q.name} as (\n{q.sql}\n)"
                for q in self.subqueries
            ) + '\n'
        else:
            sql = ''

        attributes = ',\n\t'.join(
            f'{insert_alias(a.formula, self.tables)} as {a.name}'
            for a in self.attributes
        )
        tables = '\n'.join(t.join_condition for t in self.tables)
        sql += f'SELECT\n\t{attributes}\nFROM\n\t{tables}'
        if self.filters:
            sql += '\nWHERE\n\t' + '\n\tAND '.join(self.filters)
        if self.group:
            sql += '\nGROUP BY ' + ', '.join(self.group)
        return sql

    def __str__(self):
        return self.name


class Query:
    def __init__(self, sql):
        self.name = None
        self.sql = sql
        self.attributes = []

    @classmethod
    def from_operations(cls, operations, operands):
        operations_type = operations[0].name
        if 'JOIN' in operations_type:
            # TODO join condition
            # TODO attribute selection instead of *
            sql = (
                'SELECT * FROM\n' +
                f'\t{operands[0].name}\n\t' +
                '\n\t'.join(
                    f'{operation.name} {subquery.name} ON a=a'
                    for operation, subquery in zip(operations, operands[1:])
                )
            )
            return cls(sql)
        elif 'UNION' in operations_type:
            attributes = next(query.attributes for query in operands if query.attributes)
            attributes_str = ', '.join(map(str, attributes))

            sql = (
                f'SELECT {attributes_str} FROM {operands[0].name} \n' +
                '\n'.join(
                    f'{operation.name}\nSELECT {attributes_str} FROM {subquery.name}'
                    for operation, subquery in zip(operations, operands[1:])
                )
            )
            query = cls(sql)
            query.attributes = attributes
            return query
        elif 'DIFFERENCE' == operations_type:
            assert len(operations) == 1
            assert len(operands) == 2

            attributes = list(sorted(
                set(map(str, operands[0].attributes))
                &
                set(map(str, operands[1].attributes))
            ))
            attributes_str = ', '.join(attributes)

            sql = '''\
SELECT {2} FROM
    (SELECT {2} FROM {0} EXCEPT SELECT {2} FROM {1}) t1
UNION ALL
SELECT {2} FROM
    (SELECT {2} FROM {1} EXCEPT SELECT {2} FROM {0}) t2'''.format(operands[0].name, operands[1].name, attributes_str)
            query = cls(sql)
            query.attributes = attributes
            return query
        else:
            raise RuntimeError()

    @classmethod
    def from_queries(cls, queries):
        if len(queries) == 1:
            return cls(queries[0].sql)
        else:
            sql = '\n'.join([
                'WITH',
                ',\n'.join(
                    f"{q.name} as (\n{q.sql}\n)" for q in queries[:-1]
                ),
                queries[-1].sql
            ])
            return cls(sql)


class Action:
    def __init__(self, command):
        self.command = command.strip()
        if self.command.startswith('.'):
            self.command = self.command.lstrip('.')
            self.type = 'Transform'
            self.func = self._transform
        else:
            self.type = 'Filter'
            self.func = self._filter

    def _filter(self, q: PartedQuery):
        q.filters.append(
            insert_alias(self.command, q.tables)
        )

    def _transform(self, q: PartedQuery):
        dispatcher.get_handler(self.command)(q)

    def modify(self, q: PartedQuery):
        self.func(q)

    def __str__(self):
        return self.type + ': ' + self.command


def _split_attributes(attributes):
    """
    Transform list of attributes in Excel-like format to internal format, divided by tables
    :param attributes: List[Dict[]]
    :return: Dict[List[Attribute]]
    """

    res = defaultdict(list)
    for a in attributes:
        schemata = _re_blankline.sub('', a['source_schema']).splitlines()
        tables = _re_blankline.sub('', a['source_table']).splitlines()
        # multiline expressions in source_field value
        fields = _re_line_continuation.sub(' ', a['source_field']).splitlines()
        if not len(schemata) == len(tables) == len(fields):
            raise ValueError()

        alias, is_pk = a['name'].upper(), a['is_pk']

        for s_list, t_list, field in zip(schemata, tables, fields):
            for schema, table in product(s_list.split(','), t_list.split(',')):
                table_name = schema.strip().upper() + '.' + table.strip().upper()
                res[table_name].append(Attribute(alias, field.upper(), is_pk))

    return res


def _parse_to_rpn(algorithm: str, attributes, blank_line_operation='SEPARATOR'):
    """
    Parsing algorithm into reverse polish notation for further compiling in sql
    :param algorithm: str: algorithm in cellSQL notation
    :param attributes: list of attributes for each table in algorithm
    :param blank_line_operation: operation to use when hit blank line
    :return:
    """
    res = []
    stack = []
    last_obj = None
    sql_snipped_flg = False

    for line in algorithm.splitlines():
        if sql_snipped_flg:
            # SQL snipped body
            if line.startswith(')'):
                sql_snipped_flg = False
            else:
                last_obj.sql += line + '\n'
            continue

        line = line.split('#', 1)[0].rstrip()
        if not line:
            continue
        elif line.startswith(' '):
            if not isinstance(last_obj, PartedQuery):
                raise SyntaxError()
            res.append(Action(line.strip()))
        elif line == '(':
            stack.append(BRACKET)
        elif line == ')':
            while stack[-1] != BRACKET:
                res.append(stack.pop())
            stack.pop()
        elif Operation.is_operation(line):
            o = Operation(line.upper())
            while stack and stack[-1] > o:
                res.append(stack.pop())

            stack.append(o)
            last_obj = o
        else:
            if last_obj is not None and not isinstance(last_obj, Operation):
                o = Operation(blank_line_operation)
                while stack and stack[-1] > o:
                    res.append(stack.pop())

                if o == PART_SEPARATOR:
                    # Если разделитель угодил в скобки, мы их "раскрываем"
                    # т.е. распространяем действие того, что до скобок, на каждую из операций
                    # FIXME не работает, если будет что-то после скобок, а не до.
                    prev_op = [o for o in reversed(stack[:-1]) if o != BRACKET]
                    res.extend(prev_op)
                    yield res
                    res = res[:len(prev_op)]
                else:
                    stack.append(o)

            if line.upper().endswith(' AS ('):
                sql_snipped_flg = True
                q = Query('')
                q.name = line[:-5]
            else:
                q = PartedQuery()
                q.name = line.replace('.', '_')
                q.tables.append(Table(line, alias='t1'))
                q.attributes = attributes[line]
            res.append(q)
            last_obj = q

    res.extend(reversed(stack))
    yield res


def _compile(rpn):
    subqueries = []
    stack = []  # stack of items (queries) to perform operations on
    operations = []  # list of similar operations
    for item in rpn:
        if isinstance(item, Action):
            q = stack[-1]
            if isinstance(q, Query):
                raise NotImplementedError()
            elif isinstance(q, PartedQuery):
                item.modify(q)
            else:
                raise SyntaxError()
        elif isinstance(item, Operation) and (not operations or operations[-1] == item):
            # accumulate equal operation to perform in one query
            operations.append(item)
        else:
            if operations:
                q = Query.from_operations(operations, stack[-len(operations) - 1:])
                q.name = 'subquery{}'.format(len(subqueries) + 1)
                stack = stack[:-len(operations) - 1]
                subqueries.append(q)
                stack.append(q)
                operations.clear()

            if isinstance(item, Operation):
                operations.append(item)
            else:
                # проверить, что будет работать вложенные WITH. Избавляться от вложенности опасно из-за
                # потенциальных коллизий имен
                subqueries.append(item)
                stack.append(item)

    if operations:
        assert len(stack) == len(operations) + 1
        q = Query.from_operations(operations, stack)
        q.name = 'subquery{}'.format(len(subqueries) + 1)
        subqueries.append(q)

    return Query.from_queries(subqueries)


def prepare(attributes, algorithm: str, join_parts=False):
    """
    SQL-generator using schematic algorithm
    :param attributes: List[Dict[name, is_pk, source_schema, source_table, source_field]]
    :param algorithm: str: algorithm in cellSQL syntax
    :param join_parts: bool: use FULL JOIN to combine parts in one query
    :return: str or list: SQL-query
    """
    attributes = _split_attributes(attributes)
    if join_parts:
        rpn = next(_parse_to_rpn(algorithm, attributes, 'FULL JOIN'))
        return _compile(rpn).sql
    else:
        rpn_gen = _parse_to_rpn(algorithm, attributes, 'SEPARATOR')
        return [
            _compile(rpn).sql
            for rpn in rpn_gen
        ]

