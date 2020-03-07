from .objects import *


class Dispatcher:

    def get_handler(self, command):
        if command.startswith('get'):
            command, alias = command.rsplit(' as ', 1)
        else:
            alias = None
        func, args, kwargs = eval(command, {}, self)

        def handler(q):
            if alias is not None:
                return func(q, *args, alias=alias, **kwargs)
            else:
                return func(q, *args, **kwargs)

        return handler

    def __getitem__(self, item):
        if item in globals():
            handler = globals()[item]

            def func(*args, **kwargs):
                return handler, args, kwargs
            return func
        else:
            return item


dispatcher = Dispatcher()


def get_real_trader_id(
    query,
    original_field='GTP_ID',
    join_field='REAL_TRADER_ID',
    date_field='TARGET_DATE',
    filter_trader_type=100,
    filter_dpg_type=None,
    filter_impex=None,
    addition_filters=None,
    alias=None
):
    # TODO автоматическое определение gtp_id и date
    # TODO автоматическое определение alias

    n = len(query.tables) + 1
    t = Table('ODS_001.TRADER',
              alias=f't{n}',
              join_condition=f"\tINNER JOIN ODS_001.TRADER t{n} "
                             f"ON t1.{original_field} = t{n}.{join_field} "
                             f"AND {date_field} between t{n}.BEGIN_DATE and t{n}.END_DATE"
              )
    query.tables.append(t)
    if alias is not None:
        a = Attribute(alias, f"t{n}.REAL_TRADER_ID", True)
        query.attributes.insert(0, a)

    query.filters.append(f't{n}.TRADER_TYPE = {filter_trader_type}')
    if filter_dpg_type is not None:
        query.filters.append(f't{n}.DPG_TYPE = {filter_dpg_type}')
    if filter_impex is not None:
        query.filters.append(f't{n}.IS_IMPEX = {filter_impex}')
    if addition_filters is not None:
        query.filters.append(
            insert_alias(addition_filters, [t, ])
        )


def get_target_date(query, addition_filters=None, alias=None):
    n = len(query.tables) + 1
    t = Table('ODS_002.TRADE_SESSION',
              alias=f't{n}',
              join_condition=f"\tINNER JOIN ODS_002.TRADE_SESSION t{n} "
                             f"ON t1.TRADE_SESSION_ID = t{n}.TRADE_SESSION_ID "
                             f"AND t{n}.VALID_TO_DTTM = '5999-12-31'"
              )
    query.tables.append(t)
    if alias is not None:
        a = Attribute(alias, f"t{n}.TARGET_DATE", True)
        query.attributes.insert(0, a)
    if addition_filters is not None:
        query.filters.append(
            insert_alias(addition_filters, [t, ])
        )


def group(query, by=None):
    if by is None:
        by = [
            a.name
            for a in query.attributes
            if a.is_pk
        ]
    query.group = by
