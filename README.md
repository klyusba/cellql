# cellql
Cell based query language

# Algorithm syntax

`#` - inline comments
- table definition starting at beginning of line
- table modification starting with padding (1 or more spaces):
```
    .modification_name(attributes)
    .get_some_field() as TARGET_NAME
    SOME_FLG = 1
```
- table definitions with no operation between them are separate parts.
- parts are not combined (by default) or combined by FULL JOIN (if join_parts=True)
- supported operations: LEFT JOIN, INNER JOIN, FULL JOIN, UNION, UNION ALL
Unlike SQL UNION has lower priority then JOIN (Two tables will be unioned before joined)

Algorithm may contain sql snippets:

    ```
    table1 as (
        SELECT * FROM a
    )
    ```

# Attributes syntax
for each attribute there are should be:
- attribute target name
- primary key flag
- source schema (multiple values supported, see below)
- source table name (multiple values supported, see below)
- column‑expression for select each source table. column‑expression can be multiline, in that case indend applied

Schema, table and column connected to triple "by rows". All values should have the same number of rows
if schema or table have multiple values separated by coma, then cross product wil be used to get triples.

Examples:
```
{
    "source_schema": "s1, s2",
    "source_table": "t1, t2",
    "source_field": "a"
}
```
is same as
```
{
    "source_schema": "s1, s2\ns1, s2",
    "source_table": "t1\nt2",
    "source_field": "a\na"
}
```
is same as
```
{
    "source_schema": "s1\ns1\ns2\ns2",
    "source_table": "t1\nt2\nt1\nt2",
    "source_field": "a\na\na\na"
}
```
