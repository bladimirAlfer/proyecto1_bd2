# Gramática formal del subconjunto SQL

Esta gramática describe el subconjunto SQL implementado en `SQLParser`.
Se usa un parser descendente recursivo. Las palabras reservadas son
case-insensitive.

```ebnf
<statement> ::= <create_table>
              | <select>
              | <insert>
              | <delete>
              | <drop_table>

<create_table> ::= CREATE TABLE <identifier>
                   "(" <column_def> { "," <column_def> } ")"
                   [ FROM FILE <file_path> ] [ ";" ]

<column_def> ::= <identifier> <type_name> [ INDEX <index_type> ]

<type_name> ::= int | integer | float | double | real
              | str | string | text | varchar [ "(" <number> ")" ]
              | bool | boolean

<index_type> ::= sequential | sequential_file
               | extendible_hash | hash
               | bplus | bplus_tree
               | rtree | r_tree

<select> ::= SELECT [ TOP <integer> ] "*" FROM <identifier> [ WHERE <condition> ] [ ";" ]

<condition> ::= <identifier> "=" <literal>
              | <identifier> BETWEEN <literal> AND <literal>
              | <identifier> IN "(" POINT "(" <number> "," <number> ")" "," RADIUS <number> ")"
              | <identifier> IN "(" POINT "(" <number> "," <number> ")" "," K <integer> ")"

<insert> ::= INSERT INTO <identifier> VALUES
             "(" [ <literal> { "," <literal> } ] ")" [ ";" ]

<delete> ::= DELETE FROM <identifier> WHERE <identifier> "=" <literal> [ ";" ]

<drop_table> ::= DROP TABLE <identifier> [ ";" ]
               | DELETE TABLE <identifier> [ ";" ]

<literal> ::= <number> | <string> | <identifier> | TRUE | FALSE | NULL
<file_path> ::= <string> | { cualquier token hasta ";" o EOF }
```

## Traducción hacia operaciones internas

| SQL | AST | Llamada lógica |
|---|---|---|
| `CREATE TABLE ...` | `CreateTableCommand` | `create_table(...)` |
| `CREATE TABLE ... FROM FILE ...` | `CreateTableCommand` | `create_table_from_csv(...)` |
| `SELECT * FROM table` | `SelectAllCommand` | `select_all(table)` con límite por defecto |
| `SELECT TOP n * FROM table` | `SelectAllCommand(top=n)` | `select_all(table, limit=n)` |
| `SELECT ... WHERE col = value` | `SelectCommand + EqualsCondition` | `search_by_index(table, col, value)` |
| `SELECT ... WHERE col BETWEEN a AND b` | `SelectCommand + BetweenCondition` | `range_search_by_index(table, col, a, b)` |
| `SELECT ... POINT(...), RADIUS r` | `SelectCommand + SpatialRadiusCondition` | `rtree_range_search(table, col, point, r)` |
| `SELECT ... POINT(...), K k` | `SelectCommand + SpatialKNNCondition` | `rtree_knn(table, col, point, k)` |
| `INSERT INTO ... VALUES (...)` | `InsertCommand` | `insert_record(table, values)` |
| `DELETE FROM ... WHERE col = value` | `DeleteCommand` | `delete_by_key(table, col, value)` |
| `DROP TABLE table` | `DropTableCommand` | `drop_table(table)` |
