# Autómatas / tablas de análisis del parser SQL

El proyecto usa dos etapas: análisis léxico y análisis sintáctico.

## 1. Autómata léxico

El tokenizer recorre el texto de izquierda a derecha y clasifica tokens.

```text
START
  ├── espacio              -> START
  ├── letra/_              -> IDENTIFIER_OR_KEYWORD
  ├── dígito               -> NUMBER
  ├── - seguido de dígito  -> NUMBER
  ├── ' o "                -> STRING
  ├── símbolo SQL          -> SYMBOL
  ├── --                   -> LINE_COMMENT
  └── /*                   -> BLOCK_COMMENT

IDENTIFIER_OR_KEYWORD
  ├── letra/dígito/_       -> IDENTIFIER_OR_KEYWORD
  └── otro                 -> aceptar IDENTIFIER o KEYWORD

NUMBER
  ├── dígito               -> NUMBER
  ├── . seguido de dígito  -> NUMBER_FLOAT
  └── otro                 -> aceptar NUMBER

STRING
  ├── cierre de comilla    -> aceptar STRING
  ├── comilla duplicada    -> STRING
  └── otro                 -> STRING
```

## 2. Tabla LL(1) simplificada para sentencias

| No terminal | Lookahead | Producción |
|---|---|---|
| `<statement>` | CREATE | `<create_table>` |
| `<statement>` | SELECT | `<select>` |
| `<statement>` | INSERT | `<insert>` |
| `<statement>` | DELETE | `<delete>` o alias `DELETE TABLE` |
| `<statement>` | DROP | `<drop_table>` |
| `<condition>` | IDENTIFIER + `=` | `col = literal` |
| `<condition>` | IDENTIFIER + BETWEEN | `col BETWEEN literal AND literal` |
| `<condition>` | IDENTIFIER + IN | consulta espacial |

## 3. Decisión semántica de llamada

Después de construir el AST, cada nodo implementa `to_engine_call()`:

```text
EqualsCondition        -> search_by_index
BetweenCondition       -> range_search_by_index
SpatialRadiusCondition -> rtree_range_search
SpatialKNNCondition    -> rtree_knn
InsertCommand          -> insert_record
SelectAllCommand       -> select_all con paginación
SelectAllCommand(top)  -> select_all(limit=top)
DeleteCommand          -> delete_by_key
DropTableCommand       -> drop_table
CreateTableCommand     -> create_table o create_table_from_csv
```

La ejecución real quedará centralizada en `QueryExecutor` en la siguiente fase.
