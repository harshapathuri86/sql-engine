# Mini SQL Engine

---

This is a mini SQL engine that can execute SQL queries on a given database. The database is a CSV file with a fixed schema. The engine can execute the following queries:

- PROJECTION:
  - `SELECT * FROM <table_name>`
  - `SELECT <column_list> FROM <table_name>, <table_name>, ...`

- AGGREGATION:
  - Supported aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`
  - `SELECT <aggregate_function>(<column_name>) FROM <table_name>`

- DISTINCT:
  - `SELECT DISTINCT <column_list> FROM <table_name>`

- WHERE:
  - `SELECT <column_list> FROM <table_name> WHERE <condition>`
  - Supported conditions: `=`, `>`, `<`, `>=`, `<=`, and maximum of one `AND` or `OR` condition

- GROUP BY:
  - `SELECT <column_list> FROM <table_name> GROUP BY <column_name>`
  - Supported aggregate functions: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

- ORDER BY:
  - `SELECT <column_list> FROM <table_name> ORDER BY <column_name> ASC|DESC`
  - Supports multiple tables

## How to run

- Clone the repository
- Go to the src directory

  ```sh
  cd src
  ```

- Make the script executable

  ```sh
  chmod +x sql_engine.sh
  ```

- Run the following command

  ```sh
  ./sql_engine.sh <sql_query>
  ```
