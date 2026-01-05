# Bear Lake

A lightweight, file-based database built on [Polars](https://pola.rs/) and Parquet, designed for fast analytics and easy data management.

Bear Lake provides a simple API for creating partitioned tables, inserting data, and running efficient queries using Polars' lazy evaluation. All data is stored as Parquet files with automatic partitioning support.

## Installation

You can install `bear-lake` using `pip`.

```bash
pip install bear-lake
```

## Usage

### Quick Start

```python
import polars as pl
import bear_lake as bl

# Connect to database
db = bl.connect("my_database")

# Create a table with schema and partitioning
schema = {
    "date": pl.Date,
    "ticker": pl.String,
    "price": pl.Float64
}

db.create(
    name="stocks",
    schema=schema,
    partition_keys=["ticker"],
    primary_keys=["date", "ticker"],
    mode="error"
)

# Insert data
data = pl.DataFrame({
    "date": ["2024-01-01", "2024-01-02"],
    "ticker": ["AAPL", "AAPL"],
    "price": [150.0, 152.5]
})

db.insert("stocks", data, mode="append")

# Query data using Polars lazy evaluation
result = db.query(
    bl.table("stocks")
    .filter(pl.col("ticker") == "AAPL")
    .select(["date", "price"])
)

print(result)
```

### API Reference

#### Database Connection

```python
db = bl.connect(path: str) -> Database
```

Connect to a database at the specified path. Creates the directory if it doesn't exist.

#### Creating Tables

```python
db.create(
    name: str,
    schema: dict[str, pl.DataType],
    partition_keys: list[str],
    primary_keys: list[str],
    mode: str = "error"
)
```

**Parameters:**
- `name`: Table name
- `schema`: Dictionary mapping column names to Polars data types
- `partition_keys`: Columns to partition data by (creates hierarchical folder structure)
- `primary_keys`: Columns that form a unique identifier (used for deduplication)
- `mode`: How to handle existing tables - `"error"` (default), `"replace"`, or `"skip"`

#### Inserting Data

```python
db.insert(name: str, data: pl.DataFrame, mode: str = "append")
```

**Parameters:**
- `name`: Table name
- `data`: Polars DataFrame to insert
- `mode`: How to handle existing partitions - `"append"` (default), `"overwrite"`, or `"error"`

#### Querying Data

```python
result = db.query(expression: pl.LazyFrame) -> pl.DataFrame
```

Execute a lazy Polars query and return results. Use `bl.table(name)` to get a LazyFrame for a table.

```python
# Get a LazyFrame for querying
lazy_df = bl.table("stocks")

# Build query with Polars operations
result = db.query(
    lazy_df
    .filter(pl.col("date") > "2024-01-01")
    .group_by("ticker")
    .agg(pl.col("price").mean())
)
```

#### Deleting Data

```python
db.delete(name: str, expression: pl.Expr)
```

Delete rows matching the given expression from all partitions.

```python
# Delete all rows where ticker is AAPL
db.delete("stocks", pl.col("ticker") == "AAPL")
```

#### Dropping Tables

```python
db.drop(name: str)
```

Remove a table and all its data.

#### Table Metadata

```python
# List all tables
tables = db.list_tables() -> list[str]

# Get table schema
schema = db.get_schema(name: str) -> dict[str, pl.DataType]

# Get partition keys
partition_keys = db.get_partition_keys(name: str) -> list[str]

# Get primary keys
primary_keys = db.get_primary_keys(name: str) -> list[str]
```

#### Optimizing Tables

```python
db.optimize(name: str)
```

Deduplicate rows based on primary keys (keeping the last occurrence) and sort data. This compacts storage and improves query performance.

