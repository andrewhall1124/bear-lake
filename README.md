# Sample Syntax

```python
import bear_lake as bl
import polars as pl
import datetime as dt

df = pl.read_parquet("data.parquet")

db = bl.connect("database")

# Create table
db.command(
    bl.table("stock_prices")
    .create(
        schema={
            "date": pl.Date,
            "ticker": pl.String,
            "price": pl.Float64
        },
        partition_keys=[pl.col("date").dt.year(), "ticker"],
        if_not_exists=False,
        replace=True
    )
)

# Insert data
db.insert(
    table="stock_prices",
    data=df
)

# Optimize table (deduplicate and repartition)
db.optimize(table="stock_prices")

# Query data
df = db.query(
    bl.table("stock_prices").select("*")
)

# Complex query
df = db.query(
    bl.table("stock_prices")
    .join(
        other=bl.table("calendar"),
        on=["date", "ticker"],
        how="inner"
    )
    .filter(
        pl.col("date").eq(dt.date.today())
    )
    .sort("date", "ticker")
    .select(
        "date", "ticker", "prices"
    )
)

# Delete data
db.command(
    bl.table("stock_prices")
    .delete(pl.col("date").le(dt.date.today()))
)

# Drop table
db.command(
    bl.table("stock_prices").drop()
)
```

## Commands

### Create
1. Create folders
2. Create metadata.json file