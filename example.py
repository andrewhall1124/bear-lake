import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent / "src"))

import bear_lake as bl
import polars as pl
import datetime as dt

db = bl.connect("my_database")

# Create and insert
db.command(
    bl.table("stock_prices").create(
        schema={"date": pl.Date, "ticker": pl.String, "price": pl.Float64},
        partition_keys=[pl.col("date").dt.year(), "ticker"],
        mode="replace",
    )
)

df = pl.DataFrame({
    "date": [dt.date(2024, 1, 1), dt.date(2024, 1, 2), dt.date(2025, 1, 1)],
    "ticker": ["AAPL", "AAPL", "GOOGL"],
    "price": [150.0, 151.0, 2800.0],
})

db.insert(table="stock_prices", data=df)

# Query
result = db.query(
    bl.table("stock_prices")
    .filter(pl.col("ticker") == "AAPL")
    .select("date", "ticker", "price")
)

print("Results:")
print(result)

print("\nDatabase info:")
print(db)
