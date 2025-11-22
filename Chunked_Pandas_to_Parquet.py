import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

input_path = "bigfile.csv"
output_path = "bigfile.parquet"

chunksize = 500_000  # adjust based on RAM

parquet_writer = None

for chunk in pd.read_csv(
    input_path,
    sep="¦",
    dtype=str,          # ✅ force pandas to read everything as string
    keep_default_na=False  # ✅ prevents NaN becoming float or missing
):
    # Convert pandas → pyarrow, force string columns
    table = pa.Table.from_pandas(
        chunk,
        preserve_index=False,  # ✅ avoids unnecessary index column
        schema=None            # we will infer only for 1st chunk
    )

    if parquet_writer is None:
        # ✅ Build a string-only schema for Parquet
        string_schema = pa.schema(
            [(name, pa.string()) for name in table.column_names]
        )
        parquet_writer = pq.ParquetWriter(
            output_path,
            string_schema,
            compression="snappy"
        )

    parquet_writer.write_table(table)

if parquet_writer:
    parquet_writer.close()

print("✅ Finished writing Parquet with all columns as STRING!")