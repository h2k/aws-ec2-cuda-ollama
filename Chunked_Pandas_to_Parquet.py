import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_csv_to_parquet_all_strings(
    input_path: str,
    output_path: str,
    sep: str = "¦",
    target_ram_gb: int = 18,
    sample_rows: int = 100_000
):
    """
    Convert a large CSV to Parquet (Snappy) using chunking and
    optimizing chunksize to use approximately `target_ram_gb` of RAM.

    - Reads ALL columns as strings.
    - Uses pandas for CSV reading and pyarrow for Parquet writing.
    - Works well for big files (~5GB+) on a 32GB RAM machine.
    """

    # ---- Step 1: Sample to estimate memory per row ----
    print(f"Sampling {sample_rows} rows to estimate memory usage...")
    sample = pd.read_csv(
        input_path,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        nrows=sample_rows
    )

    if len(sample) == 0:
        raise ValueError("Sample has 0 rows. Is the CSV file empty?")

    bytes_used = sample.memory_usage(deep=True).sum()
    bytes_per_row = bytes_used / len(sample)

    print(f"Estimated bytes per row: {bytes_per_row:,.2f}")

    # ---- Step 2: Compute chunksize based on target RAM ----
    target_bytes = target_ram_gb * 1024 ** 3  # GB → bytes
    chunk_rows = int(target_bytes / bytes_per_row)

    # Safety floor to avoid too-small chunks
    if chunk_rows < 100_000:
        chunk_rows = 100_000

    print(f"Target RAM: {target_ram_gb} GB")
    print(f"Calculated chunksize: {chunk_rows:,} rows per chunk")

    # ---- Step 3: Stream CSV in chunks and write Parquet ----
    parquet_writer = None
    total_rows = 0
    chunk_idx = 0

    for chunk in pd.read_csv(
        input_path,
        sep=sep,
        dtype=str,
        keep_default_na=False,
        chunksize=chunk_rows
    ):
        chunk_idx += 1
        rows_in_chunk = len(chunk)
        total_rows += rows_in_chunk
        print(f"Processing chunk {chunk_idx} with {rows_in_chunk:,} rows...")

        # Convert pandas chunk → pyarrow Table
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if parquet_writer is None:
            # Enforce all-string schema
            string_schema = pa.schema(
                [(name, pa.string()) for name in table.column_names]
            )
            # Cast first table to ensure it matches the schema
            table = table.cast(string_schema)

            parquet_writer = pq.ParquetWriter(
                output_path,
                string_schema,
                compression="snappy"
            )
        else:
            # Ensure subsequent tables match schema as well
            table = table.cast(parquet_writer.schema)

        parquet_writer.write_table(table)

    if parquet_writer:
        parquet_writer.close()

    print(f"✅ Finished writing Parquet: {output_path}")
    print(f"✅ Total rows processed: {total_rows:,}")


# Example usage:
# convert_csv_to_parquet_all_strings("bigfile.csv", "bigfile.parquet")