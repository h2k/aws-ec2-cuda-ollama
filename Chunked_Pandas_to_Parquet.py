import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq



if __name__ == "__main__":
    input_path = "sample_5gb.csv"
    output_path = "bigfile.parquet"

    chunksize = 500_000  # adjust based on RAM

    parquet_writer = None

    for chunk in pd.read_csv(input_path, sep="¦", chunksize=chunksize):
        table = pa.Table.from_pandas(chunk)

        if parquet_writer is None:
            parquet_writer = pq.ParquetWriter(
                output_path,
                table.schema,
                compression="snappy"
            )

        parquet_writer.write_table(table)

    if parquet_writer:
        parquet_writer.close()

    print("Finished writing parquet!")
