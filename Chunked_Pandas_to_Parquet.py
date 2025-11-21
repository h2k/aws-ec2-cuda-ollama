import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def convert_csv_to_parquet(input_path, output_path, separator = "¦",  chunksize= 500_000 , compression="snappy"):
    
    # adjust chunksize based on RAM
    try:
        parquet_writer = None
        for chunk in pd.read_csv(input_path, sep=separator, chunksize=chunksize):
            table = pa.Table.from_pandas(chunk)

            if parquet_writer is None:
                parquet_writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                    compression=compression
                )
            parquet_writer.write_table(table)

        if parquet_writer:
            parquet_writer.close()

        return {"status":1 , "Msg": f"Finished writing {output_path} parquet!", "output_path": output_path}
    except Exception as e:
        return {"status":-1 , "Msg": f"Error during conversion: {e}"}
    
if __name__ == "__main__":
    input_path = "sample_5gb.csv"
    output_path = "bigfile.parquet"
    status = convert_csv_to_parquet(input_path, output_path, chunksize= 500_000 )
    print(status)
