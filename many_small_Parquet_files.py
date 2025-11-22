import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def split_csv_and_convert_to_parquet(
    input_path: str,
    output_prefix: str,
    sep: str = "¦",
    max_csv_mb: int = 80,      # target max size per temp CSV chunk
    delete_temp_csv: bool = True
):
    """
    1) Split a large CSV into smaller CSV files (by size).
    2) Convert each small CSV into a Parquet file (Snappy, all columns as string).

    - max_csv_mb: size of each CSV chunk on disk (Parquet will usually be < 100MB).
    - output_prefix: base name for chunk files, e.g., 'bigfile' -> bigfile_part0001.parquet
    """

    max_bytes = max_csv_mb * 1024 * 1024
    part_index = 1
    temp_files = []

    # ---------- STEP 1: Split the big CSV into smaller CSV files ----------
    print(f"Splitting '{input_path}' into ~{max_csv_mb}MB CSV chunks...")

    with open(input_path, "r", encoding="utf-8", errors="ignore") as fin:
        header = fin.readline()
        if not header:
            raise ValueError("Input CSV appears to be empty.")

        # open first part file
        current_bytes = 0
        temp_name = f"{output_prefix}_part{part_index:04d}.csv"
        fout = open(temp_name, "w", encoding="utf-8", newline="")
        temp_files.append(temp_name)

        # write header
        fout.write(header)
        current_bytes += len(header.encode("utf-8"))

        for line in fin:
            line_bytes = len(line.encode("utf-8"))

            # if adding this line exceeds the max size, start a new file
            if current_bytes + line_bytes > max_bytes and current_bytes > len(header):
                fout.close()
                part_index += 1
                temp_name = f"{output_prefix}_part{part_index:04d}.csv"
                fout = open(temp_name, "w", encoding="utf-8", newline="")
                temp_files.append(temp_name)

                # write header to new file
                fout.write(header)
                current_bytes = len(header.encode("utf-8"))

            fout.write(line)
            current_bytes += line_bytes

        fout.close()

    print(f"Created {len(temp_files)} CSV chunk(s):")
    for f in temp_files:
        print(f"  - {f} ({os.path.getsize(f) / (1024*1024):.2f} MB)")

    # ---------- STEP 2: Convert each small CSV to Parquet (all strings) ----------
    print("\nConverting each CSV chunk to Parquet (Snappy, all columns as string)...")

    parquet_files = []

    for temp_csv in temp_files:
        part_name = os.path.splitext(os.path.basename(temp_csv))[0]
        parquet_path = f"{part_name}.parquet"
        parquet_files.append(parquet_path)

        print(f"Processing {temp_csv} -> {parquet_path} ...")

        # Read whole small CSV at once (it's only ~80MB)
        df = pd.read_csv(
            temp_csv,
            sep=sep,
            dtype=str,            # ✅ all columns as string
            keep_default_na=False # ✅ keep empty strings as-is
        )

        table = pa.Table.from_pandas(df, preserve_index=False)

        # Enforce all-string schema explicitly (optional but safe)
        string_schema = pa.schema(
            [(name, pa.string()) for name in table.column_names]
        )
        table = table.cast(string_schema)

        pq.write_table(
            table,
            parquet_path,
            compression="snappy"
        )

        size_mb = os.path.getsize(parquet_path) / (1024 * 1024)
        print(f"  -> Parquet size: {size_mb:.2f} MB")

        # Optionally remove the temp CSV
        if delete_temp_csv:
            os.remove(temp_csv)

    print("\n✅ Done! Generated the following Parquet files:")
    for f in parquet_files:
        print(f"  - {f} ({os.path.getsize(f) / (1024*1024):.2f} MB)")

    return parquet_files

if __name__ == "__main__":
# Example usage:
    split_csv_and_convert_to_parquet("sample_5gb.csv", "bfile", max_csv_mb=100)



