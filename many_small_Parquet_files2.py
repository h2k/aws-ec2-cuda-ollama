import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def split_csv_and_convert_to_packed_parquet(
    input_path: str,
    output_prefix: str,
    sep: str = "¦",
    max_mb: int = 100,
    delete_temp_csv: bool = True,
    delete_temp_parquet: bool = True,
):
    """
    1) Split a large CSV into smaller CSV files by size (max_mb each, approx).
    2) Convert each small CSV into a Parquet file (Snappy, all columns as string).
    3) Merge small Parquet files into larger final Parquet parts such that:
       - each final Parquet file <= max_mb (compressed size)
       - number of final files is as small as possible (greedy packing).

    Returns list of final Parquet file paths.
    """

    max_bytes = max_mb * 1024 * 1024

    # ---------- STEP 1: Split big CSV into smaller CSV files ----------
    print(f"Splitting '{input_path}' into ~{max_mb}MB CSV chunks...")

    temp_csv_files = []
    part_index = 1

    with open(input_path, "r", encoding="utf-8", errors="ignore") as fin:
        header = fin.readline()
        if not header:
            raise ValueError("Input CSV appears to be empty.")

        current_bytes = 0
        temp_name = f"{output_prefix}_part{part_index:04d}.csv"
        fout = open(temp_name, "w", encoding="utf-8", newline="")
        temp_csv_files.append(temp_name)

        fout.write(header)
        current_bytes += len(header.encode("utf-8"))

        for line in fin:
            line_bytes = len(line.encode("utf-8"))

            if current_bytes + line_bytes > max_bytes and current_bytes > len(header):
                fout.close()
                part_index += 1
                temp_name = f"{output_prefix}_part{part_index:04d}.csv"
                fout = open(temp_name, "w", encoding="utf-8", newline="")
                temp_csv_files.append(temp_name)

                fout.write(header)
                current_bytes = len(header.encode("utf-8"))

            fout.write(line)
            current_bytes += line_bytes

        fout.close()

    print(f"Created {len(temp_csv_files)} CSV chunk(s):")
    for f in temp_csv_files:
        print(f"  - {f} ({os.path.getsize(f) / (1024*1024):.2f} MB)")

    # ---------- STEP 2: Convert each CSV chunk to Parquet (all strings) ----------
    print("\nConverting each CSV chunk to Parquet (Snappy, all columns as string)...")

    temp_parquet_files = []
    string_schema = None

    for temp_csv in temp_csv_files:
        base = os.path.splitext(os.path.basename(temp_csv))[0]
        parquet_path = f"{base}.parquet"
        temp_parquet_files.append(parquet_path)

        print(f"Processing {temp_csv} -> {parquet_path} ...")

        df = pd.read_csv(
            temp_csv,
            sep=sep,
            dtype=str,            # all columns as string
            keep_default_na=False # keep empty strings as-is
        )

        table = pa.Table.from_pandas(df, preserve_index=False)

        if string_schema is None:
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

        if delete_temp_csv:
            os.remove(temp_csv)

    # ---------- STEP 3: Pack/merge Parquet files into final parts ----------
    print("\nPacking Parquet chunks into final files (≤ "
          f"{max_mb}MB, as few files as possible)...")

    # 3.1: Collect sizes
    sizes = {p: os.path.getsize(p) for p in temp_parquet_files}
    # Sort by size (largest first) for greedy bin packing
    files_sorted = sorted(temp_parquet_files, key=lambda x: sizes[x], reverse=True)

    # 3.2: Greedy grouping
    groups = []  # each: {"files": [...], "size": total_bytes}
    for f in files_sorted:
        placed = False
        for g in groups:
            if g["size"] + sizes[f] <= max_bytes:
                g["files"].append(f)
                g["size"] += sizes[f]
                placed = True
                break
        if not placed:
            groups.append({"files": [f], "size": sizes[f]})

    print(f"Formed {len(groups)} final group(s):")
    for i, g in enumerate(groups, start=1):
        print(
            f"  Group {i}: {len(g['files'])} file(s), "
            f"total ~{g['size'] / (1024*1024):.2f} MB"
        )

    # 3.3: Merge each group into a final Parquet file
    final_parquet_files = []
    for idx, group in enumerate(groups, start=1):
        final_path = f"{output_prefix}_final_part{idx:04d}.parquet"
        print(f"\nMerging group {idx} -> {final_path} ...")

        tables = []
        for pfile in group["files"]:
            t = pq.read_table(pfile)
            if string_schema is not None:
                t = t.cast(string_schema)
            tables.append(t)

        combined = pa.concat_tables(tables, promote=True)
        pq.write_table(combined, final_path, compression="snappy")

        size_mb = os.path.getsize(final_path) / (1024 * 1024)
        print(f"  -> Final Parquet size: {size_mb:.2f} MB")

        final_parquet_files.append(final_path)

    if delete_temp_parquet:
        for p in temp_parquet_files:
            os.remove(p)

    print("\n✅ Done! Final Parquet files:")
    for f in final_parquet_files:
        print(f"  - {f} ({os.path.getsize(f) / (1024*1024):.2f} MB)")

    return final_parquet_files


# Example usage:
# split_csv_and_convert_to_packed_parquet("bigfile.csv", "bigfile", max_mb=100)
if __name__ == "__main__":
# Example usage:
    split_csv_and_convert_to_packed_parquet("sample_5gb.csv", "bfile", max_mb=100)

