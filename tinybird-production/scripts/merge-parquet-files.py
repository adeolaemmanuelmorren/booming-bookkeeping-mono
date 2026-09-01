import glob
import sys

import pyarrow.parquet as parquet


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python scripts/merge-parquet-files.py '<input-glob>' <output-file>"
        )

    input_pattern = sys.argv[1]
    output_path = sys.argv[2]
    input_paths = sorted(glob.glob(input_pattern))

    if not input_paths:
        raise SystemExit(f"No Parquet files matched {input_pattern}")

    schema = parquet.read_schema(input_paths[0])
    row_count = 0

    with parquet.ParquetWriter(output_path, schema, compression="zstd") as writer:
        for input_path in input_paths:
            table = parquet.read_table(input_path)

            if not table.schema.equals(schema, check_metadata=False):
                raise RuntimeError(f"Schema mismatch in {input_path}")

            writer.write_table(table.cast(schema))
            row_count += table.num_rows

    print(row_count)


if __name__ == "__main__":
    main()
