import json
import sys

import duckdb


def quote_identifier(value: str) -> str:
    return f'"{value.replace(chr(34), chr(34) * 2)}"'


def quote_literal(value: str) -> str:
    return f"'{value.replace(chr(39), chr(39) * 2)}'"


def is_timestamp(column_type: str) -> bool:
    return column_type.upper().startswith("TIMESTAMP")


def mismatch_expression(
    column: str,
    expected_type: str,
    actual_type: str,
) -> str:
    quoted_column = quote_identifier(column)
    expected_value = f"expected.{quoted_column}"
    actual_value = f"actual.{quoted_column}"

    if is_timestamp(expected_type) and is_timestamp(actual_type):
        expected_value = f"epoch_us({expected_value})"
        actual_value = f"epoch_us({actual_value})"

    return (
        "COUNT(*) FILTER (WHERE "
        f"{expected_value} IS DISTINCT FROM {actual_value}) "
        f"AS {quoted_column}"
    )


def main() -> None:
    if len(sys.argv) != 3:
        raise SystemExit(
            "Usage: python scripts/compare-parquet-rows.py <expected.parquet> <actual.parquet>"
        )

    expected_path = sys.argv[1]
    actual_path = sys.argv[2]
    connection = duckdb.connect()
    connection.execute(
        "CREATE VIEW expected_rows AS SELECT * FROM read_parquet("
        f"{quote_literal(expected_path)})"
    )
    connection.execute(
        "CREATE VIEW actual_rows AS SELECT * FROM read_parquet("
        f"{quote_literal(actual_path)})"
    )

    expected_schema = {
        row[0]: row[1]
        for row in connection.execute("DESCRIBE expected_rows").fetchall()
    }
    actual_schema = {
        row[0]: row[1]
        for row in connection.execute("DESCRIBE actual_rows").fetchall()
    }
    actual_columns = list(actual_schema)
    missing_columns = [
        column for column in actual_columns if column not in expected_schema
    ]

    if missing_columns:
        raise RuntimeError(f"Expected file is missing columns: {missing_columns}")

    key_columns = ["source_system", "page_view_id"]
    comparison_columns = [
        column for column in actual_columns if column not in key_columns
    ]
    join_condition = " AND ".join(
        f"expected.{quote_identifier(column)} = actual.{quote_identifier(column)}"
        for column in key_columns
    )
    mismatch_expressions = ",\n".join(
        mismatch_expression(
            column,
            expected_schema[column],
            actual_schema[column],
        )
        for column in comparison_columns
    )
    result = connection.execute(
        f"""
        SELECT
          COUNT(*) AS compared_rows,
          COUNT(*) FILTER (WHERE expected.page_view_id IS NULL) AS missing_expected_rows,
          {mismatch_expressions}
        FROM actual_rows AS actual
        LEFT JOIN expected_rows AS expected ON {join_condition}
        """
    ).fetchone()
    result_columns = [description[0] for description in connection.description]
    values = dict(zip(result_columns, result))
    column_mismatches = {
        column: values[column]
        for column in comparison_columns
        if values[column] > 0
    }

    print(
        json.dumps(
            {
                "compared_rows": values["compared_rows"],
                "missing_expected_rows": values["missing_expected_rows"],
                "column_mismatches": column_mismatches,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
