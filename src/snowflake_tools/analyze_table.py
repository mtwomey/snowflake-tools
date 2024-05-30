import os, argparse
import snowflake.connector
from snowflake_tools import snowflake_config
import pkg_resources
from snowflake_tools.SnowflakeTable import SnowflakeTable

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
version = pkg_resources.get_distribution("snowflake-tools").version


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def cli():
    parser = argparse.ArgumentParser(
        description=f"Analyze Snowflake Table v{version}.",
        epilog="Example: snowflake-analyze-table --profile bd --table FIVETRAN_DATABASE.MYSQL_LOTTERY_PROD.NLDLS_DLSLOT_ENTRIES",
    )

    parser.add_argument(
        "--profile",
        help="Profile name",
        required=True,
    )

    parser.add_argument(
        "--table", help="Fully qualified table or view name", required=True
    )

    args = parser.parse_args()

    # args = parser.parse_args(['BD_DEV_PRD.MTWOMEY.STG_LT__ENTRIES_CURRENT'])

    if args.table is None:
        parser.print_help()
    else:
        try:
            config = snowflake_config.get_profile(args.profile)
        except ValueError:
            print(
                "Error: Please provide a fully qualified table or view name: DATABASE.SCHEMA.TABLE"
            )
            exit()

        table = SnowflakeTable(
            # SNOWFLAKE_TABLE,
            args.table,
            {
                "user": config["user"],
                "password": config["password"],
                "account": config["account"],
                "role": config["role"],
            },
            debug=True,
        )

        table.analyze()

        print(f"\nTotal rows: {table.total_rows:,}\n")
        print(
            table.column_info[
                [
                    "COLUMN_NAME",
                    "DATA_TYPE",
                    "NULLS",
                    "EMPTY_STRINGS",
                    "ZEROS",
                    "DIST",
                    "UNIQUE",
                    "VALUES",
                ]
            ]
            .sort_values(by="COLUMN_NAME")
            .to_markdown(index=False, tablefmt="simple")
        )
