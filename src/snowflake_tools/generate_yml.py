import os, argparse
import threading
from snowflake_tools import snowflake_config
import pkg_resources
from snowflake_tools.SnowflakeTable import SnowflakeTable
import yaml

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
version = pkg_resources.get_distribution("snowflake-tools").version


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))

def is_mixed_case(input_str):
    return any(char.isupper() for char in input_str) and any(char.islower() for char in input_str)

def cli():
    parser = argparse.ArgumentParser(
        description=f"Generate DBT yml from Snowflake Table v{version}.",
        epilog="Example: snowflake-generate-yml --profile bd --table FIVETRAN_DATABASE.MYSQL_LOTTERY_PROD.NLDLS_DLSLOT_ENTRIES",
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

        # dump(table.column_info[["DATA_TYPE"]])

        # print(f"\nTotal rows: {table.total_rows:,}\n")
        # print(
        #     table.column_info[
        #         [
        #             # "COLUMN_NAME",
        #             "DATA_TYPE",
        #             # "NULLS",
        #             # "EMPTY_STRINGS",
        #             # "ZEROS",
        #             # "DIST",
        #             # "UNIQUE",
        #             # "VALUES",
        #         ]
        #     ]
        #     # .sort_values(by="COLUMN_NAME")
        #     .to_markdown(index=False, tablefmt="simple")
        # )

        def str_presenter(dumper, data):
            if data.count("\n") > 0:
                data = "\n".join(
                    [line.rstrip() for line in data.splitlines()]
                )  # Remove any trailing spaces, then put it back together again
                return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
            return dumper.represent_scalar("tag:yaml.org,2002:str", data)

        yaml.add_representer(str, str_presenter)

        class MyDumper(yaml.Dumper):
            def increase_indent(self, flow=False, indentless=False):
                return super(MyDumper, self).increase_indent(flow, False)

        dict = {
            "version": 2,
            "models": [{"name": args.table.split(".")[-1], "columns": []}],
        }

        for row in table.column_info.itertuples():

            tests = []
            description_line = []
            description_line.append(f"[ {row.DATA_TYPE} ]")
            description_line.append("")

            if row.UNIQUE:
                description_line.append("* Column has Unique values")
                tests.append('unique')

            if row.NULLS:
                description_line.append("* Column has NULL values")
            else:
                description_line.append("* Column has NO NULL values")
                tests.append("not_null")

            if row.DATA_TYPE == "TEXT":
                if row.EMPTY_STRINGS:
                    description_line.append("* Column has empty string values")
                else:
                    description_line.append("* Column has NO empty string values")
                    tests.append("dbt_utils.not_empty_string")

            if row.DATA_TYPE == "NUMBER":
                if row.ZEROS:
                    description_line.append("* Column has zero values")
                else:
                    description_line.append("* Column has NO zero values")
                    tests.append("not_zero")

            columns = dict["models"][0]["columns"]
            columns.append(
                {
                    "name": f'"{row.COLUMN_NAME}"' if is_mixed_case(row.COLUMN_NAME) else row.COLUMN_NAME.lower(),
                    "description": "\n".join(description_line),
                    "tests": tests,
                }
            )
            # dict[models][0]["columns": []]
            # print(row.DATA_TYPE)

        # print(dict["models"][0]["columns"])
        print(yaml.dump(dict, Dumper=MyDumper, sort_keys=False))
        # print(dict)
