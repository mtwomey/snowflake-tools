import os, sys, argparse
import snowflake.connector
from snowflake_tools import snowflake_config
from snowflake_tools import Timer
import pkg_resources

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
version = pkg_resources.get_distribution("snowflake-tools").version


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


class SnowflakeTable:
    def __init__(self, table_name, connection_config, max_distinct=8, debug=False):
        self.table_name = table_name
        self.max_distinct = max_distinct
        self.connection_config = connection_config
        self.connection = snowflake.connector.connect(
            user=connection_config["user"],
            password=connection_config["password"],
            account=connection_config["account"],
            database=connection_config["database"],
            schema=connection_config["schema"],
        )
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            rf"""select * from {connection_config['database']}.INFORMATION_SCHEMA.COLUMNS
            where table_schema = '{connection_config['schema']}'
            and table_name = '{table_name}'"""
        )
        self.column_info = self.cursor.fetch_pandas_all()
        if len(self.column_info) == 0:
            print(
                f'Could not find information on table or view: {connection_config["database"]}.{connection_config["schema"]}.{table_name}'
            )
            exit()
        self.debug = debug

        self.total_rows = self.cursor.execute(
            f"""select count(1) from {connection_config['database']}.{connection_config['schema']}.{self.table_name}"""
        ).fetchone()[  # type: ignore
            0
        ]

        if debug == True:
            print(f"Max distinct values: {max_distinct}")

    def analyze(self):
        self._add_has_null()
        self._add_has_empty_strings()
        self._add_has_zeros()
        self._add_distinct_count()
        self._add_values()

    def _add_has_null(self):
        if self.debug == True:
            print("Checking for null values...", end="", flush=True)

        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                result = self.cursor.execute(
                    f"""select count(1) from {self.connection_config['database']}.{self.connection_config['schema']}.{self.table_name} where {row["COLUMN_NAME"]} is null"""
                )
                null_count = result.fetchone()[0]  # type: ignore

                if null_count > 0:
                    self.column_info.at[index, "NULLS"] = "yes"
                else:
                    self.column_info.at[index, "NULLS"] = "no"

    def _add_has_empty_strings(self):
        if self.debug == True:
            print("Checking for empty strings...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                if row["DATA_TYPE"] == "TEXT":
                    result = self.cursor.execute(
                        f"""select count(1) from {self.connection_config['database']}.{self.connection_config['schema']}.{self.table_name} where {row["COLUMN_NAME"]} = ''"""
                    )
                    empty_strings_count = result.fetchone()[0]  # type: ignore
                    if empty_strings_count > 0:
                        self.column_info.at[index, "EMPTY_STRINGS"] = "yes"
                    else:
                        self.column_info.at[index, "EMPTY_STRINGS"] = "no"
                else:
                    self.column_info.at[index, "EMPTY_STRINGS"] = ""

    def _add_has_zeros(self):
        if self.debug == True:
            print("Checking for zero values...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                if row["DATA_TYPE"] == "NUMBER":
                    result = self.cursor.execute(
                        f"""select count(1) from {self.connection_config['database']}.{self.connection_config['schema']}.{self.table_name} where {row["COLUMN_NAME"]} = 0"""
                    )
                    zeros_count = result.fetchone()[0]  # type: ignore

                    if zeros_count > 0:
                        self.column_info.at[index, "ZEROS"] = "yes"
                    else:
                        self.column_info.at[index, "ZEROS"] = "no"
                else:
                    self.column_info.at[index, "ZEROS"] = ""

    def _add_distinct_count(self):
        if self.debug == True:
            print("Checking distinct count...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                result = self.cursor.execute(
                    f"""select count(distinct({row["COLUMN_NAME"]})) from {self.connection_config['database']}.{self.connection_config['schema']}.{self.table_name}"""
                )
                distinct_count = result.fetchone()[0]  # type: ignore

                self.column_info.at[index, "DIST"] = distinct_count

            self.column_info["DIST"] = self.column_info["DIST"].astype(int)

    def _add_values(self):
        if self.debug == True:
            print("Finding distinct values...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                distinct_count = row["DIST"]
                if distinct_count <= self.max_distinct:

                    result = self.cursor.execute(
                        f"""select listagg(distinct({row["COLUMN_NAME"]}), ', ') from {self.connection_config['database']}.{self.connection_config['schema']}.{self.table_name}"""
                    )
                    values = result.fetchone()[0]  # type: ignore

                    self.column_info.at[index, "VALUES"] = values
                else:
                    self.column_info.at[index, "VALUES"] = (
                        f"> {self.max_distinct} values"
                    )


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
            (SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_TABLE) = args.table.split(
                "."
            )
        except ValueError:
            print(
                "Error: Please provide a fully qualified table or view name: DATABASE.SCHEMA.TABLE"
            )
            exit()

        table = SnowflakeTable(
            SNOWFLAKE_TABLE,
            {
                "user": config["user"],
                "password": config["password"],
                "account": config["account"],
                "database": SNOWFLAKE_DATABASE,
                "schema": SNOWFLAKE_SCHEMA,
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
                    "VALUES",
                ]
            ]
            .sort_values(by="COLUMN_NAME")
            .to_markdown(index=False, tablefmt="simple")
        )
