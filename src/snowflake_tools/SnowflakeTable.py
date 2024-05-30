import snowflake.connector
from snowflake_tools import Timer


class SnowflakeTable:
    def __init__(self, fq_table, connection_config, max_distinct=8, debug=False):
        (self.snowflake_database, self.snowflake_schema, self.snowflake_table) = (
            fq_table.upper().split(".")
        )
        self.max_distinct = max_distinct
        self.connection_config = connection_config
        self.connection = snowflake.connector.connect(
            user=connection_config["user"],
            password=connection_config["password"],
            account=connection_config["account"],
            database=self.snowflake_database,
            schema=self.snowflake_schema,
            role=connection_config["role"],
        )
        self.cursor = self.connection.cursor()
        self.debug = debug

        if debug == True:
            print(f"Max distinct values: {max_distinct}")

    def analyze(self):
        self.cursor.execute(
            rf"""select * from {self.snowflake_database}.INFORMATION_SCHEMA.COLUMNS
            where table_schema = '{self.snowflake_schema}'
            and table_name = '{self.snowflake_table}'"""
        )
        self.column_info = self.cursor.fetch_pandas_all()
        if len(self.column_info) == 0:
            print(
                f"Could not find information on table or view: {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"
            )
            exit()
        self.total_rows = self.cursor.execute(
            f"""select count(1) from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"""
        ).fetchone()[  # type: ignore
            0
        ]
        self._add_has_null()
        self._add_has_empty_strings()
        self._add_has_zeros()
        self._add_distinct_count()
        self._add_values()
        self._add_unique()

    def _add_has_null(self):
        if self.debug == True:
            print("Checking for null values...", end="", flush=True)

        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                result = self.cursor.execute(
                    f"""select count(1) from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table} where "{row["COLUMN_NAME"]}" is null"""
                )
                null_count = result.fetchone()[0]  # type: ignore

                if null_count > 0:
                    self.column_info.at[index, "NULLS"] = True
                else:
                    self.column_info.at[index, "NULLS"] = False

    def _add_has_empty_strings(self):
        if self.debug == True:
            print("Checking for empty strings...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                if row["DATA_TYPE"] == "TEXT":
                    result = self.cursor.execute(
                        f"""select count(1) from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table} where "{row["COLUMN_NAME"]}" = ''"""
                    )
                    empty_strings_count = result.fetchone()[0]  # type: ignore
                    if empty_strings_count > 0:
                        self.column_info.at[index, "EMPTY_STRINGS"] = True
                    else:
                        self.column_info.at[index, "EMPTY_STRINGS"] = False
                else:
                    self.column_info.at[index, "EMPTY_STRINGS"] = ""

    def _add_has_zeros(self):
        if self.debug == True:
            print("Checking for zero values...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                if row["DATA_TYPE"] == "NUMBER":
                    result = self.cursor.execute(
                        f"""select count(1) from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table} where "{row["COLUMN_NAME"]}" = 0"""
                    )
                    zeros_count = result.fetchone()[0]  # type: ignore

                    if zeros_count > 0:
                        self.column_info.at[index, "ZEROS"] = True
                    else:
                        self.column_info.at[index, "ZEROS"] = False
                else:
                    self.column_info.at[index, "ZEROS"] = ""

    def _add_distinct_count(self):
        if self.debug == True:
            print("Checking distinct count...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                result = self.cursor.execute(
                    f"""select count(distinct("{row["COLUMN_NAME"]}")) from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"""
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
                        f"""select listagg(distinct("{row["COLUMN_NAME"]}"), ', ') from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"""
                    )
                    values = result.fetchone()[0]  # type: ignore

                    self.column_info.at[index, "VALUES"] = values
                else:
                    self.column_info.at[index, "VALUES"] = (
                        f"> {self.max_distinct} values"
                    )

    def _add_unique(self):
        if self.debug == True:
            print("Checking if column is unique...", end="", flush=True)
        with Timer(output=self.debug):
            for index, row in self.column_info.iterrows():
                result = self.cursor.execute(
                    f"""select case when count(1) = count(distinct "{row["COLUMN_NAME"]}") then true else false end from {self.snowflake_database}.{self.snowflake_schema}.{self.snowflake_table}"""
                )

                self.column_info.at[index, "UNIQUE"] = result.fetchone()[0]  # type: ignore
