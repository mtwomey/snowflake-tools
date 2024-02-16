import os, sys, argparse
import snowflake.connector
import pandas as pd
from snowflake_tools import snowflake_config

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


class Snowflake:
    def __init__(self, connection_config, debug=False):
        self.connection_config = connection_config
        self.connection = snowflake.connector.connect(
            user=connection_config["user"],
            password=connection_config["password"],
            account=connection_config["account"],
        )
        self.cursor = self.connection.cursor()

        self.debug = debug

    def mirror_db_permissions(
        self, source_db, source_grantee, target_db, target_grantee
    ):

        self.cursor.execute(
            f"select * from {source_db}.information_schema.object_privileges"
        )
        grants = self.cursor.fetch_pandas_all()
        grant_rows = grants.loc[
            (grants["GRANTEE"] == source_grantee)
            & (grants["OBJECT_TYPE"] == "DATABASE")
            & (grants["OBJECT_NAME"] == source_db),
            ["PRIVILEGE_TYPE", "GRANTEE"],
        ]

        new_grants = [
            f"grant {row['PRIVILEGE_TYPE']} on database {target_db} to {target_grantee};"
            for row in grant_rows.to_dict(orient="records")
        ]

        self.cursor.execute(f"show future grants in database {source_db}")
        privileges_columns = [desc[0] for desc in self.cursor.description]
        future_grants = pd.DataFrame(self.cursor.fetchall(), columns=privileges_columns)
        future_grant_rows = future_grants.loc[
            (future_grants["grantee_name"] == source_grantee),
            [
                "privilege",
                "grantee_name",
                "grant_on",
            ],
        ]

        new_future_grants = [
            f"grant {row['privilege']} on future {row['grant_on']}S in database {target_db} to {target_grantee};"
            for row in future_grant_rows.to_dict(orient="records")
        ]

        return new_grants + new_future_grants


def cli():

    parser = argparse.ArgumentParser(
        description="Get SQL to mirror permissions from another table."
    )

    parser.add_argument(
        "--profile",
        help="Profile name.",
        required=True,
    )

    parser.add_argument(
        "--source-db",
        help="Source database.",
        required=True,
    )

    parser.add_argument(
        "--source-grantee",
        help="Source grantee.",
        required=True,
    )

    parser.add_argument(
        "--target-db",
        help="Target database.",
        required=True,
    )

    parser.add_argument(
        "--target-grantee",
        help="Target grantee.",
        required=True,
    )

    # poetry run permissions --profile bd --source-db BD_DEV_PRD --source-grantee DATA_ENGINEERING --target-db ARCHTICS --target-grantee DATA_ENGINEERING
    # snowflake-mirror-permissions --profile bd --source-db BD_DEV_PRD --source-grantee DATA_ENGINEERING --target-db ARCHTICS --target-grantee DATA_ENGINEERING

    args = parser.parse_args()
    # args = parser.parse_args(
    #     [
    #         "--profile",
    #         "bd",
    #         "--source-db",
    #         "BD_DEV_PRD",
    #         "--source-grantee",
    #         "DATA_ENGINEERING",
    #         "--target-db",
    #         "ARCHTICS",
    #         "--target-grantee",
    #         "DATA_ENGINEERING",
    #     ]
    # )

    if args.profile is None:
        parser.print_help()
    else:
        try:
            config = snowflake_config.get_profile(args.profile)
            snowflake = Snowflake(
                {
                    "user": config["user"],
                    "password": config["password"],
                    "account": config["account"],
                },
                debug=True,
            )

            for row in snowflake.mirror_db_permissions(
                args.source_db, args.source_grantee, args.target_db, args.target_grantee
            ):
                print(row)
        except Exception as e:
            print(e)
