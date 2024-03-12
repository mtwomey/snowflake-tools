import os, sys, argparse
import snowflake.connector
import pandas as pd
from snowflake_tools import snowflake_config
from snowflake_tools.Snowflake import Snowflake

# import pkg_resources
from importlib.metadata import version

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
# version = pkg_resources.get_distribution("snowflake-tools").version
snowflake_tools_version = version("snowflake-tools")


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def cli():

    parser = argparse.ArgumentParser(
        description=f"Get ddl for an object v{snowflake_tools_version}.",
        epilog="Example: snowflake-get-ddl --profile bd --type TABLE --name ARCHTICS.DC_DATA.AUDIT_SUMMARY",
    )

    parser.add_argument(
        "--profile",
        help="Profile name",
        required=True,
    )

    parser.add_argument(
        "--type",
        help="Object type",
        required=True,
    )

    parser.add_argument(
        "--name",
        help="Object name",
        required=True,
    )

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

            print(snowflake.get_ddl(args.type , args.name))
        except Exception as e:
            print(e)
