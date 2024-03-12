import os, sys
from snowflake_tools.ArgumentParserTweaked import ArgumentParserTweaked
from snowflake_tools import snowflake_config
from snowflake_tools.Snowflake import Snowflake
from importlib.metadata import version

project_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
snowflake_tools_version = version("snowflake-tools")


def dump(obj):
    for attr in dir(obj):
        print("obj.%s = %r" % (attr, getattr(obj, attr)))


def cli():
    tool_name = "snowflake-get-ddl"
    epilog = f"Example: {tool_name} --profile bd --type TABLE --name ARCHTICS.DC_DATA.AUDIT_SUMMARY"

    parser = ArgumentParserTweaked(
        description=f"Get ddl for an object v{snowflake_tools_version}.",
        epilog=epilog,
        exit_on_error=False,
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

    try:
        args = parser.parse_args()
    except Exception as e:
        print(f"Error: {e}\n")
        print(f"Help: {tool_name} -h\n")
        parser.print_usage()
        print(f"\n{epilog}")
        sys.exit(2)

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

        print(snowflake.get_ddl(args.type, args.name))
    except Exception as e:
        print(f'{e}"\n"{epilog}')
