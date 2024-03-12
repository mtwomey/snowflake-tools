import snowflake.connector
import pandas as pd

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

    def exec(self, statement):
        self.cursor.execute(statement)
        return self.cursor.fetchall()

    def get_ddl(self, type, name):
        return self.exec(f"select get_ddl('{type}', '{name}')")[0][0]
