import os
import toml

def get_profile(profile_name):
    with open(os.path.expanduser("~/.snowflake-tools"), "r") as file:
        config = toml.load(file)
    return config["profiles"][profile_name]
