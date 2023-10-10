import os

import yaml

from src.config import Config
from src.utility import dotset


def test_dotset():
    d = {}

    dotset(d, "x", 3)
    assert d == {"x": 3}

    dotset(d, "a.b.c", 1)

    assert d == {"a": {"b": {"c": 1}}, "x": 3}


def test_config():
    config_filename: str = "tests/output/config.yml"

    os.makedirs("tests/output", exist_ok=True)

    expected_config = Config(
        {
            "source": {
                "username": "humle",
                "password": "secret-humle",
                "server": "humle.se",
                "database": "humledb",
            },
            "target": {
                "username": "dumle",
                "password": "secret-dumle",
                "server": "dumle.se",
                "database": "dumledb",
            },
        }
    )
    with open(config_filename, "w", encoding="utf-8") as fp:
        yaml.dump(dict(expected_config), fp)

    config = Config.load(config_filename)

    assert dict(config) == dict(expected_config)

    os.environ["prefix_source:password"] = "#secret-humle#"

    config = Config.load(config_filename, env_prefix="prefix_")

    expected_config["source"]["password"] = "#secret-humle#"

    assert dict(config) == dict(expected_config)


# def test_data_compare():
#     data_compare(
#         config="config/config.yml",
#         schemas=("public",),
#         break_on_diff=False,
#         verbose=True,
#         progress=True,
#         output_file=None,
#     )
