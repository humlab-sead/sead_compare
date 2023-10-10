import pytest
from jinja2 import Template, TemplateNotFound
from src.generate_options import get_template, DatabaseProxy
import dotenv
import os


def test_load_templates():
    with pytest.raises(TemplateNotFound):
        _: Template = get_template("template.sql")

    template: Template = get_template("options.jinja2")
    assert template is not None


@pytest.fixture
def db() -> DatabaseProxy:
    dotenv.load_dotenv()
    return DatabaseProxy(os.environ.get("TEST_HOST"), os.environ.get("TEST_USER"))


def test_database_proxy(db: DatabaseProxy):
    assert db is not None


def test_get_schemas(db: DatabaseProxy):

    with pytest.raises(Exception):
        db.get_schemas(None)

    # schemas: list[str] = get_schemas("host", "user", "database", "schema")
    # assert schemas == ["schema"]

    # schemas: list[str] = get_schemas("host", "user", "database", "schema1", "schema2")
    # assert schemas == ["schema1", "schema2"]
