import os
from jinja2 import Environment, FileSystemLoader, Template
import psycopg2
from collections import defaultdict
import click
from os.path import dirname, join
from fnmatch import fnmatch

application_name = "pg-diff"

IGNORE_SCHEMAS = {"sqitch", "audit", "pg_*", "information_schema"}


class DatabaseProxy:
    def __init__(self, host: str, user: str):
        self.host = host
        self.user = user

    def get_connection(self, database: str) -> psycopg2.extensions.connection:
        return psycopg2.connect(host=self.host, user=self.user, database=database)

    def get_schemas(self, database: str) -> list[str]:
        """Get a list of schemas from the database"""
        sql: str = (
            "select nspname as schema_name from pg_namespace order by schema_name;"
        )
        with self.get_connection(database) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                return [
                    row[0]
                    for row in rows
                    if not any(fnmatch(row[0], pattern) for pattern in IGNORE_SCHEMAS)
                ]

    def get_schema_tables(self, database: str, schemas: list[str]) -> dict:

        snuttify = lambda l: [f"'{x}'" for x in l]
        where_clause: str = (
            f"where tc.table_schema in ({', '.join(snuttify(schemas))})"
            if len(schemas or []) > 0
            else ""
        )
        sql: str = f"""
            select tc.table_schema, tc.table_name, string_agg(kc.column_name, ','), count(*)
            from information_schema.table_constraints tc
            join information_schema.key_column_usage kc
            on tc.constraint_type = 'PRIMARY KEY'
            and kc.table_name = tc.table_name and kc.table_schema = tc.table_schema
            and kc.constraint_name = tc.constraint_name
            { where_clause}
            group by tc.table_schema, tc.table_name
            order by 1, 2;
        """
        with self.get_connection(database) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                rows = cursor.fetchall()
                data = defaultdict(list)
                for row in rows:
                    if row[3] == 0:
                        print(f"info: skipping {row[0]}.{row[1]} key count {row[3]}")
                        continue

                    data[row[0]].append(
                        dict(table_name=row[1], primary_key=row[2], key_count=row[3])
                    )

                return dict(data)


def get_template(template_name: str) -> Template:
    """Get a template from the templates directory"""
    return Environment(
        loader=FileSystemLoader(join(dirname(__file__), "templates"))
    ).get_template(template_name)


@click.command()
@click.argument("filename", type=click.STRING)
@click.option(
    "-h", "--host", type=click.STRING, help="Database server", default="", required=True
)
@click.option(
    "-u", "--user", type=click.STRING, help="Username", default="", required=True
)
@click.option(
    "-sd", "--source-database", type=click.STRING, help="Source database", required=True
)
@click.option(
    "-td", "--target-database", type=click.STRING, help="Target database", required=True
)
@click.option("-s", "--schema", type=click.STRING, multiple=True, help="Schema")
@click.option("-o", "--output-dir", type=click.STRING, help="Output directory")
@click.option(
    "-dc",
    "--data-compare",
    type=click.BOOL,
    is_flag=True,
    help="Compare data",
    default=False,
)
@click.option(
    "-ds",
    "--data-schema",
    type=click.STRING,
    multiple=True,
    help="Compare data schemas",
)
@click.option("-r", "--roles", type=click.STRING, multiple=True, help="Roles")
@click.option("-a", "--author", type=click.STRING, default=None, help="Author")
@click.option(
    "-p",
    "--project",
    type=click.STRING,
    default="development",
    help="Project name in options file",
)
def generate(
    filename: str,
    host: str,
    user: str,
    source_database: str,
    target_database: str,
    schema: list[str],
    output_dir: str = "./output",
    data_compare: bool = False,
    data_schema: list[str] = None,
    roles: list[str] = None,
    author: str | None = None,
    project: str | None = None,
):
    data_tables = None

    db: DatabaseProxy = DatabaseProxy(host, user)

    schemas: list[str] = schema or db.get_schemas(source_database)

    if data_compare:
        data_tables = db.get_schema_tables(
            database=source_database,
            schemas=data_schema if data_schema else schemas,
        )

    config = {
        "application_name": application_name,
        "author": author or os.environ.get("USER", "Humlab").title(),
        "project": project or "development",
        "source": {
            "host": host,
            "database": source_database,
            "user": user,
        },
        "target": {
            "host": host,
            "database": target_database,
            "user": user,
        },
        "outputDirectory": output_dir,
        "namespaces": schemas,
        "roles": roles or [],
        "data_compare": data_compare,
        "data_tables": data_tables or {},
    }

    content: str = get_template("options.jinja2").render(config)
    with open(filename, mode="w", encoding="utf-8") as fp:
        fp.write(content)

        click.echo(
            f"{application_name} --compare {project} diff.sql --config-file {filename}"
        )
        click.echo(f"info: wrote {filename}")


if __name__ == "__main__":

    generate()

    # generate(
    #     host="humlabseadserv.srv.its.umu.se",
    #     user="humlab_admin",
    #     source_database="sead_staging",
    #     target_database="sead_staging_test",
    #     schemas=[
    #         "bugs_import",
    #         "clearing_house",
    #         "clearing_house_commit",
    #         "facet",
    #         "postgrest_api",
    #         "postgrest_default_api",
    #         "public",
    #         "sead_utility"
    #     ],
    #     roles=["humlab_admin"],
    #     data_schema=["public", "facet"],
    #     filename="compare_output.json"
    # )
