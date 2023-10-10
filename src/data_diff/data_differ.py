import sys
from dataclasses import dataclass
from functools import cached_property
from logging import ERROR, INFO, WARNING
from urllib.parse import urlparse

import click
import data_diff
import dotenv
from data_diff.sqeleton.databases import postgresql
from loguru import logger
from psycopg2 import sql
from tqdm import tqdm

from src.data_diff.config import Config

dotenv.load_dotenv()


@dataclass
class DbTableInfo:
    schema_name: str
    table_name: str
    primary_keys: tuple[str]
    columns: tuple[str]

    @cached_property
    def timestamp(self) -> str | None:
        return "date_updated" if "date_updated" in self.columns else None

    @cached_property
    def value_columns(self) -> tuple[str]:
        return self.columns if self.timestamp is None else tuple(x for x in self.columns if x != self.timestamp)

    def __eq__(self, __value: object) -> bool:
        if not isinstance(__value, DbTableInfo):
            return False
        return (
            self.schema_name == __value.schema_name
            and self.table_name == __value.table_name
            and self.primary_keys == __value.primary_keys
            and self.columns == __value.columns
            and self.timestamp == __value.timestamp,
        )


class DatabaseProxy:
    """A proxy for a database connection"""

    SQL_COLUMNS_QUERY = """
        with columns as (
            select t.schemaname::information_schema.sql_identifier as schema_name,
                t.tablename::information_schema.sql_identifier as table_name,
                a.attname::information_schema.sql_identifier as column_name,
                a.attnum::information_schema.cardinal_number as ordinal_position,
                case when pk.contype is null then 'NO' else 'YES' end::information_schema.yes_or_no as is_pk
            from pg_tables t
            join pg_class c
            on c.relname = t.tablename
            join pg_namespace ns
            on ns.oid = c.relnamespace
            and ns.nspname = t.schemaname
            join pg_attribute a
            on c.oid = a.attrelid
            and a.attnum > 0
            left join pg_constraint pk
            on pk.contype = 'p'::"char"
            and pk.conrelid = c.oid
            and (a.attnum = any (pk.conkey))
            where a.atttypid <> 0::oid
        )
            select schema_name, table_name,
                string_agg(column_name, ',' order by ordinal_position) filter (where is_pk = 'YES') as primary_keys,
                string_agg(column_name, ',' order by ordinal_position) filter (where is_pk = 'NO') as column_names
            from columns
            where schema_name not in ('pg_catalog', 'information_schema', 'sqitch')
            group by schema_name, table_name
            order by schema_name, table_name;
        """

    def __init__(self, opts: dict):
        self.opts: dict = opts

    @cached_property
    def database(self) -> postgresql.PostgreSQL:
        """Get the database connection"""
        return data_diff.connect(self.uri)

    @cached_property
    def uri(self) -> str:
        """Create a connection url from a dict of options"""
        opts = self.opts
        return f"postgresql://{opts['username']}:{opts['password']}@{opts['server']}/{opts['database']}"

    @cached_property
    def _tables_infos(self) -> list:
        """Find all columns in the database"""
        with self.database.create_connection() as con:
            with con.cursor() as cursor:
                cursor.execute(self.SQL_COLUMNS_QUERY)
                return cursor.fetchall()

    def record_count(self, schema: str, table_name: str) -> int:
        """Find all columns in the database"""
        with self.database.create_connection() as con:
            with con.cursor() as cursor:
                cursor.execute(sql.SQL("select count(*) from {}").format(sql.Identifier(schema, table_name)))
                return cursor.fetchone()[0]

    @cached_property
    def schemas(self) -> dict[str, dict[str, DbTableInfo]]:
        data: dict = {
            schema_name: {
                table_name: DbTableInfo(
                    schema_name,
                    table_name,
                    tuple(pk_keys.split(",")) if pk_keys else None,
                    tuple(columns.split(",")),
                )
                for name, table_name, pk_keys, columns in self._tables_infos
                if schema_name == name
            }
            for schema_name in set(x[0] for x in self._tables_infos)
        }
        return data

    def get_table_segment(self, schema_name: str, table_name: str) -> list:
        table_info: DbTableInfo = self.schemas[schema_name].get(table_name)
        return (
            data_diff.TableSegment(
                database=self.database,
                table_path=(table_info.schema_name, table_info.table_name),
                key_columns=table_info.primary_keys,
                update_column=table_info.timestamp,
                extra_columns=table_info.value_columns,
                case_sensitive=True,
            )
            if table_info
            else None
        )
    
    @property
    def name(self) -> str:
        return self.opts['database']


def log_diff(obj: str, level: int, msg: str, verbose: bool):
    if not verbose and level <= INFO:
        return
    logger.log(level, f"{obj} {msg}")


def data_compare(
    *,
    config: str | Config | tuple[DatabaseProxy, DatabaseProxy] = None,
    schemas: list[str] = None,
    break_on_diff: bool = False,
    verbose: bool = False,
    progress: bool = True,
    output_file: str = None,
) -> bool:
    """Compare two databases"""

    source: DatabaseProxy = None
    target: DatabaseProxy = None

    if isinstance(config, (str, Config)):
        if isinstance(config, str):
            config = Config.load(config)
        source = DatabaseProxy(config["source"])
        target = DatabaseProxy(config["target"])
    elif isinstance(config, tuple):
        source, target = config
    else:
        raise TypeError(f"expected str, Config or tuple, found {type(config)}")

    is_same = True
    for schema_name, tables in source.schemas.items():
        if schemas and schema_name not in schemas:
            log_diff(schema_name, INFO, "not in schemas (skipping)", verbose)
            continue

        if any(table_name not in source.schemas[schema_name] for table_name in target.schemas[schema_name]):
            is_same = False
            log_diff(schema_name, ERROR, "target has more tables than source", verbose)

            if break_on_diff:
                return False

        if schema_name not in target.schemas:
            is_same = False
            log_diff(schema_name, ERROR, "missing in target", verbose)
            if break_on_diff:
                return False
            continue

        def progress_bar(iterable, **kwargs):
            if progress:
                return tqdm(iterable, **kwargs) if progress else iterable
            return iterable

        for table_name, source_table in (pbar := progress_bar(tables.items(), total=len(tables))):
            label: str = f"{schema_name}.{table_name}"
            pbar.set_description(f"{label:.<50}")

            if not source_table.primary_keys:
                log_diff(
                    f"{schema_name}.{table_name}",
                    INFO,
                    "has no primary key (skipped)",
                    verbose,
                )
                continue

            target_table = target.schemas[schema_name].get(table_name)

            if source_table != target_table:
                is_same = False
                log_diff(
                    f"{schema_name}.{table_name}",
                    ERROR,
                    "columns or keys differ",
                    verbose,
                )
                if break_on_diff:
                    return False
                continue

            source_count: int = source.record_count(schema_name, table_name)
            target_count: int = target.record_count(schema_name, table_name)
            if source_count != target_count:
                is_same = False
                log_diff(
                    f"{schema_name}.{table_name}",
                    ERROR,
                    f"record count differs ({source.name}:{source_count} vs {target.name}:{target_count})",
                    verbose
                )
                if break_on_diff:
                    return False
                continue

            if source_count == 0:
                log_diff(f"{schema_name}.{table_name}", INFO, "is empty", verbose)
                continue

            diff_res = data_diff.diff_tables(
                source.get_table_segment(schema_name, table_name),
                target.get_table_segment(schema_name, table_name),
                key_columns=source_table.primary_keys,
                extra_columns=source_table.columns,
            )

            try:
                compare_result = list(diff_res)
            except ValueError as ex:
                log_diff(
                    f"{schema_name}.{table_name}",
                    ERROR,
                    f"diff failed: {ex}",
                    verbose,
                )
                return False

            if compare_result:
                is_same = False
                log_diff(f"{schema_name}.{table_name}", INFO, "data differs", verbose)

                if output_file:
                    """Append diff to file"""

                    with open(output_file, "a", encoding="utf-8") as fp:
                        fp.write(
                            data_diff.format_diff(  # pylint: disable=no-member
                                compare_result,
                                key_columns=source_table.primary_keys,
                                extra_columns=source_table.columns,
                            )
                        )
                if break_on_diff:
                    return False
            else:
                log_diff(f"{schema_name}.{table_name}", INFO, "data is the same", verbose)

    return is_same


def db_urlparse(uri: str) -> dict:
    """Parse a database uri"""
    parsed = urlparse(uri)

    return {
        "username": parsed.username,
        "password": parsed.password,
        "server": parsed.hostname,
        "database": parsed.path[1:],
    }


@click.command()
@click.option("--config", "-c", default=None, help="config file")
@click.option("--source-uri", default=None, help="source database uri")
@click.option("--target-uri", default=None, help="target database uri")
@click.option("--schema", "-s", default=None, multiple=True, help="schemas to compare")
@click.option("--verbose", "-v", default=False, is_flag=True, help="verbose")
@click.option(
    "--break-on-diff/--no-break-on-diff",
    "-b",
    default=True,
    is_flag=True,
    help="break on diff",
)
@click.option("--progress/--no-progress", "-p", default=True, is_flag=True, help="break on diff")
@click.option("--output-file", "-o", default=None, type=str, help="store diff in file")
def main(
    config: str,
    source_uri: str,
    target_uri: str,
    schema: list[str],
    verbose: bool,
    break_on_diff: bool,
    progress: bool,
    output_file: str,
):
    if config is None:
        if source_uri is None or target_uri is None:
            click.echo("error: config or source-uri and target-uri must be specified", err=True)
            sys.exit(1)

        config = Config({"source": db_urlparse(source_uri), "target": db_urlparse(target_uri)}).load_environment(
            "DIFFER_"
        )

    is_same: bool = data_compare(
        config=config,
        schemas=schema,
        verbose=verbose,
        break_on_diff=break_on_diff,
        progress=progress,
        output_file=output_file,
    )

    sys.exit(0 if is_same else 1)


if __name__ == "__main__":
    import logging

    for key in ["table_segment", "diff_tables", "hashdiff_tables", "joindiff_tables"]:
        logging.getLogger(key).setLevel(logging.ERROR)

    main()  # pylint: disable=no-value-for-parameter
