import psycopg2
from collections import defaultdict
import click

def get_schema_tables( host: str, user: str, database: str, schemas: list[str] ) -> dict:

    snuttify = lambda l: [f"'{x}'" for x in l]
    connection = psycopg2.connect( host=host, user=user, database=database)
    schema_clause: str = ""
    if schemas:
        if isinstance(schemas, str):
            schemas = [schemas]
        schema_clause = f"and tc.table_schema in ({', '.join(snuttify(schemas))})"

    sql: str = f"""
    with
        table_columns as (
            select t_1.oid, ns.nspname, t_1.relname, attr.attname, attr.attnum
            from pg_class t_1
            join pg_namespace ns on ns.oid = t_1.relnamespace
            join pg_attribute attr on attr.attrelid = t_1.oid and attr.attnum > 0
        ),
        foreign_keys as (
            select distinct
                t.nspname as schema_name,
                t.oid     as table_oid,
                t.relname as table_name,
                t.attname as column_name,
                t.attnum,
                s.nspname as f_schema_name,
                s.relname as f_table_name,
                s.attname as f_column_name,
                s.oid     as f_table_oid,
                t.attnum  as f_attnum
            from pg_constraint
            join table_columns t on t.oid = pg_constraint.conrelid
             and t.attnum = pg_constraint.conkey[1]
             and (t.attnum = any (pg_constraint.conkey))
            join table_columns s on s.oid = pg_constraint.confrelid
             and (s.attnum = any (pg_constraint.confkey))
            where pg_constraint.contype = 'f'::"char"
        )
    select
        pg_tables.schemaname::information_schema.sql_identifier                     as table_schema,
        pg_tables.tablename::information_schema.sql_identifier                      as table_name,
        pg_attribute.attname::information_schema.sql_identifier                     as column_name,
        pg_attribute.attnum::information_schema.cardinal_number                     as ordinal_position,
        format_type(pg_attribute.atttypid, null)::information_schema.character_data as data_type,
        case pg_attribute.atttypid
            when 21 /*int2*/ then 16
            when 23 /*int4*/ then 32
            when 20 /*int8*/ then 64
            when 1700 /*numeric*/ then case
                when PG_ATTRIBUTE.ATTTYPMOD = - 1 then null
                else ((PG_ATTRIBUTE.ATTTYPMOD - 4) >> 16) & 65535 -- calculate the precision
            end
            when 700 /*float4*/ then 24 /*flt_mant_dig*/
            when 701 /*float8*/ then 53 /*dbl_mant_dig*/
            else null
        end::information_schema.cardinal_number as numeric_precision,
        case
            when pg_attribute.atttypid in (21, 23, 20) then 0
            when pg_attribute.atttypid in (1700) then case
                when pg_attribute.atttypmod = - 1 then null
                else (pg_attribute.atttypmod - 4) & 65535 -- calculate the scale
            end
            else null
        end::information_schema.cardinal_number as numeric_scale,
        case
            when pg_attribute.atttypid not in (1042, 1043)
            or pg_attribute.atttypmod = - 1 then null
            else pg_attribute.atttypmod - 4
        end::information_schema.cardinal_number as character_maximum_length,
        case pg_attribute.attnotnull
            when false then 'YES'
            else 'NO'
        end::information_schema.yes_or_no as is_nullable,
        case
            when pk.contype is null then 'NO' else 'YES'
            end::information_schema.yes_or_no as is_pk,
        case
            when fk.table_oid is null then 'NO'
            else 'YES'
        end::information_schema.yes_or_no as is_fk,
        fk.f_table_name::information_schema.sql_identifier,
        fk.f_column_name::information_schema.sql_identifier
    from
        pg_tables
        join pg_class on pg_class.relname = pg_tables.tablename
        join pg_namespace ns on ns.oid = pg_class.relnamespace
        and ns.nspname = pg_tables.schemaname
        join pg_attribute on pg_class.oid = pg_attribute.attrelid
        and pg_attribute.attnum > 0
        left join pg_constraint pk on pk.contype = 'p'::"char"
        and pk.conrelid = pg_class.oid
        and (pg_attribute.attnum = any (pk.conkey))
        left join foreign_keys as fk on fk.table_oid = pg_class.oid
        and fk.attnum = pg_attribute.attnum
    where
        true --and (p_owner is null or pg_tables.tableowner = p_owner)
        and pg_attribute.atttypid <> 0::oid
        { schema_clause }
    order by table_name, ordinal_position asc;
    """
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    data = defaultdict(list)

    for row in rows:

        if row[3] != 1:
            print(f"info: skipping {row[0]}.{row[1]} key count {row[3]}")
            continue

        data[row[0]].append(dict(
            table_name=row[1],
            primary_key=row[2],
            key_count=row[3])
        )

    return dict(data)

@click.command()
@click.argument('filename', type=click.STRING)
@click.option('-h', '--host', type=click.STRING, help='Database server', default="")
@click.option('-u', '--user', type=click.STRING, help='Username', default="")
@click.option('-sd', '--source-database', type=click.STRING, help='Source database')
@click.option('-td', '--target-database', type=click.STRING, help='Target database')
@click.option('-s', '--schema', type=click.STRING, multiple=True, help='Schema')
@click.option('-o', '--output-dir', type=click.STRING, help='Output directory')
@click.option('-dc', '--data-compare', type=click.BOOL, is_flag=True, help='Compare data', default=False)
@click.option('-ds', '--data-schema', type=click.STRING, multiple=True, help='Compare data schemas')
@click.option('-r', '--roles', type=click.STRING, multiple=True, help='Roles')
def generate(
    filename: str,
    host: str,
    user: str,
    source_database: str,
    target_database: str,
    schema: list[str],
    output_dir: str="./output",
    data_compare: bool=False,
    data_schema: list[str]=None,
    roles: list[str]=None,
):
    data_tables = None
    if data_compare:
        data_tables = get_schema_tables(host=host, user=user, database=source_database, schemas=data_schema if data_schema else schema)

    config = {
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
        "author": "Roger Mähler",
        "outputDirectory": output_dir,
        "namespaces": schema,
        "roles": roles or [],
        "data_compare": len(data_tables or {}) > 0,
        "data_tables": data_tables or {},
    }

    content: str = Environment().from_string(TEMPLATE).render(config)
    with open(filename, mode="w", encoding="utf-8") as fp:
        fp.write(content)
        print(f"info: wrote {filename}")

if __name__ == '__main__':

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
