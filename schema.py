from tortoise.connection import get_connection
from tortoise.transactions import in_transaction


_CARD_TABLE_SQL = '''CREATE TABLE "{name}" (
    "id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    "uuid" CHAR(36) NOT NULL UNIQUE,
    "uid" VARCHAR(50) NOT NULL UNIQUE,
    "is_active" INT NOT NULL,
    "issued_at" TIMESTAMP NOT NULL,
    "producer_id" INT NOT NULL REFERENCES "producer" ("id") ON DELETE CASCADE,
    "species_id" INT {required}REFERENCES "species" ("id") ON DELETE CASCADE
)'''


async def migrate_optional_card_species() -> bool:
    """Upgrade the previous card cache schema without replacing any stored data."""
    connection = get_connection("default")
    columns = await connection.execute_query_dict('PRAGMA table_info("rfid_card")')
    if any(column["name"] == "species_id" and not column["notnull"] for column in columns):
        return False

    tables = await connection.execute_query_dict(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'rfid_card'"
    )
    expected = _CARD_TABLE_SQL.format(name="rfid_card", required="NOT NULL ")
    if not tables or " ".join(tables[0]["sql"].split()) != " ".join(expected.split()):
        raise ValueError("Unsupported RFID card schema; database was not changed")

    foreign_keys = (await connection.execute_query_dict("PRAGMA foreign_keys"))[0]["foreign_keys"]
    await connection.execute_query("PRAGMA foreign_keys = OFF")
    try:
        async with in_transaction() as transaction:
            objects = await transaction.execute_query_dict(
                "SELECT sql FROM sqlite_master WHERE tbl_name = 'rfid_card' "
                "AND type IN ('index', 'trigger') AND sql IS NOT NULL"
            )
            sequence = await transaction.execute_query_dict(
                "SELECT seq FROM sqlite_sequence WHERE name = 'rfid_card'"
            )
            # Execute each statement separately: executescript() can implicitly commit.
            await transaction.execute_query(
                _CARD_TABLE_SQL.format(name="rfid_card_optional_species", required="")
            )
            await transaction.execute_query(
                'INSERT INTO "rfid_card_optional_species" '
                '(id, uuid, uid, is_active, issued_at, producer_id, species_id) '
                'SELECT id, uuid, uid, is_active, issued_at, producer_id, species_id FROM "rfid_card"'
            )
            await transaction.execute_query('DROP TABLE "rfid_card"')
            await transaction.execute_query('ALTER TABLE "rfid_card_optional_species" RENAME TO "rfid_card"')
            if sequence:
                await transaction.execute_query(
                    "UPDATE sqlite_sequence SET seq = ? WHERE name = 'rfid_card'", [sequence[0]["seq"]]
                )
            for item in objects:
                await transaction.execute_query(item["sql"])
            if await transaction.execute_query_dict("PRAGMA foreign_key_check"):
                raise ValueError("Foreign key validation failed during RFID card schema upgrade")
    finally:
        await connection.execute_query(f"PRAGMA foreign_keys = {int(foreign_keys)}")
    return True
