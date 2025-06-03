"""
Migration v2: Add pending_action and remote_synced columns to messages table.

This migration adds:
1. pending_action (TEXT) - tracks pending actions ('insert' or 'delete')
2. remote_synced (BOOLEAN) - tracks whether the pending action has been synced
"""

import logging
from peewee import TextField, BooleanField
from playhouse.migrate import SqliteMigrator, migrate

from ..db import database_proxy
from ..utils import column_exists


logger = logging.getLogger(__name__)


def run() -> bool:
    """
    Add the pending_action and remote_synced columns to the messages table if they don't exist.

    Returns:
        bool: True if the migration was successful or columns already exist,
              False if the migration failed.
    """
    table_name = "messages"
    columns = {
        "pending_action": TextField(null=True),
        "remote_synced": BooleanField(default=False)
    }

    try:
        migrator = SqliteMigrator(database_proxy.obj)
        
        for column_name, field in columns.items():
            if column_exists(table_name, column_name):
                logger.info(f"Column {column_name} already exists in {table_name} table")
                continue

            logger.info(f"Adding {column_name} column to {table_name} table")
            migrate(migrator.add_column(table_name, column_name, field))

            # Set default values for existing rows
            if column_name == "remote_synced":
                database_proxy.obj.execute_sql(
                    f"UPDATE {table_name} SET {column_name} = ? WHERE {column_name} IS NULL",
                    (False,)
                )

        logger.info(f"Successfully added columns to {table_name} table")
        return True

    except Exception as e:
        logger.error(f"Failed to add columns: {e}")
        return False 