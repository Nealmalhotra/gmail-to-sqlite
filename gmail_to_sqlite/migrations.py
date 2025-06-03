"""
Database migrations for gmail-to-sqlite.

This module contains migration functions to update the database schema
when new features are added or existing schema needs to be modified.
"""

import logging
from typing import List, Optional

from peewee import BooleanField, SQL
from playhouse.migrate import SqliteMigrator, migrate

from .db import database_proxy, Message, SchemaVersion
from .schema_migrations.v1_add_is_deleted_column import run as migration_v1_run
from .schema_migrations.v2_add_pending_action_columns import run as migration_v2_run


logger = logging.getLogger(__name__)

# List of migration functions in order
MIGRATIONS: List[callable] = [
    migration_v1_run,
    migration_v2_run,
]


def get_schema_version() -> int:
    """
    Get the current database schema version.

    Returns:
        int: The current schema version, or 0 if no version is tracked.
    """
    try:
        version_record = SchemaVersion.select().first()
        return version_record.version if version_record else 0
    except Exception as e:
        logger.debug(f"Schema version table doesn't exist or error occurred: {e}")
        return 0


def set_schema_version(version: int) -> bool:
    """
    Set the database schema version.

    Args:
        version (int): The schema version to set.

    Returns:
        bool: True if successful, False otherwise.
    """
    try:
        database_proxy.obj.create_tables([SchemaVersion], safe=True)

        # Delete existing version record and insert new one
        SchemaVersion.delete().execute()
        SchemaVersion.create(version=version)
        logger.info(f"Schema version set to {version}")
        return True
    except Exception as e:
        logger.error(f"Failed to set schema version to {version}: {e}")
        return False


def run_migrations() -> bool:
    """
    Run all necessary migrations for the database.

    Returns:
        bool: True if all migrations were successful, False otherwise.
    """
    logger.info("Running database migrations...")

    try:
        current_version = get_schema_version()
        logger.info(f"Current schema version: {current_version}")

        if current_version == 0:
            logger.info("Running migration v1: add is_deleted column")
            if migration_v1_run():
                if set_schema_version(1):
                    logger.info("Migration v1 completed successfully, version set to 1")
                else:
                    logger.error("Failed to set schema version to 1")
                    return False
            else:
                logger.error("Migration v1 failed")
                return False
        elif current_version == 1:
            logger.info("Running migration v2: add pending_action columns")
            if migration_v2_run():
                if set_schema_version(2):
                    logger.info("Migration v2 completed successfully, version set to 2")
                else:
                    logger.error("Failed to set schema version to 2")
                    return False
            else:
                logger.error("Migration v2 failed")
                return False
        elif current_version >= 2:
            logger.info(
                f"Database already at version {current_version}, no migrations needed"
            )
        else:
            logger.warning(f"Unexpected schema version {current_version}")

        logger.info("All migrations completed successfully")
        return True

    except Exception as e:
        logger.error(f"Error during migrations: {e}")
        return False
