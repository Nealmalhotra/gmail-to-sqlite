import argparse
import logging
import os
import signal
import sys
from typing import Any, Callable, List, Optional
import json
import sqlite3
from googleapiclient.errors import HttpError

from . import auth, db, sync
from .constants import DEFAULT_WORKERS, LOG_FORMAT
from .utils import build_raw_message


class ApplicationError(Exception):
    """Custom exception for application-level errors."""

    pass


def prepare_data_dir(data_dir: str) -> None:
    """
    Create the data directory if it doesn't exist.

    Args:
        data_dir (str): The path where to store data.

    Raises:
        ApplicationError: If directory creation fails.
    """
    try:
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    except Exception as e:
        raise ApplicationError(f"Failed to create data directory {data_dir}: {e}")


def setup_signal_handler(
    shutdown_requested: Optional[List[bool]] = None,
    executor: Any = None,
    futures: Any = None,
) -> Any:
    """
    Set up a signal handler for graceful shutdown.

    Args:
        shutdown_requested: Mutable container for shutdown state.
        executor: The executor instance to manage task cancellation.
        futures: Dictionary mapping futures to their IDs.

    Returns:
        The original signal handler.
    """

    def handle_sigint(sig: Any, frame: Any) -> None:
        if shutdown_requested is not None:
            if not shutdown_requested[0]:
                logging.info(
                    "Shutdown requested. Waiting for current tasks to complete..."
                )
                shutdown_requested[0] = True

                # Cancel non-running futures if provided
                if executor and futures:
                    for future in list(futures.keys()):
                        if not future.running():
                            future.cancel()
            else:
                logging.warning("Forced shutdown. Exiting immediately.")
                sys.exit(1)
        else:
            logging.warning(
                "Forced shutdown. No graceful shutdown available. Exiting immediately."
            )
            sys.exit(1)

    original_sigint_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, handle_sigint)
    return original_sigint_handler


def setup_logging() -> None:
    """Set up application logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        handlers=[logging.StreamHandler()],
    )


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the argument parser."""
    parser = argparse.ArgumentParser(
        description="Sync Gmail messages to SQLite database"
    )

    # Add subcommands
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Sync command
    sync_parser = subparsers.add_parser("sync", help="Sync messages from Gmail")
    sync_parser.add_argument(
        "--data-dir", required=True, help="Directory to store the database"
    )
    sync_parser.add_argument(
        "--full-sync",
        action="store_true",
        help="Perform a full sync instead of incremental",
    )
    sync_parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"Number of worker threads (default: {DEFAULT_WORKERS})",
    )

    # Sync message command
    sync_message_parser = subparsers.add_parser(
        "sync-message", help="Sync a specific message"
    )
    sync_message_parser.add_argument(
        "--data-dir", required=True, help="Directory to store the database"
    )
    sync_message_parser.add_argument(
        "--message-id", required=True, help="ID of the message to sync"
    )

    # Sync deleted messages command
    sync_deleted_parser = subparsers.add_parser(
        "sync-deleted-messages", help="Detect and mark deleted messages"
    )
    sync_deleted_parser.add_argument(
        "--data-dir", required=True, help="Directory to store the database"
    )

    # Sync to Gmail command
    sync_to_gmail_parser = subparsers.add_parser(
        "sync-to-gmail", help="Sync local changes to Gmail"
    )
    sync_to_gmail_parser.add_argument(
        "--data-dir", required=True, help="Directory to store the database"
    )

    return parser


def main() -> None:
    """Main application entry point."""
    setup_logging()

    try:
        parser = create_argument_parser()
        args = parser.parse_args()

        # Validate command-specific arguments
        if args.command == "sync-message" and not args.message_id:
            parser.error("--message-id is required for sync-message command")

        prepare_data_dir(args.data_dir)
        credentials = auth.get_credentials(args.data_dir)

        # Set up shutdown handling
        shutdown_state = [False]

        def check_shutdown() -> bool:
            return shutdown_state[0]

        original_sigint_handler = setup_signal_handler(
            shutdown_requested=shutdown_state
        )

        try:
            db_conn = db.init(args.data_dir)

            if args.command == "sync":
                sync.all_messages(
                    credentials,
                    full_sync=args.full_sync,
                    num_workers=args.workers,
                    check_shutdown=check_shutdown,
                )
            elif args.command == "sync-message":
                sync.single_message(
                    credentials, args.message_id, check_shutdown=check_shutdown
                )
            elif args.command == "sync-deleted-messages":
                sync.sync_deleted_messages(credentials, check_shutdown=check_shutdown)
            elif args.command == "sync-to-gmail":
                operations_count = sync.sync_local_changes_to_gmail(
                    credentials, check_shutdown=check_shutdown
                )
                logging.info(f"Completed sync to Gmail with {operations_count} operations")

            db_conn.close()
            logging.info("Operation completed successfully")

        except (auth.AuthenticationError, db.DatabaseError, sync.SyncError) as e:
            logging.error(f"Operation failed: {e}")
            sys.exit(1)
        except Exception as e:
            logging.error(f"Unexpected error: {e}")
            sys.exit(1)
        finally:
            signal.signal(signal.SIGINT, original_sigint_handler)

    except KeyboardInterrupt:
        logging.info("Operation cancelled by user")
        sys.exit(0)


if __name__ == "__main__":
    main()
