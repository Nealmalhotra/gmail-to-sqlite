#!/usr/bin/env python3
"""
Example script showing how to use gmail-to-sqlite's bidirectional sync functionality.

This script demonstrates:
1. Creating a local message that will be sent to Gmail
2. Marking an existing message for deletion
3. Syncing local changes to Gmail
"""

import logging
from gmail_to_sqlite import auth, db
from gmail_to_sqlite.sync import sync_local_changes_to_gmail
from gmail_to_sqlite.utils import create_message_for_sending

def setup_logging():
    """Set up basic logging."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s'
    )

def create_and_send_message(data_dir: str):
    """Create a local message and sync it to Gmail."""
    
    # Initialize database
    db_conn = db.init(data_dir)
    
    # Create a local message that will be sent to Gmail
    message_id = create_message_for_sending(
        from_email="your-email@gmail.com",  # Replace with your Gmail address
        from_name="Your Name",
        to_emails=["recipient@example.com"],  # Replace with actual recipient
        subject="Test message from gmail-to-sqlite",
        body="This is a test message created locally and sent via Gmail API.\n\nBest regards,\nYour Name",
        cc_emails=None,  # Optional: ["cc@example.com"]
        thread_id=None   # Optional: specify if replying to existing thread
    )
    
    print(f"Created local message with ID: {message_id}")
    print("Message is marked for sending to Gmail (pending_action='insert', remote_synced=False)")
    
    db_conn.close()
    return message_id

def mark_message_for_deletion(data_dir: str, gmail_message_id: str):
    """Mark an existing message for deletion from Gmail."""
    
    # Initialize database
    db_conn = db.init(data_dir)
    
    try:
        # Update the message to mark it for deletion
        db.Message.update(
            pending_action='delete',
            remote_synced=False
        ).where(
            db.Message.message_id == gmail_message_id
        ).execute()
        
        print(f"Marked message {gmail_message_id} for deletion from Gmail")
        
    except Exception as e:
        print(f"Error marking message for deletion: {e}")
    finally:
        db_conn.close()

def sync_to_gmail(data_dir: str):
    """Sync all pending local changes to Gmail."""
    
    # Get credentials
    credentials = auth.get_credentials(data_dir)
    
    # Initialize database
    db_conn = db.init(data_dir)
    
    try:
        # Get pending operations for logging
        pending_inserts = db.get_pending_inserts()
        pending_deletes = db.get_pending_deletes()
        
        print(f"Found {len(pending_inserts)} messages to send to Gmail")
        print(f"Found {len(pending_deletes)} messages to delete from Gmail")
        
        if pending_inserts or pending_deletes:
            # Sync local changes to Gmail
            operations_count = sync_local_changes_to_gmail(credentials)
            print(f"Completed sync with {operations_count} operations")
        else:
            print("No pending changes to sync")
            
    except Exception as e:
        print(f"Error during sync: {e}")
    finally:
        db_conn.close()

def main():
    """Main function demonstrating the functionality."""
    setup_logging()
    
    data_dir = "./data"  # Update this path as needed
    
    print("=== Gmail-to-SQLite Bidirectional Sync Example ===\n")
    
    try:
        # Example 1: Create and send a message
        print("1. Creating a local message for sending to Gmail...")
        message_id = create_and_send_message(data_dir)
        print(f"   Created message: {message_id}\n")
        
        # Example 2: Mark an existing message for deletion (optional)
        # Uncomment and replace with actual message ID to test deletion
        # print("2. Marking a message for deletion...")
        # mark_message_for_deletion(data_dir, "existing-gmail-message-id")
        # print("   Message marked for deletion\n")
        
        # Example 3: Sync changes to Gmail
        print("3. Syncing local changes to Gmail...")
        sync_to_gmail(data_dir)
        print("   Sync completed\n")
        
        print("=== Example completed successfully ===")
        print("\nNote: Make sure you have:")
        print("- Valid Gmail API credentials (credentials.json)")
        print("- Proper permissions (gmail.send and gmail.modify scopes)")
        print("- Updated the email addresses in this script")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 