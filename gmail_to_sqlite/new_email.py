from gmail_to_sqlite import db
from gmail_to_sqlite.utils import create_message_for_sending

# First, initialize the database
db_conn = db.init("./data")

# Create a message that will be sent to Gmail
message_id = create_message_for_sending(
    from_email="neal@wordware.ai",
    from_name="Neal Malhotra", 
    to_emails=["nealm@berkeley.edu"],
    subject="Wordware testing",
    body="this is a test from my wordware account"
)

print(f"Created message with ID: {message_id}")
print("Message is marked for sending (pending_action='insert', remote_synced=False)")

# If you want to mark an EXISTING message for deletion, uncomment below:
# Replace 'existing-gmail-message-id' with an actual Gmail message ID from your database
"""
db.Message.update(
    pending_action='delete',
    remote_synced=False
).where(
    db.Message.message_id == 'existing-gmail-message-id'
).execute()
print("Marked existing message for deletion")
"""

# Check what's pending
pending_inserts = db.get_pending_inserts()
pending_deletes = db.get_pending_deletes()

print(f"\nCurrent pending operations:")
print(f"Messages to send: {len(pending_inserts)}")
print(f"Messages to delete: {len(pending_deletes)}")

# Close the database connection
db_conn.close()

print(f"\nTo sync these changes to Gmail, run:")
print(f"python main.py sync-to-gmail --data-dir ./data")