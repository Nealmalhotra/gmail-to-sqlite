"""
Utility functions for gmail-to-sqlite.
"""
import base64
import json
from email.mime.text import MIMEText
import logging

from .db import database_proxy

logger = logging.getLogger(__name__)


def column_exists(table_name: str, column_name: str) -> bool:
    """
    Check if a column exists in a table.

    Args:
        table_name (str): The name of the table.
        column_name (str): The name of the column to check.

    Returns:
        bool: True if the column exists, False otherwise.
    """
    try:
        # PRAGMA table_info doesn't support parameter binding for table names
        cursor = database_proxy.obj.execute_sql(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        return column_name in columns
    except Exception as e:
        logger.error(f"Error checking if column {column_name} exists: {e}")
        return False 

def build_raw_message(row_dict):
    """
    row_dict must contain:
      - row_dict["sender"]    (JSON‐string or Python dict) 
      - row_dict["recipients"] (JSON‐string or Python dict)
      - row_dict["subject"]   (str)
      - row_dict["body"]      (str plaintext; you can extend to HTML if desired)
    Returns the base64url‐encoded string for Gmail API "raw" field.
    """
    # If stored as JSON‐string, parse it into Python dict
    sender = row_dict["sender"]
    if isinstance(sender, str):
        sender = json.loads(sender)
    recips = row_dict["recipients"]
    if isinstance(recips, str):
        recips = json.loads(recips)

    # Build MIMEText (plain‐text)
    msg = MIMEText(row_dict["body"], "plain")
    msg["From"] = sender.get("email")
    msg["To"] = ", ".join([r.get("email") for r in recips.get("to", [])])

    # Optional CC
    if recips.get("cc"):
        msg["Cc"] = ", ".join([r.get("email") for r in recips["cc"]])

    # Subject header
    msg["Subject"] = row_dict["subject"]

    # Gmail wants the entire message as a base64url string:
    raw_bytes = base64.urlsafe_b64encode(msg.as_bytes())
    return raw_bytes.decode()


def create_message_for_sending(
    from_email: str,
    from_name: str,
    to_emails: list,
    subject: str,
    body: str,
    cc_emails: list = None,
    thread_id: str = None
) -> str:
    """
    Helper function to create a local message that will be sent to Gmail.
    
    Args:
        from_email (str): Sender's email address.
        from_name (str): Sender's display name.
        to_emails (list): List of recipient email addresses.
        subject (str): Email subject.
        body (str): Email body text.
        cc_emails (list, optional): List of CC email addresses.
        thread_id (str, optional): Thread ID if replying.
        
    Returns:
        str: The local message ID that was created.
    """
    from .db import create_local_message
    
    # Format sender
    sender = {
        "name": from_name,
        "email": from_email
    }
    
    # Format recipients
    recipients = {
        "to": [{"name": "", "email": email} for email in to_emails]
    }
    
    if cc_emails:
        recipients["cc"] = [{"name": "", "email": email} for email in cc_emails]
    
    return create_local_message(
        sender=sender,
        recipients=recipients,
        subject=subject,
        body=body,
        thread_id=thread_id
    )