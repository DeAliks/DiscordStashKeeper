"""
QueueManager with support for issue management
"""

import datetime
import logging
import uuid
from typing import Optional, List, Dict, Any

import config
from sheets_adapter import SheetsAdapter

logger = logging.getLogger(__name__)


class QueueManager:
    def __init__(self, sheets_adapter: SheetsAdapter):
        self.sheets = sheets_adapter

    def _now_iso(self) -> str:
        return datetime.datetime.utcnow().isoformat()

    def _generate_row(self, discord_id: int, discord_name: str, character: str,
                      grade: str, resource: str, qty: int, channel_id: int,
                      message_id: int, screenshot_url: str = "", priority: int = None,
                      status: str = "active", purple_approval: str = "n/a") -> List[str]:
        if priority is None:
            # Try to get user priority
            try:
                from priority_manager import get_user_priority
                priority = get_user_priority(str(discord_id))
            except:
                priority = config.DEFAULT_PRIORITY

        row_uuid = str(uuid.uuid4())
        now = self._now_iso()

        # New columns order with IssuedQuantity and Remaining
        row = [
            now,  # CreatedAt
            str(discord_id),  # DiscordID
            discord_name,  # DiscordName
            character,  # CharacterName
            grade,  # ResourceGrade
            resource,  # ResourceName
            str(qty),  # Quantity
            "0",  # IssuedQuantity
            str(qty),  # Remaining
            str(priority),  # PriorityLevel
            now,  # RequestTimestamp
            "",  # QueuePosition
            status,  # Status
            str(channel_id),  # ChannelID
            str(message_id),  # MessageID
            row_uuid,  # RowID
            screenshot_url or "",  # Screenshoot
            purple_approval,  # PurpleApproval
            "",  # ApproverID
            ""  # Notes
        ]
        return row

    def add_request(self, discord_id: int, discord_name: str, character: str,
                    grade: str, resource: str, qty: int, channel_id: int,
                    message_id: int, screenshot_url: str = "",
                    priority: Optional[int] = None, force_new: bool = False) -> str:
        """Add a new request."""
        try:
            # Check for existing similar request
            if not force_new:
                records = self.sheets.get_all_records()
                for rec in records:
                    if (str(rec.get("DiscordID")) == str(discord_id) and
                            str(rec.get("ResourceName")) == resource and
                            str(rec.get("CharacterName")) == character and
                            rec.get("Status") in ("active", "pending")):

                        # Update existing request
                        existing_qty = int(rec.get("Quantity") or 0)
                        existing_issued = int(rec.get("IssuedQuantity") or 0)
                        new_qty = existing_qty + qty

                        # Update quantity and remaining
                        updates = {
                            "Quantity": str(new_qty),
                            "Remaining": str(new_qty - existing_issued),
                            "RequestTimestamp": self._now_iso()
                        }

                        rownum = rec.get("__row_number")
                        if rownum:
                            self.sheets.update_row(rownum, updates)
                            self.sheets.recompute_queue_positions(resource)
                            return rec.get("RowID") or ""

            # Create new request
            status = "pending" if grade.lower().startswith("purple") else "active"
            purple_approval = "awaiting" if grade.lower().startswith("purple") else "n/a"

            row = self._generate_row(
                discord_id, discord_name, character, grade, resource, qty,
                channel_id, message_id, screenshot_url, priority, status, purple_approval
            )

            rowid = row[15]  # RowID position
            self.sheets.append_row(row)
            self.sheets.recompute_queue_positions(resource)

            return rowid

        except Exception as e:
            logger.exception(f"add_request error: {e}")
            raise

    def approve_purple_request(self, row_number: int, approver_id: int):
        """
        Approve pending purple request (row_number is 1-based row index in sheet).
        """
        try:
            updates = {
                "PurpleApproval": "approved",
                "ApproverID": str(approver_id),
                "Status": "active"
            }
            self.sheets.update_row(row_number, updates)
            # update queue positions for that resource (read row to get resource)
            row = self.sheets.get_row(row_number)
            resource = row.get("ResourceName")
            if resource:
                self.sheets.recompute_queue_positions(resource)
        except Exception as e:
            logger.exception("approve_purple_request error: %s", e)
            raise

    def deny_purple_request(self, row_number: int, approver_id: int, reason: str = ""):
        try:
            updates = {
                "PurpleApproval": "denied",
                "ApproverID": str(approver_id),
                "Status": "cancelled",
                "Notes": reason
            }
            self.sheets.update_row(row_number, updates)
        except Exception as e:
            logger.exception("deny_purple_request error: %s", e)
            raise

    def cancel_request_by_row(self, row_number: int, requester_id: Optional[int] = None) -> None:
        """
        Cancel a request: set Status=cancelled and optionally write who cancelled in Notes.
        """
        try:
            note = f"Cancelled by requester {requester_id}" if requester_id else "Cancelled"
            self.sheets.update_row(row_number, {
                "Status": "cancelled",
                "Notes": note,
                "QueuePosition": "",
                "IssuedQuantity": "0",
                "Remaining": "0"
            })
            # recompute positions for that resource
            row = self.sheets.get_row(row_number)
            resource = row.get("ResourceName")
            if resource:
                self.sheets.recompute_queue_positions(resource)
        except Exception as e:
            logger.exception("cancel_request_by_row error: %s", e)
            raise

    def list_user_requests(self, discord_id: int) -> List[Dict[str, Any]]:
        """
        Return user's active/pending records as list of dicts. Each dict will include a special key '__row_number'.
        """
        out = []
        try:
            headers = self.sheets.get_headers()
            all_values = self.sheets.sheet.get_all_values()
            if not all_values or len(all_values) < 2:
                return out
            for i, row in enumerate(all_values[1:], start=2):
                # build dict
                rec = {}
                for idx, h in enumerate(headers):
                    rec[h] = row[idx] if idx < len(row) else ""
                if str(rec.get("DiscordID")) == str(discord_id) and rec.get("Status") in (
                "active", "pending", "completed"):
                    rec["__row_number"] = i

                    # Convert numeric fields
                    numeric_fields = ["Quantity", "IssuedQuantity", "Remaining", "PriorityLevel"]
                    for field in numeric_fields:
                        if field in rec and rec[field]:
                            try:
                                rec[field] = int(rec[field])
                            except ValueError:
                                rec[field] = 0
                        else:
                            rec[field] = 0

                    out.append(rec)
            return out
        except Exception as e:
            logger.exception("list_user_requests error: %s", e)
            raise

    def update_issued_quantity(self, row_number: int, issued_quantity: int) -> bool:
        """Update issued quantity for a request."""
        try:
            return self.sheets.update_issued_quantity(row_number, issued_quantity)
        except Exception as e:
            logger.exception(f"update_issued_quantity error: {e}")
            return False

    def complete_request(self, row_number: int) -> bool:
        """Mark request as completed."""
        try:
            return self.sheets.complete_request(row_number)
        except Exception as e:
            logger.exception(f"complete_request error: {e}")
            return False

    def get_active_requests(self) -> List[Dict[str, Any]]:
        """Get all active requests."""
        try:
            return self.sheets.get_active_requests()
        except Exception as e:
            logger.exception(f"get_active_requests error: {e}")
            return []