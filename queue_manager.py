# queue_manager.py
import datetime
import logging
import uuid
from typing import Optional, List, Dict, Any

import config
from sheets_adapter import SheetsAdapter

logger = logging.getLogger(__name__)

HEADERS = ["CreatedAt","DiscordID","DiscordName","CharacterName","ResourceGrade","ResourceName",
           "Quantity","PriorityLevel","RequestTimestamp","QueuePosition","Status","ChannelID",
           "MessageID","RowID","Screenshoot","PurpleApproval","ApproverID","Notes"]

class QueueManager:
    def __init__(self, sheets_adapter: SheetsAdapter):
        self.sheets = sheets_adapter

    def _now_iso(self) -> str:
        return datetime.datetime.utcnow().isoformat()

    def _generate_row(self, discord_id: int, discord_name: str, character: str, grade: str, resource: str,
                      qty: int, channel_id: int, message_id: int, screenshot_url: str = "", priority: int = None,
                      status: str = "active", purple_approval: str = "n/a") -> List[str]:
        if priority is None:
            priority = config.DEFAULT_PRIORITY
        row_uuid = str(uuid.uuid4())
        now = self._now_iso()
        row = [
            now,
            str(discord_id),
            discord_name,
            character,
            grade,
            resource,
            str(qty),
            str(priority),
            now,
            "",
            status,
            str(channel_id),
            str(message_id),
            row_uuid,
            screenshot_url or "",
            purple_approval,
            "",
            ""
        ]
        return row

    def add_request(self, discord_id: int, discord_name: str, character: str, grade: str, resource: str,
                    qty: int, channel_id: int, message_id: int, screenshot_url: str = "", priority: Optional[int] = None,
                    force_new: bool = False) -> str:
        """
        Add a request. If not force_new and existing request found for same DiscordID+ResourceName+CharacterName,
        update the existing request (quantity) instead of creating a new one.
        Returns RowID (uuid).
        """
        try:
            # dedupe: search all records for matches
            records = self.sheets.get_all_records()
            for idx, rec in enumerate(records, start=2):  # row numbers start at 2 for records
                try:
                    if str(rec.get("DiscordID")) == str(discord_id) and \
                       str(rec.get("ResourceName")) == str(resource) and \
                       str(rec.get("CharacterName")) == str(character) and not force_new:
                        # update this row: increase quantity (or set)
                        new_qty = qty
                        # Option: sum quantities
                        try:
                            existing_qty = int(rec.get("Quantity") or 0)
                            new_qty = existing_qty + qty
                        except Exception:
                            pass
                        rownum = idx
                        self.sheets.update_row(rownum, {"Quantity": str(new_qty), "RequestTimestamp": self._now_iso()})
                        # recompute positions for resource
                        self.sheets.recompute_queue_positions(resource)
                        rowid = rec.get("RowID") or ""
                        return rowid
                except Exception:
                    continue
            # if not found, append new row
            status = "pending" if grade.lower().startswith("purple") else "active"
            purple_approval = "awaiting" if grade.lower().startswith("purple") else "n/a"
            row = self._generate_row(discord_id, discord_name, character, grade, resource, qty, channel_id, message_id, screenshot_url, priority, status, purple_approval)
            # RowID is at position index 13 (0-based)
            rowid = row[13]
            self.sheets.append_row(row)
            # recompute positions
            self.sheets.recompute_queue_positions(resource)
            return rowid
        except Exception as e:
            logger.exception("add_request error: %s", e)
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
            self.sheets.update_row(row_number, {"Status": "cancelled", "Notes": note})
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
                if str(rec.get("DiscordID")) == str(discord_id) and rec.get("Status") in ("active", "pending"):
                    rec["__row_number"] = i
                    out.append(rec)
            return out
        except Exception as e:
            logger.exception("list_user_requests error: %s", e)
            raise
