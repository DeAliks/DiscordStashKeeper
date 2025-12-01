# sheets_adapter.py
import logging
import time
from typing import List, Dict, Any, Optional

import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

import config

logger = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

class SheetsAdapter:
    """
    Adapter for Google Sheets using gspread and a service account.
    Expectation: first row contains headers exactly as in config/table spec.
    """

    def __init__(self, creds_file: str = config.GOOGLE_CREDENTIALS_FILE, spreadsheet_id: str = config.SPREADSHEET_ID, sheet_name: str = config.SHEET_NAME):
        self.creds_file = creds_file
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.client = None
        self.sheet = None
        self._init_client()

    def _init_client(self):
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.creds_file, SCOPES)
            self.client = gspread.authorize(creds)
            ss = self.client.open_by_key(self.spreadsheet_id)
            self.sheet = ss.worksheet(self.sheet_name)
            logger.info("Connected to sheet: %s / %s", self.spreadsheet_id, self.sheet_name)
        except Exception as e:
            logger.exception("Failed to initialize SheetsAdapter: %s", e)
            raise

    # ---- Basic operations ----
    def get_headers(self) -> List[str]:
        """Return header row values (first row)."""
        try:
            headers = self.sheet.row_values(1)
            return headers
        except Exception as e:
            logger.exception("get_headers error: %s", e)
            raise

    def get_all_records(self) -> List[Dict[str, Any]]:
        """
        Return all rows as list of dict keyed by headers.
        Uses gspread's get_all_records (skips header).
        """
        try:
            records = self.sheet.get_all_records()
            return records
        except Exception as e:
            logger.exception("get_all_records error: %s", e)
            raise

    def append_row(self, values: List[Any], value_input_option: str = 'USER_ENTERED', max_retries: int = 3) -> None:
        """
        Append a row to the sheet.
        Note: gspread.append_row does not return created row index reliably.
        We rely on unique RowID in values to find the row later.
        """
        for attempt in range(max_retries):
            try:
                self.sheet.append_row(values, value_input_option=value_input_option)
                return
            except APIError as e:
                logger.warning("append_row APIError attempt %s: %s", attempt+1, e)
                time.sleep(1 + attempt * 2)
            except Exception as e:
                logger.exception("append_row error: %s", e)
                time.sleep(1 + attempt * 2)
        raise RuntimeError("Failed to append row after retries")

    def find_rows(self, column_name: str, value: str) -> List[int]:
        """
        Find row numbers (1-based) where column_name == value (exact match).
        Returns list of row numbers (may be empty).
        """
        headers = self.get_headers()
        if column_name not in headers:
            logger.debug("find_rows: column_name %s not in headers", column_name)
            return []
        col_idx = headers.index(column_name) + 1
        # Read the full column
        try:
            col_values = self.sheet.col_values(col_idx)
        except Exception as e:
            logger.exception("find_rows col_values error: %s", e)
            raise
        matches = []
        for idx, cell_value in enumerate(col_values, start=1):
            if str(cell_value) == str(value):
                matches.append(idx)
        return matches

    def get_row_number_by_rowid(self, rowid_value: str) -> Optional[int]:
        """
        Convenience: find first row number by "RowID" column.
        """
        rows = self.find_rows("RowID", rowid_value)
        return rows[0] if rows else None

    def update_row(self, row_number: int, updates: Dict[str, Any]) -> None:
        """
        Update a row by mapping header->new_value.
        """
        if row_number < 1:
            raise ValueError("row_number must be >= 1")
        headers = self.get_headers()
        cells = []
        for key, val in updates.items():
            if key not in headers:
                logger.debug("update_row: header %s not found; skipping", key)
                continue
            col_idx = headers.index(key) + 1
            # gspread expects list of Cell to update via update_cells
            # but using update_cell is simpler per-cell (slower but OK for MVP).
            try:
                self.sheet.update_cell(row_number, col_idx, val)
            except Exception as e:
                logger.exception("update_row update_cell error for row %s col %s: %s", row_number, col_idx, e)
                raise

    def get_row(self, row_number: int) -> Dict[str, Any]:
        """
        Return a dict of header->value for the given 1-based row_number.
        """
        headers = self.get_headers()
        try:
            values = self.sheet.row_values(row_number)
        except Exception as e:
            logger.exception("get_row error: %s", e)
            raise
        row = {}
        for idx, header in enumerate(headers):
            val = values[idx] if idx < len(values) else ""
            row[header] = val
        return row

    # ---- Utility: recompute queue positions for a given resource ----
    def recompute_queue_positions(self, resource_name: str):
        """
        Recompute QueuePosition for all rows with ResourceName==resource_name and Status active/pending.
        Sorting by PriorityLevel desc, RequestTimestamp asc.
        Writes QueuePosition back to sheet.
        """
        headers = self.get_headers()
        # read all rows (including header) as values to find matching rows indices
        try:
            all_values = self.sheet.get_all_values()
        except Exception as e:
            logger.exception("recompute_queue_positions get_all_values error: %s", e)
            raise

        if not all_values or len(all_values) < 2:
            return

        header_row = all_values[0]
        # find indices of columns
        def col_idx(col_name):
            try:
                return header_row.index(col_name)
            except ValueError:
                return None

        idx_resource = col_idx("ResourceName")
        idx_status = col_idx("Status")
        idx_priority = col_idx("PriorityLevel")
        idx_timestamp = col_idx("RequestTimestamp")
        idx_queuepos = col_idx("QueuePosition")

        if None in (idx_resource, idx_status, idx_priority, idx_timestamp, idx_queuepos):
            logger.warning("recompute_queue_positions: missing expected columns")
            return

        # collect rows to sort: list of (row_number, priority_int, timestamp_str)
        candidates = []
        for i, row in enumerate(all_values[1:], start=2):
            try:
                rn_resource = row[idx_resource] if idx_resource < len(row) else ""
                rn_status = row[idx_status] if idx_status < len(row) else ""
                if rn_resource != resource_name:
                    continue
                if rn_status not in ("active", "pending"):
                    continue
                p = int(row[idx_priority]) if (idx_priority < len(row) and row[idx_priority]) else 0
                ts = row[idx_timestamp] if idx_timestamp < len(row) else ""
                candidates.append((i, p, ts))
            except Exception as e:
                logger.exception("Error parsing row %s: %s", i, e)
                continue

        # sort: priority desc, timestamp asc
        candidates.sort(key=lambda x: (-x[1], x[2]))

        # update QueuePosition values
        for pos, item in enumerate(candidates, start=1):
            rownum = item[0]
            try:
                # update specific cell
                self.sheet.update_cell(rownum, idx_queuepos + 1, str(pos))
            except Exception as e:
                logger.exception("Failed to update QueuePosition for row %s: %s", rownum, e)
