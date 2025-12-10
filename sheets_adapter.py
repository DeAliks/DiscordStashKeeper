"""
SheetsAdapter with support for issue management
"""

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

    def __init__(self, creds_file: str = config.GOOGLE_CREDENTIALS_FILE,
                 spreadsheet_id: str = config.SPREADSHEET_ID,
                 sheet_name: str = config.SHEET_NAME):
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
                logger.warning("append_row APIError attempt %s: %s", attempt + 1, e)
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
        Update a row by mapping header->new value.
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

    def get_row(self, row_number: int) -> Optional[Dict[str, Any]]:
        """Получить строку по номеру."""
        try:
            # === ДОБАВЛЕНА ПРОВЕРКА НА НЕВАЛИДНЫЙ НОМЕР СТРОКИ ===
            if row_number is None:
                logger.error("get_row вызван с row_number=None")
                return None
            if not isinstance(row_number, int) or row_number < 1:
                logger.error(f"Неверный номер строки: {row_number} (тип: {type(row_number)})")
                return None
            # === КОНЕЦ ПРОВЕРКИ ===

            values = self.sheet.row_values(row_number)
            if not values:
                return None

            headers = self.sheet.row_values(1)
            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(values):
                    row_dict[header] = values[i]
                else:
                    row_dict[header] = ""

            # Добавляем номер строки в возвращаемый словарь
            row_dict['__row_number'] = row_number

            return row_dict
        except gspread.exceptions.APIError as e:
            logger.exception(f"get_row API error: {e}")
            return None
        except Exception as e:
            logger.exception(f"get_row error: {e}")
            return None

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

    # ---- Issue management methods ----
    def get_active_requests(self) -> List[Dict[str, Any]]:
        """
        Получить все активные заявки с реальным номером строки в таблице.
        Возвращает список словарей с ключом __row_number.
        """
        try:
            # Получаем все данные как есть (включая заголовок)
            all_values = self.sheet.get_all_values()

            if len(all_values) <= 1:  # Только заголовок или пусто
                logger.info("Таблица пуста или содержит только заголовки")
                return []

            headers = all_values[0]
            active_requests = []

            # Определяем индексы нужных колонок
            try:
                status_idx = headers.index("Status")
                resource_idx = headers.index("ResourceName")
                player_idx = headers.index("DiscordName")
                character_idx = headers.index("CharacterName")
                quantity_idx = headers.index("Quantity")
                issued_idx = headers.index("IssuedQuantity")
                remaining_idx = headers.index("Remaining")
                position_idx = headers.index("QueuePosition")
                priority_idx = headers.index("PriorityLevel")
                discord_id_idx = headers.index("DiscordID")
                rowid_idx = headers.index("RowID")
                resource_grade_idx = headers.index("ResourceGrade")
            except ValueError as e:
                logger.error(f"Ошибка поиска колонок: {e}. Доступные колонки: {headers}")
                return []

            # Проходим по всем строкам, начиная со второй (индекс 1)
            for i, row in enumerate(all_values[1:], start=2):  # i = реальный номер строки в Google Sheets
                if len(row) <= max(status_idx, resource_idx, player_idx):
                    continue  # Пропускаем неполные строки

                status = row[status_idx].strip().lower() if status_idx < len(row) else ""

                # Проверяем только активные заявки
                if status == "active":
                    request_dict = {
                        "__row_number": i,  # Важно! Реальный номер строки в таблице
                        "RowID": row[rowid_idx] if rowid_idx < len(row) else "",
                        "DiscordID": row[discord_id_idx] if discord_id_idx < len(row) else "",
                        "ResourceName": row[resource_idx] if resource_idx < len(row) else "Unknown",
                        "DiscordName": row[player_idx] if player_idx < len(row) else "Unknown",
                        "CharacterName": row[character_idx] if character_idx < len(row) else "Unknown",
                        "ResourceGrade": row[resource_grade_idx] if resource_grade_idx < len(row) else "Blue",
                        "Quantity": self._safe_int(row[quantity_idx]) if quantity_idx < len(row) else 0,
                        "IssuedQuantity": self._safe_int(row[issued_idx]) if issued_idx < len(row) else 0,
                        "Remaining": self._safe_int(row[remaining_idx]) if remaining_idx < len(row) else 0,
                        "QueuePosition": row[position_idx] if position_idx < len(row) else "?",
                        "PriorityLevel": self._safe_int(row[priority_idx]) if priority_idx < len(row) else 1,
                        "Status": status,
                        # Дополнительные поля из вашей таблицы
                        "CreatedAt": row[headers.index("CreatedAt")] if "CreatedAt" in headers and headers.index("CreatedAt") < len(row) else "",
                        "RequestTimestamp": row[headers.index("RequestTimestamp")] if "RequestTimestamp" in headers and headers.index("RequestTimestamp") < len(row) else "",
                    }

                    active_requests.append(request_dict)

            logger.info(f"Найдено {len(active_requests)} активных заявок")
            return active_requests

        except Exception as e:
            logger.exception(f"get_active_requests error: {e}")
            return []

    def _safe_int(self, value: str) -> int:
        """Безопасное преобразование строки в целое число."""
        try:
            if not value or value.strip() == "":
                return 0
            return int(float(value))  # Обрабатываем случаи вроде "16.0"
        except (ValueError, TypeError):
            return 0

    def update_issued_quantity(self, row_number: int, issued_quantity: int,
                               completed: bool = False) -> bool:
        """Update issued quantity and remaining for a request."""
        try:
            # Get current row
            row = self.get_row(row_number)
            if not row:
                return False

            total_quantity = row.get("Quantity", 0)

            # Calculate remaining
            remaining = max(0, total_quantity - issued_quantity)

            # Prepare updates
            updates = {
                "IssuedQuantity": str(issued_quantity),
                "Remaining": str(remaining)
            }

            if completed:
                updates["Status"] = "completed"
                updates["QueuePosition"] = ""

            # Update row
            self.update_row(row_number, updates)

            # If completed, recompute queue positions
            if completed:
                resource = row.get("ResourceName")
                if resource:
                    self.recompute_queue_positions(resource)

            return True
        except Exception as e:
            logger.exception("update_issued_quantity error: %s", e)
            return False

    def complete_request(self, row_number: int) -> bool:
        """Mark request as completed."""
        try:
            row = self.get_row(row_number)
            if not row:
                return False

            updates = {
                "Status": "completed",
                "QueuePosition": "",
                "IssuedQuantity": str(row.get("Quantity", 0)),
                "Remaining": "0"
            }
            self.update_row(row_number, updates)

            # Recompute queue positions for the resource
            resource = row.get("ResourceName")
            if resource:
                self.recompute_queue_positions(resource)

            return True
        except Exception as e:
            logger.exception("complete_request error: %s", e)
            return False