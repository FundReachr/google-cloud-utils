import logging
import gspread
from google.auth.transport.requests import AuthorizedSession
from google.oauth2.credentials import Credentials as UserCredentials
from google.oauth2.service_account import Credentials as SACredentials

logger = logging.getLogger(__name__)

_SA_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
_DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"

_BATCH_SIZE = 500


class SheetsHandler:
    """
    Wrapper around gspread.

    Service accounts have zero personal Drive storage, so creating files in
    the SA's own Drive always fails with a quota error.

    Set parent_folder_id to a Shared Drive (or folder inside one) where the SA
    is a Contributor.  Files are created there via the Drive API with
    supportsAllDrives=True and count against the Shared Drive's org quota rather
    than any personal Drive storage.

    Drive API calls use google.auth.transport.requests.AuthorizedSession (requests-
    based) instead of googleapiclient/httplib2, which has SSL incompatibilities
    with Python 3.13.
    """

    def __init__(self, serviceAccountJson: dict, parent_folder_id: str | None = None):
        self._sa_json          = serviceAccountJson
        self._parent_folder_id = parent_folder_id

        sa_creds = SACredentials.from_service_account_info(serviceAccountJson, scopes=_SA_SCOPES)
        self._sa_creds  = sa_creds
        self._sa_client = gspread.authorize(sa_creds)
        # requests-based Drive session — avoids httplib2/Python-3.13 SSL issues
        self._drive_session = AuthorizedSession(sa_creds)

    def _client_for(self, access_token: str | None):
        if access_token:
            creds = UserCredentials(token=access_token)
            return gspread.authorize(creds)
        return self._sa_client

    def _write_rows(self, ws, rows: list[list]):
        if not rows:
            return
        total = len(rows) - 1
        logger.info("Writing sheet header + %d data rows in batches of %d", total, _BATCH_SIZE)
        ws.update("A1", rows[:1], value_input_option="RAW")
        body = rows[1:]
        for i in range(0, len(body), _BATCH_SIZE):
            batch_end = min(i + _BATCH_SIZE, len(body))
            logger.info("Writing rows %d–%d / %d", i + 1, batch_end, total)
            ws.append_rows(body[i:batch_end], value_input_option="RAW")
        logger.info("Sheet write complete (%d rows)", total)

    def _create_in_shared_drive(self, title: str, folder_id: str) -> tuple[str, str]:
        """Create a blank Spreadsheet inside a Shared Drive folder via Drive API."""
        resp = self._drive_session.post(
            _DRIVE_FILES_URL,
            params={"supportsAllDrives": "true", "fields": "id,webViewLink"},
            json={
                "name": title,
                "mimeType": "application/vnd.google-apps.spreadsheet",
                "parents": [folder_id],
            },
        )
        resp.raise_for_status()
        f = resp.json()
        return f["id"], f["webViewLink"]

    def create_sheet(
        self,
        title: str,
        rows: list[list],
        access_token: str | None = None,
        share_with_domains: list[str] | None = None,
        share_with_emails: list[str] | None = None,
    ) -> tuple[str, str]:
        """Create a spreadsheet, populate it, and share it.

        When parent_folder_id is configured the file is created inside that
        Shared Drive folder (no personal quota consumed).  Otherwise falls back
        to gspread's create() which requires the SA or user to have Drive storage.

        Returns (spreadsheet_id, edit_url).
        """
        if self._parent_folder_id:
            sheet_id, sheet_url = self._create_in_shared_drive(title, self._parent_folder_id)
            sh = self._sa_client.open_by_key(sheet_id)
        else:
            client = self._client_for(access_token)
            sh = client.create(title)
            sheet_id  = sh.id
            sheet_url = sh.url

        ws = sh.get_worksheet(0)
        self._write_rows(ws, rows)

        for domain in (share_with_domains or []):
            try:
                sh.share(domain, perm_type="domain", role="writer", notify=False)
                logger.info("Shared sheet %s with domain %s", sheet_id, domain)
            except Exception as exc:
                status = getattr(getattr(exc, 'response', None), 'status_code', None)
                if status == 400 or '[400]' in str(exc):
                    logger.info("Skipping domain share for sheet %s — not supported on Shared Drive files", sheet_id)
                else:
                    logger.warning("Could not share %s with domain %s: %s", sheet_id, domain, exc)

        for email in (share_with_emails or []):
            try:
                sh.share(email, perm_type="user", role="writer", notify=False)
                logger.info("Shared sheet %s with user %s", sheet_id, email)
            except Exception as exc:
                logger.warning("Could not share %s with user %s: %s", sheet_id, email, exc)

        return sheet_id, sheet_url

    def delete_sheet(self, spreadsheet_id: str):
        """Delete a spreadsheet (works for both personal Drive and Shared Drive)."""
        try:
            resp = self._drive_session.delete(
                f"{_DRIVE_FILES_URL}/{spreadsheet_id}",
                params={"supportsAllDrives": "true"},
            )
            resp.raise_for_status()
            logger.info("Deleted sheet %s", spreadsheet_id)
        except Exception as exc:
            logger.warning("Could not delete sheet %s: %s", spreadsheet_id, exc)

    def read_sheet(self, spreadsheet_id: str, access_token: str | None = None) -> list[list]:
        client = self._client_for(access_token)
        sh = client.open_by_key(spreadsheet_id)
        ws = sh.get_worksheet(0)
        return ws.get_all_values()

    def update_sheet(self, spreadsheet_id: str, rows: list[list], access_token: str | None = None):
        client = self._client_for(access_token)
        sh = client.open_by_key(spreadsheet_id)
        ws = sh.get_worksheet(0)
        ws.clear()
        self._write_rows(ws, rows)
