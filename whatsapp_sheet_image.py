"""
Shared module: Capture Google Sheet range → convert to image → send via WHAPI.
Use from any script after pushing data to a worksheet.

Usage:
    from whatsapp_sheet_image import send_sheet_range_to_whatsapp

    send_sheet_range_to_whatsapp(worksheet)                        # A1:O24, auto caption
    send_sheet_range_to_whatsapp(worksheet, "A1:G30", "My Report")   # custom range & caption

Standalone test (same sheet as Automatic_Untraceable_Googlesheet_Reports):
    python whatsapp_sheet_image.py
"""

import os
import tempfile
from datetime import datetime

try:
    import requests
except ImportError:
    requests = None

# -----------------------------------------------------------------------------
# CONFIG - Set via environment variables (required in GitHub Actions)
# Use secrets: WHAPI_TOKEN, WHATSAPP_PHONE (comma/newline/semicolon separated for multiple)
# -----------------------------------------------------------------------------
import re
def _parse_recipients(s):
    if not s:
        return []
    return [p.strip() for p in re.split(r'[,\n;]+', str(s)) if p.strip()]
WHAPI_TOKEN = os.getenv('WHAPI_TOKEN', '')
WHATSAPP_PHONE = _parse_recipients(os.getenv('WHATSAPP_PHONE', ''))
WHATSAPP_ENABLED = os.getenv('WHATSAPP_ENABLED', '1') != '0'
CHROMEDRIVER_PATH = None  # Optional: path to chromedriver, or None for webdriver-manager

def _get_recipients():
    """Return list of WhatsApp recipients. Env WHATSAPP_PHONE: comma, newline, or semicolon separated."""
    env = os.getenv('WHATSAPP_PHONE')
    if env:
        return _parse_recipients(env)
    if isinstance(WHATSAPP_PHONE, str):
        return [WHATSAPP_PHONE] if WHATSAPP_PHONE else []
    return list(WHATSAPP_PHONE) if WHATSAPP_PHONE else []


WHATSAPP_CONFIG = {
    'enabled': WHATSAPP_ENABLED and (os.getenv('WHATSAPP_ENABLED', '1') != '0'),
    'token': os.getenv('WHAPI_TOKEN') or WHAPI_TOKEN,
    'api_url': 'https://gate.whapi.cloud/messages/image',
    'chromedriver_path': CHROMEDRIVER_PATH,
}


def _log(msg, level='INFO', log_func=None):
    """Log via callback or print."""
    if log_func:
        try:
            log_func(msg, level)
        except Exception:
            print(f"[{level}] {msg}")
    else:
        print(f"[{level}] {msg}")


def _get_last_row_with_data(worksheet, end_col, max_rows=200):
    """Find the last row (1-based) that has any non-empty cell in columns A to end_col."""
    try:
        range_full = f"A1:{end_col}{max_rows}"
        data = worksheet.get(range_full)
        if not data:
            return 1
        for row_idx in range(len(data) - 1, -1, -1):
            row = data[row_idx]
            if any(cell is not None and str(cell).strip() for cell in row):
                return row_idx + 1  # 1-based
        return 1
    except Exception:
        return 1


def _rgb_to_css(rgb_dict):
    """Convert Sheets API backgroundColor {red,green,blue 0-1} to CSS rgb()."""
    if not rgb_dict or not any(rgb_dict.get(k, 0) for k in ('red', 'green', 'blue')):
        return None
    r = int((rgb_dict.get('red', 0) or 0) * 255)
    g = int((rgb_dict.get('green', 0) or 0) * 255)
    b = int((rgb_dict.get('blue', 0) or 0) * 255)
    if r == 255 and g == 255 and b == 255:
        return None  # white = default
    return f"rgb({r},{g},{b})"


def _get_sheet_range_with_format(worksheet, range_a1, credentials=None):
    """
    Fetch sheet range with cell formatting (colors) using Sheets API v4.
    Returns (rows, cell_colors_2d) or (rows, None) if format fetch fails.
    cell_colors_2d[row_idx][col_idx] = CSS color string or None.
    """
    try:
        from googleapiclient.discovery import build
    except ImportError:
        return None, None

    spreadsheet_id = worksheet.spreadsheet.id
    sheet_title = worksheet.title
    range_full = f"'{sheet_title}'!{range_a1}"

    creds = credentials
    if not creds:
        service_account_path = os.path.join(os.path.dirname(__file__) or '.', 'service_account_key.json')
        if os.path.exists(service_account_path):
            from google.oauth2.service_account import Credentials
            creds = Credentials.from_service_account_file(
                service_account_path,
                scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            )
    if not creds:
        return None, None

    try:
        service = build('sheets', 'v4', credentials=creds)
        result = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id,
            ranges=[range_full],
            includeGridData=True
        ).execute()
    except Exception:
        return None, None

    rows = []
    colors_2d = []
    if not result.get('sheets') or not result['sheets'][0].get('data'):
        return None, None

    sheet_data = result['sheets'][0]['data'][0]
    row_data_list = sheet_data.get('rowData', [])

    for row_info in row_data_list:
        row_values = []
        row_colors = []
        cells = row_info.get('values', [])
        for cell in cells:
            val = cell.get('formattedValue', '')
            row_values.append(val)
            bg = cell.get('effectiveFormat', {}).get('backgroundColor', {})
            row_colors.append(_rgb_to_css(bg))
        rows.append(row_values)
        colors_2d.append(row_colors)

    if not rows:
        return None, None
    num_cols = max(len(r) for r in rows)
    for i in range(len(rows)):
        while len(rows[i]) < num_cols:
            rows[i].append('')
        while len(colors_2d[i]) < num_cols:
            colors_2d[i].append(None)
    return rows, colors_2d


def sheet_range_to_html(rows, cell_colors=None):
    """Convert sheet range data (list of lists) to styled HTML table for image conversion.
    Font size 14px, column widths fit to max content length.
    cell_colors: optional 2D list, cell_colors[row][col] = CSS color string or None (from Sheets API)."""
    if not rows or len(rows) < 2:
        return None

    num_cols = max(len(r) for r in rows) if rows else 0
    if num_cols == 0:
        return None

    # Compute max char length per column (px per char ~9 for 14px font)
    max_len = [0] * num_cols
    for row in rows:
        for i in range(num_cols):
            val = str(row[i]) if i < len(row) and row[i] is not None else ""
            max_len[i] = max(max_len[i], len(val))
    # Min width 75px for data cols (fits "100.00%", "26020.8"), 60px for first; max 400/180.
    col_widths = []
    for i, n in enumerate(max_len):
        min_w = 60 if i == 0 else 75  # Ensure percentages & integers display fully (no ellipsis)
        w = max(min_w, n * 9)
        max_w = 400 if i == 0 else 180  # Hub Name column: allow up to 400px
        col_widths.append(min(max_w, w))
    table_width = sum(col_widths) + (num_cols + 1) * 2  # borders

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        html, body {{ margin: 0; padding: 0; font-family: Arial, sans-serif; font-size: 14px; }}
        .container {{ margin: 0; padding: 0; display: inline-block; }}
        table {{ border-collapse: collapse; table-layout: fixed; width: {table_width}px; margin: 0; }}
        th, td {{ border: 1px solid #ddd; padding: 3px 5px; text-align: left; white-space: nowrap; overflow: hidden; }}
        th {{ background-color: #33cc33; color: #000; font-weight: bold; font-size: 13px; }}
        .total-row {{ background-color: #ffff00; font-weight: bold; }}
        .amount-row {{ background-color: #b3e6ff; font-weight: bold; }}
    </style>
</head>
<body><div class="container"><table>
<colgroup>
"""
    for w in col_widths:
        html += f"<col style=\"width:{w}px\">\n"
    html += "</colgroup>\n"
    header = rows[0]
    has_colors = cell_colors and len(cell_colors) > 0
    html += "<thead><tr>"
    for col_idx, cell in enumerate(header):
        val = str(cell) if cell is not None and str(cell).strip() else ""
        style = ""
        if has_colors and col_idx < len(cell_colors[0]) and cell_colors[0][col_idx]:
            style = f' style="background-color:{cell_colors[0][col_idx]}"'
        html += f"<th{style}>{val}</th>"
    html += "</tr></thead><tbody>"

    for row_idx, row in enumerate(rows[1:], start=1):
        row_class = ""
        if row and len(row) > 0:
            first_cell = str(row[0]).strip().upper() if row[0] else ""
            if "TOTAL ALL HUBS" in first_cell or "TOTAL ALL CLM" in first_cell or "TOTAL ALL STATES" in first_cell:
                row_class = ' class="total-row"'
            elif "TOTAL AMOUNT" in first_cell:
                row_class = ' class="amount-row"'
        html += f"<tr{row_class}>"
        for col_idx in range(len(header)):
            cell = row[col_idx] if col_idx < len(row) else ""
            val = str(cell) if cell is not None and str(cell).strip() else ""
            style = ""
            if has_colors and row_idx < len(cell_colors) and col_idx < len(cell_colors[row_idx]) and cell_colors[row_idx][col_idx]:
                style = f' style="background-color:{cell_colors[row_idx][col_idx]}"'
            html += f"<td{style}>{val}</td>"
        html += "</tr>"
    html += "</tbody></table></div></body></html>"
    return html


def html_to_image_bytes(html_content, chromedriver_path=None):
    """Convert HTML to PNG base64. Uses local html_table_to_image (Chrome/Selenium)."""
    try:
        from html_table_to_image import html_to_image
        chromedriver_path = chromedriver_path or WHATSAPP_CONFIG.get('chromedriver_path')
        with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
            output_path = tmp.name
        try:
            result = html_to_image(
                html_content=html_content,
                output_path=output_path,
                include_base64=True,
                raw_html=True,
                crop_selector=".container",
                chromedriver_path=chromedriver_path
            )
            if result.get('success') and result.get('image_base64'):
                return True, result['image_base64'], None
            return False, None, result.get('error', 'Conversion failed')
        finally:
            try:
                os.unlink(output_path)
            except OSError:
                pass
    except ImportError:
        return False, None, "html_table_to_image not found. Install: pip install selenium webdriver-manager pillow"
    except Exception as e:
        return False, None, str(e)


def send_sheet_range_to_whatsapp(worksheet, range="A1:O24", caption=None, log_func=None, auto_detect_rows=False):
    """
    Read sheet range, convert to image, send via WHAPI.

    Args:
        worksheet: gspread Worksheet object (e.g. from spreadsheet.worksheet('TabName'))
        range: A1 notation range to capture (default "A1:O24"). When auto_detect_rows=True,
               use format "A1:I" - columns only; last row with data is auto-detected.
        caption: Message caption (default: auto-generated with timestamp)
        log_func: Optional callback(message, level) for logging (e.g. print_detailed_log)
        auto_detect_rows: If True, find last row with data and capture only up to that row.
    """
    if not WHATSAPP_CONFIG['enabled']:
        _log("WhatsApp disabled (WHATSAPP_ENABLED=0)", "INFO", log_func)
        return

    token = WHATSAPP_CONFIG['token']
    recipients = _get_recipients()
    if not token or not recipients:
        _log("WHAPI_TOKEN or WHATSAPP_PHONE not set - skipping WhatsApp send", "WARNING", log_func)
        return
    _log(f"Sending to {len(recipients)} recipient(s)", "INFO", log_func)

    if not requests:
        _log("requests package required for WHAPI. Install: pip install requests", "WARNING", log_func)
        return

    if auto_detect_rows:
        parts = range.split(':')
        if len(parts) == 2 and parts[1].isalpha():
            end_col = parts[1].upper()
            last_row = _get_last_row_with_data(worksheet, end_col)
            range = f"A1:{end_col}{last_row}"
            _log(f"Auto-detected last row: {last_row}", "INFO", log_func)

    _log(f"Capturing {range} from sheet for WhatsApp image...", "PROGRESS", log_func)
    rows = None
    cell_colors = None
    try:
        rows, cell_colors = _get_sheet_range_with_format(worksheet, range)
    except Exception:
        pass
    if rows is None:
        try:
            rows = worksheet.get(range)
        except Exception as e:
            _log(f"Failed to read {range}: {e}", "ERROR", log_func)
            return

    if not rows or len(rows) < 2:
        _log(f"No data in {range} to capture", "WARNING", log_func)
        return

    if cell_colors:
        _log("Using cell colors from sheet", "INFO", log_func)
    html = sheet_range_to_html(rows, cell_colors)
    if not html:
        _log("Could not build HTML from sheet range", "ERROR", log_func)
        return

    _log("Converting table to image (Chrome)...", "PROGRESS", log_func)
    success, img_base64, err = html_to_image_bytes(html)
    if not success:
        _log(f"HTML to image failed: {err}", "ERROR", log_func)
        return

    media_value = f"data:image/png;base64,{img_base64}"
    if caption is None:
        caption = f"Report - {datetime.now().strftime('%d-%b-%Y %H:%M')}"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    for recipient in recipients:
        payload = {
            "to": recipient,
            "caption": caption,
            "media": media_value
        }
        try:
            _log(f"Sending image to WhatsApp ({recipient})...", "PROGRESS", log_func)
            resp = requests.post(WHATSAPP_CONFIG['api_url'], json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            _log(f"WhatsApp image sent to {recipient}", "SUCCESS", log_func)
        except requests.exceptions.RequestException as e:
            _log(f"WhatsApp send failed for {recipient}: {e}", "ERROR", log_func)
            if hasattr(e, 'response') and e.response is not None:
                try:
                    _log(f"Response: {e.response.text[:300]}", "ERROR", log_func)
                except Exception:
                    pass


# -----------------------------------------------------------------------------
# STANDALONE TEST - Run: python whatsapp_sheet_image.py
# Extracts A1:O24 from Untraceable sheet and sends to WhatsApp (no full script run).
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Same sheet as Automatic_Untraceable_Googlesheet_Reports
    SERVICE_ACCOUNT_FILE = os.path.join(os.path.dirname(__file__) or '.', 'service_account_key.json')
    SPREADSHEET_ID = '1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM'
    WORKSHEET_NAME = 'Untraceable'

    print("Testing: Google Sheet → Image → WHAPI")
    print(f"Sheet: {SPREADSHEET_ID}, Worksheet: {WORKSHEET_NAME}")
    print("-" * 50)

    if not _get_recipients():
        phone = input("Enter WhatsApp phone (e.g. 919500055366) or group JID (e.g. 120363320457092145@g.us): ").strip()
        if not phone:
            print("ERROR: WHATSAPP_PHONE required. Set it in this file (line 25) or enter when prompted.")
            exit(1)
        os.environ['WHATSAPP_PHONE'] = phone  # _get_recipients() reads from env

    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError:
        print("ERROR: Install gspread and google-auth. Run: pip install gspread google-auth")
        exit(1)

    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"ERROR: Service account key not found: {SERVICE_ACCOUNT_FILE}")
        exit(1)

    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    )
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    worksheet = spreadsheet.worksheet(WORKSHEET_NAME)

    send_sheet_range_to_whatsapp(
        worksheet,
        range="A1:O24",
        caption=f"Untraceable Report (test) - {datetime.now().strftime('%d-%b-%Y %H:%M')}",
    )

    # UTR > 5K sheet (if it exists)
    try:
        utr_worksheet = spreadsheet.worksheet("UTR > 5K")
        print("-" * 50)
        send_sheet_range_to_whatsapp(
            utr_worksheet,
            range="A1:I",
            caption=f"UTR > 5K Report (test) - {datetime.now().strftime('%d-%b-%Y %H:%M')}",
            auto_detect_rows=True,
        )
    except Exception as e:
        print(f"Skipping UTR > 5K (sheet may not exist): {e}")

    print("-" * 50)
    print("Done.")
