"""
Standalone script to extract NCD % and NCD # breach trends from source sheet.
Extracts NCD % and NCD # data for all hubs and displays latest days in destination sheet.
Two tables are published: NCD % (percentage) above, NCD # (count) below, same format.
"""

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from whatsapp_sheet_image import send_sheet_range_to_whatsapp
import pandas as pd
import numpy as np
import logging
import time
import string
import re
import requests
import google.auth.exceptions
import urllib3
from datetime import datetime, timedelta, date

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
SERVICE_ACCOUNT_FILE = 'service_account_key.json'

# NCD % Source Sheet Configuration (same as reservations_email_automation.py)
NCD_SOURCE_SPREADSHEET_ID = '1OT_fTFCiPpRuokJRPrfpx1PiKV1syyXdNN42KaP_fCQ'  # Same as reservations_email_automation.py

# NCD % Destination Sheet Configuration
NCD_DEST_SPREADSHEET_ID = '1FUH-Z98GFcCTIKpSAeZPGsjIESMVgBB2vrb6QOZO8mM'
NCD_DEST_WORKSHEET_NAME = 'NCD Breach'
NCD_DAYS_TO_FETCH = 15  # Latest 15 days

# Target Hub Names for NCD % extraction
TARGET_HUB_NAMES = [
    'LargelogicChinnamanurODH_CNM',
    'LargeLogicKuniyamuthurODH_CJB',
    'KoorieeHayathnagarODH_HYD',
    'DommasandraSplitODH_DMN',
    'SulebeleMDH_SUL',
    'KoorieeSoukyaRdTempODH_BLR',
    'NaubadMDH_BDR',
    'SaidabadSplitODH_HYD',
    'LargeLogicDharapuramODH_DHP',
    'HulimavuHub_BLR',
    'BidarFortHub_BDR',
    'LargeLogicRameswaramODH_RMS',
    'ElasticRunBidarODH_BDR',
    'CABTSRNagarODH_HYD',
    'BagaluruMDH_BAG',
    'ThavarekereMDH_THK',
    'KoorieeSoukyaRdODH_BLR',
    'TTSPLKodaikanalODH_KDI',
    'SITICSWadiODH_WDI',
    'TTSPLBatlagunduODH_BGU',
    'VadipattiMDH_VDP'
]

# Hub Name to Volume Weight mapping
HUB_VOLUME_WEIGHT_MAPPING = {
    'KoorieeSoukyaRdODH_BLR': 16.24,
    'DommasandraSplitODH_DMN': 8.65,
    'LargeLogicKuniyamuthurODH_CJB': 8.40,
    'HulimavuHub_BLR': 7.06,
    'CABTSRNagarODH_HYD': 6.52,
    'KoorieeHayathnagarODH_HYD': 5.86,
    'BidarFortHub_BDR': 5.53,
    'SaidabadSplitODH_HYD': 5.22,
    'TTSPLBatlagunduODH_BGU': 4.06,
    'LargelogicChinnamanurODH_CNM': 3.59,
    'ElasticRunBidarODH_BDR': 3.35,
    'SulebeleMDH_SUL': 3.14,
    'LargeLogicDharapuramODH_DHP': 3.12,
    'SITICSWadiODH_WDI': 2.88,
    'BagaluruMDH_BAG': 2.67,
    'NaubadMDH_BDR': 2.60,
    'ThavarekereMDH_THK': 2.44,
    'LargeLogicRameswaramODH_RMS': 2.24,
    'TTSPLKodaikanalODH_KDI': 2.22,
    'VadipattiMDH_VDP': 2.14,
    'KoorieeSoukyaRdTempODH_BLR': 0.96
}

# Hub Name to CLM Name and State mapping (from provided Hub/CLM/State list)
# Singaram hubs: LARGELOGICCHINNAMANURODH_CNM, TTSPLKODAIKANALODH_KDI, LARGELOGICKUNIYAMUTHURODH_CJB,
#                VadipattiMDH_VDP, TTSPLBATLAGUNDUODH_BGU
HUB_CLM_STATE_MAPPING = {
    # Singaram (Tamil Nadu)
    'LARGELOGICCHINNAMANURODH_CNM': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LargelogicChinnamanurODH_CNM': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LargeLogicChinnamanurODH_CNM': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLKODAIKANALODH_KDI': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLKodaikanalODH_KDI': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LARGELOGICKUNIYAMUTHURODH_CJB': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'LargeLogicKuniyamuthurODH_CJB': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'VadipattiMDH_VDP': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLBATLAGUNDUODH_BGU': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    'TTSPLBatlagunduODH_BGU': {'CLM Name': 'Singaram', 'State': 'Tamil Nadu'},
    # Madvesh (Tamil Nadu)
    'LARGELOGICRAMESWARAMODH_RMS': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    'LargeLogicRameswaramODH_RMS': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    'LARGELOGICDHARAPURAMODH_DHP': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    'LargeLogicDharapuramODH_DHP': {'CLM Name': 'Madvesh', 'State': 'Tamil Nadu'},
    # Karnataka & Telangana
    'BagaluruMDH_BAG': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'ElasticRunBidarODH_BDR': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'SITICSWadiODH_WDI': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'saidabadsplitODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'SaidabadSplitODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'HulimavuHub_BLR': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'ThavarekereMDH_THK': {'CLM Name': 'Irappa', 'State': 'Karnataka'},
    'KoorieeSoukyaRdTempODH_BLR': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'NaubadMDH_BDR': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'KOORIEEHAYATHNAGARODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'KoorieeHayathnagarODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'DommasandraSplitODH_DMN': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'KoorieeSoukyaRdODH_BLR': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
    'BidarFortHub_BDR': {'CLM Name': 'Haseem', 'State': 'Karnataka'},
    'CABTSRNAGARODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'CABTSRNagarODH_HYD': {'CLM Name': 'Asif, Haseem', 'State': 'Telengana'},
    'SulebeleMDH_SUL': {'CLM Name': 'Kishore', 'State': 'Karnataka'},
}

# Pre-built normalized lookup for reliable case-insensitive matching (source sheet hub names may vary)
def _build_hub_clm_normalized_lookup():
    d = {}
    for hub_key, clm_state in HUB_CLM_STATE_MAPPING.items():
        norm = str(hub_key).strip().lower() if hub_key else ""
        if norm:
            d[norm] = clm_state
    return d

_HUB_CLM_NORMALIZED_LOOKUP = _build_hub_clm_normalized_lookup()

# Google Sheets scopes
SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive'
]


def parse_date(date_str):
    """Parse date string in various formats and return date object"""
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    if not date_str:
        return None
    
    # Try Excel serial number first
    try:
        if date_str.replace('.', '').replace('-', '').replace('/', '').isdigit():
            excel_date = float(date_str)
            if excel_date > 59:
                excel_date -= 1
            excel_epoch = datetime(1899, 12, 30)
            parsed_datetime = excel_epoch + timedelta(days=excel_date)
            return parsed_datetime.date()
    except:
        pass
    
    # Remove time portion if present
    if ' ' in date_str:
        date_str = date_str.split(' ')[0]
    
    # Remove ordinal suffixes (1st, 2nd, 3rd, 4th, etc.) for parsing
    date_str_cleaned = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', date_str)
    
    # Try common date formats (prioritize %d-%b-%Y for dates like "12-Jan-2026")
    date_formats = [
        '%d-%b-%Y',   # 12-Jan-2026 (prioritize full date format)
        '%d-%b-%y',   # 12-Jan-26
        '%d-%b',      # 12-Jan or 28-Jan (after removing "th")
        '%d-%B',      # 12-January
        '%d %b',      # 28 Jan (with space, after removing "th")
        '%d %B',      # 28 January
        '%d %b %Y',   # 28 Jan 2026
        '%d-%B-%Y',   # 12-January-2026
        '%Y-%m-%d',   # 2026-01-12
        '%d-%m-%Y',   # 12-01-2026
        '%m/%d/%Y',   # 01/12/2026
        '%d/%m/%Y',   # 12/01/2026
        '%Y/%m/%d',   # 2026/01/12
        '%d.%m.%Y',   # 12.01.2026
        '%m-%d-%Y',   # 01-12-2026
        '%d/%m/%y',   # 12/01/26
    ]
    
    for fmt in date_formats:
        try:
            # Use cleaned date string (with ordinal suffixes removed)
            parsed_date = datetime.strptime(date_str_cleaned, fmt)
            result_date = parsed_date.date()
            
            # Handle formats without year (like "16-Dec" or "28 Dec") - use current year, but handle year wrapping
            if fmt in ['%d-%b', '%d-%B', '%d %b', '%d %B']:
                current_year = datetime.now().year
                if result_date.year == 1900:
                    try:
                        result_date = datetime(current_year, result_date.month, result_date.day).date()
                        # If the date is more than 30 days in the future, assume it's from the previous year
                        today = datetime.now().date()
                        days_ahead = (result_date - today).days
                        if days_ahead > 30:
                            result_date = datetime(current_year - 1, result_date.month, result_date.day).date()
                    except ValueError:
                        # Handle leap year edge case (Feb 29)
                        result_date = datetime(current_year, result_date.month, min(result_date.day, 28)).date()
            
            return result_date
        except (ValueError, TypeError):
            continue
    
    # Try pandas to_datetime as fallback
    try:
        parsed = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(parsed):
            return parsed.date()
    except:
        pass
    
    return None


def get_google_sheets_client():
    """Initialize and return Google Sheets client with retry logic for network/SSL errors"""
    max_retries = 3
    retry_delay = 5  # Start with 5 seconds
    
    for attempt in range(max_retries):
        try:
            logger.info("🔑 Setting up Google Sheets connection...")
            if attempt > 0:
                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries}...")
                time.sleep(retry_delay * attempt)  # Exponential backoff: 0s, 5s, 10s
            
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            client = gspread.authorize(creds)
            logger.info("✅ Google Sheets client initialized successfully")
            
            # Display service account email for sharing reference
            import json
            with open(SERVICE_ACCOUNT_FILE, 'r') as f:
                service_account_data = json.load(f)
                service_account_email = service_account_data.get('client_email', 'Not found')
                logger.info(f"📧 Service Account Email: {service_account_email}")
                logger.info("💡 Make sure the Google Sheet is shared with this email address (Editor access)")
            
            return client
        except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, 
                google.auth.exceptions.TransportError, urllib3.exceptions.SSLError,
                requests.exceptions.Timeout, urllib3.exceptions.NameResolutionError) as e:
            if attempt < max_retries - 1:
                wait_time = retry_delay * (attempt + 1)
                logger.warning(f"⚠️ Network/connection error (attempt {attempt + 1}/{max_retries}): {type(e).__name__}")
                logger.warning(f"⏳ Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            else:
                logger.error(f"❌ Failed to initialize Google Sheets client after {max_retries} attempts")
                logger.error(f"❌ Last error: {type(e).__name__}: {e}")
                logger.error("💡 This appears to be a network/connectivity issue.")
                logger.error("💡 Please check:")
                logger.error("   1. Your internet connection")
                logger.error("   2. DNS resolution (can you resolve sheets.googleapis.com?)")
                logger.error("   3. Firewall/proxy settings")
                logger.error("   4. Antivirus software blocking connections")
                logger.error("   5. Corporate network restrictions")
                raise
        except Exception as e:
            logger.error(f"❌ Error initializing Google Sheets client: {e}")
            raise


def retry_api_call(func, *args, max_retries=3, **kwargs):
    """Helper function to retry API calls with exponential backoff for rate limit and service unavailable errors"""
    retry_delay = 60  # Start with 60 seconds
    
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status_code = e.response.status_code if hasattr(e, 'response') and hasattr(e.response, 'status_code') else None
            # Retry for 429 (rate limit) and 503 (service unavailable) errors
            if status_code in [429, 503] and attempt < max_retries - 1:
                wait_time = retry_delay * (2 ** attempt)  # Exponential backoff: 60s, 120s, 240s
                error_type = "Rate limit exceeded" if status_code == 429 else "Service unavailable"
                logger.warning(f"⚠️ {error_type} ({status_code}). Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}...")
                time.sleep(wait_time)
                continue
            else:
                raise  # Re-raise if not a retryable error or out of retries
        except Exception as e:
            raise  # Re-raise non-API errors immediately


def get_column_letter(col_num):
    """Convert column number (1-based) to letter(s) (A, B, ..., Z, AA, AB, ...)"""
    result = ""
    while col_num > 0:
        col_num -= 1
        result = string.ascii_uppercase[col_num % 26] + result
        col_num //= 26
    return result


def clear_range(worksheet, start_row, end_row, num_cols):
    """Clear only a specific range in the worksheet, preserving other cells"""
    if num_cols == 0 or end_row < start_row:
        return
    
    start_col_letter = 'A'
    end_col_letter = get_column_letter(num_cols)
    range_str = f'{start_col_letter}{start_row}:{end_col_letter}{end_row}'
    
    try:
        worksheet.batch_clear([range_str])
        logger.info(f"🧹 Cleared range {range_str} (preserving other cells)")
    except Exception as e:
        logger.warning(f"⚠️ Could not clear range {range_str}: {e}")


def read_ncd_source_sheet(client):
    """Read data from the NCD % source sheet (same as reservations_email_automation.py)"""
    try:
        logger.info(f"📊 Opening NCD % source spreadsheet: {NCD_SOURCE_SPREADSHEET_ID}")
        spreadsheet = client.open_by_key(NCD_SOURCE_SPREADSHEET_ID)
        logger.info(f"✅ Opened spreadsheet: {spreadsheet.title}")
        
        # Use "Base Data" worksheet (same as reservations_email_automation.py)
        # This is the primary worksheet that contains all metrics including NCD %
        worksheet_name = "Base Data"
        worksheet = None
        
        # First, try exact match for "Base Data"
        for ws in spreadsheet.worksheets():
            if ws.title == "Base Data":
                worksheet = ws
                logger.info(f"✅ Found worksheet: {worksheet.title}")
                break
        
        # If not found, try case-insensitive match
        if not worksheet:
            for ws in spreadsheet.worksheets():
                if ws.title.lower() == worksheet_name.lower():
                    worksheet = ws
                    logger.info(f"✅ Found worksheet: {worksheet.title}")
                    break
        
        # Fallback: try to find worksheet that contains "Base" in the name (but not "Reservations")
        if not worksheet:
            for ws in spreadsheet.worksheets():
                if 'Base' in ws.title and 'Reservation' not in ws.title:
                    worksheet = ws
                    logger.info(f"✅ Found worksheet by name: {worksheet.title}")
                    break
        
        # Last fallback: try to find worksheet that contains "NCD" or "Breach" in the name
        if not worksheet:
            for ws in spreadsheet.worksheets():
                if 'NCD' in ws.title.upper() or 'Breach' in ws.title:
                    worksheet = ws
                    logger.info(f"✅ Found worksheet by name: {worksheet.title}")
                    break
        
        if not worksheet:
            logger.error(f"❌ Worksheet '{worksheet_name}' or alternatives not found!")
            logger.info(f"Available worksheets: {[ws.title for ws in spreadsheet.worksheets()]}")
            return None, None
        
        logger.info(f"✅ Using worksheet: {worksheet.title}")
        logger.info(f"   Rows: {worksheet.row_count}, Cols: {worksheet.col_count}")
        
        # Read all data
        logger.info("📖 Reading data from NCD % source worksheet...")
        values = worksheet.get_all_values()
        
        if not values:
            logger.warning("⚠️ No data found in NCD % source worksheet")
            return [], worksheet
        
        logger.info(f"✅ Read {len(values)} rows from NCD % source worksheet")
        
        return values, worksheet
    
    except Exception as e:
        logger.error(f"❌ Error reading NCD % source worksheet: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise


def find_hub_name_column(headers):
    """Find the 'Hub Name' column index - try various possible names"""
    # Exclude these from being considered as Hub Name
    exclude_as_hub_name = ['zone', 'zonal head', 'threshold', 'total', 'summary']
    
    # Try exact match first (with space, underscore, or variations)
    for idx, header in enumerate(headers):
        if not header:
            continue
        header_str = str(header).strip()
        header_lower = header_str.lower()
        # Skip if it's in exclude list
        if header_lower in exclude_as_hub_name:
            continue
        # Check for various hub name formats
        if (header_lower == 'hub name' or header_lower == 'hub_name' or 
            header_lower == 'hub' or header_lower.replace('_', ' ') == 'hub name' or
            header_lower.replace(' ', '_') == 'hub_name'):
            return idx
    
    # If not found, check if first column might be hub name (but not if it's in exclude list)
    if len(headers) > 0 and headers[0]:
        first_col = str(headers[0]).strip()
        first_col_lower = first_col.lower()
        # Don't use if it's in exclude list
        if first_col_lower in exclude_as_hub_name:
            return None
        # Check if first column looks like a hub name (not a date, not a percentage, not a number)
        if '%' not in first_col and not first_col.replace('.', '').replace('-', '').isdigit():
            try:
                parse_date(first_col)
                # If it's a date, don't use it
            except:
                # Not a date, might be hub name
                return 0
    
    return None


def convert_ncd_to_dataframe(values):
    """Convert NCD % worksheet values to pandas DataFrame
    Uses same structure as reservations_email_automation.py (headers in row 2, index 1)
    """
    try:
        if not values:
            logger.warning("⚠️ No data to convert to DataFrame")
            return pd.DataFrame()
        
        # Always use row 2 (index 1) as header row (same as reservations_email_automation.py)
        # This is MANUAL_HEADER_ROW_INDEX = 1
        # DO NOT change this based on where dates are found - the structure is fixed
        header_row_idx = 1
        logger.info(f"📅 Using row {header_row_idx + 1} (index {header_row_idx}) as header row (same as reservations script)")
        
        if len(values) <= header_row_idx:
            logger.warning("⚠️ Not enough rows for headers")
            return pd.DataFrame()
        
        headers = values[header_row_idx]
        
        # Log actual headers for debugging
        logger.info(f"📋 Header row has {len(headers)} columns")
        logger.info(f"📋 First 10 headers: {headers[:10]}")
        
        # Handle empty headers - pandas will create duplicate column names if headers are empty
        # We need to ensure unique column names
        processed_headers = []
        header_counts = {}
        for idx, header in enumerate(headers):
            header_str = str(header).strip() if header else ''
            if not header_str:
                # Generate a unique name for empty headers
                header_str = f"Column_{idx + 1}"
            # Handle duplicate headers
            if header_str in header_counts:
                header_counts[header_str] += 1
                header_str = f"{header_str}_{header_counts[header_str]}"
            else:
                header_counts[header_str] = 0
            processed_headers.append(header_str)
        
        # If dates are in row 1, data starts from row 2 (index 1)
        # If dates are in row 2, data starts from row 3 (index 2)
        data_start_idx = header_row_idx + 1
        data = values[data_start_idx:] if len(values) > data_start_idx else []
        
        # Create DataFrame with processed headers
        df = pd.DataFrame(data, columns=processed_headers)
        
        # Store original headers (before processing) for reference
        original_headers_raw = headers
        
        # Store original headers (raw) and processed headers as attributes for reference
        df.attrs['original_headers'] = processed_headers  # Use processed headers for column matching
        df.attrs['original_headers_raw'] = original_headers_raw  # Keep raw headers for reference
        df.attrs['original_values'] = values
        df.attrs['header_row_idx'] = header_row_idx
        
        # Create a mapping from date/header string to column index for easier lookup
        header_to_col_idx = {}
        for idx, header in enumerate(processed_headers):
            header_str = str(header).strip() if header else ''
            if header_str:
                header_to_col_idx[header_str] = idx
        
        df.attrs['header_to_col_idx'] = header_to_col_idx
        
        # Try to identify and rename Hub Name column if it exists with a different name
        hub_name_col_idx = find_hub_name_column(processed_headers)
        if hub_name_col_idx is not None:
            # Rename the column to 'Hub Name' for consistency
            old_name = processed_headers[hub_name_col_idx]
            df = df.rename(columns={old_name: 'Hub Name'})
            # Update the processed headers too
            processed_headers[hub_name_col_idx] = 'Hub Name'
            df.attrs['original_headers'] = processed_headers
            logger.info(f"✅ Identified and renamed column '{old_name}' to 'Hub Name'")
        elif 'Hub Name' not in df.columns and 'Hub_name' in df.columns:
            # Check for 'Hub_name' with underscore
            df = df.rename(columns={'Hub_name': 'Hub Name'})
            # Update processed headers
            for idx, h in enumerate(processed_headers):
                if str(h).strip() == 'Hub_name':
                    processed_headers[idx] = 'Hub Name'
                    break
            df.attrs['original_headers'] = processed_headers
            logger.info(f"✅ Found and renamed 'Hub_name' to 'Hub Name'")
        elif 'Hub Name' not in df.columns:
            # If still not found, try to use first column as Hub Name
            if len(df.columns) > 0:
                first_col = df.columns[0]
                df = df.rename(columns={first_col: 'Hub Name'})
                processed_headers[0] = 'Hub Name'
                df.attrs['original_headers'] = processed_headers
                logger.info(f"✅ Using first column '{first_col}' as 'Hub Name'")
        
        logger.info(f"✅ Converted to DataFrame: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"📋 Columns: {list(df.columns)}")
        
        return df
    
    except Exception as e:
        logger.error(f"❌ Error converting NCD % data to DataFrame: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise


def _find_value_column(df, value_column_hint):
    """Find the value column by exact name or fuzzy match. value_column_hint is 'NCD %' or 'NCD #'.
    Prefers exact match (e.g. 'NCD %' over 'Conv %' when both exist).
    """
    if value_column_hint == 'NCD %':
        exact_first = ['NCD %', 'Conv %']
        def fuzzy_check(h):
            hl = str(h).lower()
            return (('ncd' in hl or 'conv' in hl) and '%' in str(h))
    else:  # NCD #
        exact_first = ['NCD #']
        def fuzzy_check(h):
            hl = str(h).lower()
            return 'ncd' in hl and '#' in str(h) and '%' not in str(h)
    
    # Prefer exact match first
    for preferred in exact_first:
        if preferred in df.columns:
            return preferred
    # Fallback to fuzzy
    for col in df.columns:
        col_str = str(col).strip()
        if fuzzy_check(col_str):
            return col_str
    return None


def create_ncd_trend_table_by_dates(df, target_hub_names=None, days_to_fetch=7, value_column='NCD %'):
    """Create trend table grouped by Hub Name with latest N days as columns.
    Supports both NCD % (percentage) and NCD # (count) columns.
    value_column: 'NCD %' or 'NCD #'
    Returns: DataFrame with daily values for latest N days (same format for both)
    """
    try:
        if df.empty:
            logger.warning("⚠️ No data to create trend table")
            return pd.DataFrame()
        
        is_percentage = (value_column == 'NCD %')
        
        # Get original headers (row 2, index 1)
        original_headers = df.attrs.get('original_headers', None)
        
        # Find Date column
        date_col_name = None
        for col_idx, header in enumerate(original_headers if original_headers else df.columns):
            header_str = str(header).strip() if header else ''
            if header_str == 'Date' or header_str.lower() == 'date':
                date_col_name = header_str if header_str in df.columns else 'Date'
                break
        if not date_col_name and 'Date' in df.columns:
            date_col_name = 'Date'
        
        # Find value column (NCD % or NCD #)
        value_col_name = _find_value_column(df, value_column)
        
        if not date_col_name:
            logger.error("❌ 'Date' column not found")
            return pd.DataFrame()
        
        if not value_col_name:
            logger.error(f"❌ '{value_column}' column not found")
            logger.info(f"Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        logger.info(f"📊 Using Date: '{date_col_name}', Value column: '{value_col_name}' ({value_column})")
        
        # Check if Hub Name column exists (might be 'Hub_name' or 'Hub Name')
        hub_name_col = None
        if 'Hub Name' in df.columns:
            hub_name_col = 'Hub Name'
        elif 'Hub_name' in df.columns:
            hub_name_col = 'Hub_name'
            df = df.rename(columns={'Hub_name': 'Hub Name'})
        else:
            logger.error("❌ 'Hub Name' or 'Hub_name' column not found")
            logger.info(f"Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        # Parse dates from Date column and filter to latest N days
        logger.info(f"📅 Parsing dates from '{date_col_name}' column...")
        df['_parsed_date'] = df[date_col_name].apply(parse_date)
        
        # Filter out rows with invalid dates
        valid_dates_df = df[df['_parsed_date'].notna()].copy()
        if len(valid_dates_df) == 0:
            logger.warning("⚠️ No rows with valid dates found")
            return pd.DataFrame()
        
        logger.info(f"📊 Found {len(valid_dates_df)} rows with valid dates")
        
        # Filter to latest N days (up to yesterday)
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)
        logger.info(f"📅 Today: {today}, Yesterday: {yesterday}, Filtering dates <= {yesterday}")
        
        valid_dates_df = valid_dates_df[valid_dates_df['_parsed_date'] <= yesterday].copy()
        
        if len(valid_dates_df) == 0:
            logger.warning("⚠️ No rows with dates <= yesterday found")
            return pd.DataFrame()
        
        # Get unique dates and select latest N
        unique_dates = sorted(valid_dates_df['_parsed_date'].unique(), reverse=True)[:days_to_fetch]
        unique_dates = sorted(unique_dates)  # Sort chronologically for display
        
        if len(unique_dates) == 0:
            logger.warning("⚠️ No valid dates found for trend analysis")
            return pd.DataFrame()
        
        logger.info(f"✅ Selected {len(unique_dates)} latest dates: {[d.strftime('%d-%b-%Y') for d in unique_dates]}")
        
        # Filter to only selected dates
        valid_dates_df = valid_dates_df[valid_dates_df['_parsed_date'].isin(unique_dates)].copy()
        
        logger.info(f"📊 Filtered to {len(valid_dates_df)} rows for selected dates")
        
        # Helper function to extract numeric values (works for both % and count)
        def extract_numeric(value):
            if pd.isna(value) or value == '' or value is None:
                return None
            if isinstance(value, (int, float)):
                return float(value) if not pd.isna(value) else None
            if isinstance(value, str):
                cleaned = str(value).replace('%', '').replace('#', '').replace(',', '').replace(' ', '').strip()
                if cleaned:
                    try:
                        return float(cleaned)
                    except (ValueError, TypeError):
                        return None
            try:
                num_val = pd.to_numeric(value, errors='coerce')
                return float(num_val) if not pd.isna(num_val) else None
            except:
                return None
        
        # Extract values from the value column
        valid_dates_df['_value'] = valid_dates_df[value_col_name].apply(extract_numeric)
        
        # Log sample data
        if len(valid_dates_df) > 0:
            sample_row = valid_dates_df.iloc[0]
            logger.info(f"📋 Sample row: Hub='{sample_row.get('Hub Name', 'N/A')}', Date='{sample_row.get(date_col_name, 'N/A')}', {value_column}='{sample_row.get(value_col_name, 'N/A')}', Parsed={sample_row.get('_value', 'N/A')}")
        
        # Filter by target hub names
        if target_hub_names:
            logger.info(f"🔍 Filtering by {len(target_hub_names)} target hub names...")
            
            # Normalize hub names for comparison
            valid_dates_df['Hub Name'] = valid_dates_df['Hub Name'].astype(str).str.strip()
            normalized_target_hubs = [str(h).strip() for h in target_hub_names]
            
            # Create mapping for case-insensitive matching
            target_hubs_lower = {h.lower(): h for h in normalized_target_hubs}
            available_hubs_lower = {h.lower(): h for h in valid_dates_df['Hub Name'].dropna().unique()}
            
            # Find matching hubs
            matched_source_hubs = []
            for target_hub in normalized_target_hubs:
                target_lower = target_hub.lower()
                if target_lower in available_hubs_lower:
                    source_hub = available_hubs_lower[target_lower]
                    matched_source_hubs.append(source_hub)
                    if target_hub != source_hub:
                        logger.info(f"🔍 Matched '{target_hub}' to source hub '{source_hub}' (case variation)")
            
            if matched_source_hubs:
                valid_dates_df = valid_dates_df[valid_dates_df['Hub Name'].isin(matched_source_hubs)].copy()
                logger.info(f"✅ Filtered to {len(valid_dates_df)} rows for {len(matched_source_hubs)} target hubs")
            else:
                logger.warning("⚠️ No hubs found matching target hub names")
                return pd.DataFrame()
        
        # Get unique hubs
        unique_hubs = sorted(valid_dates_df['Hub Name'].dropna().unique())
        logger.info(f"📊 Found {len(unique_hubs)} unique hubs")
        
        if len(unique_hubs) == 0:
            logger.warning("⚠️ No hubs found after filtering")
            return pd.DataFrame()
        
        # Format date column names for output
        formatted_date_cols = []
        for date_obj in unique_dates:
            formatted_date_cols.append(date_obj.strftime('%d-%b'))
        
        logger.info(f"📅 Creating trend table with dates as columns: {formatted_date_cols}")
        
        # Pivot the data: Create one row per hub, with dates as columns
        # Structure: Each row in source has Date, Hub_name, Conv % columns
        # We pivot so Hub Name becomes rows and dates become columns
        pivot_df = valid_dates_df.pivot_table(
            index='Hub Name',
            columns='_parsed_date',
            values='_value',
            aggfunc='first'  # Use first value if duplicates exist
        ).reset_index()
        
        # Rename date columns to formatted strings
        new_columns = ['Hub Name']
        for col in pivot_df.columns[1:]:  # Skip 'Hub Name' column
            if isinstance(col, date):
                new_columns.append(col.strftime('%d-%b'))
            else:
                new_columns.append(str(col))
        
        pivot_df.columns = new_columns
        
        logger.info(f"📊 Pivoted data: {len(pivot_df)} hubs × {len(formatted_date_cols)} date columns")
        
        # Create NCD % trend table (one row per Hub)
        ncd_data = []
        
        for hub_name in unique_hubs:
            hub_row = {'Hub Name': hub_name}
            
            # Add CLM Name and State from mapping (case-insensitive lookup via normalized dict)
            hub_name_normalized = str(hub_name).strip().lower() if hub_name else ""
            clm_state_match = _HUB_CLM_NORMALIZED_LOOKUP.get(hub_name_normalized) if hub_name_normalized else None
            
            if not clm_state_match and hub_name_normalized:
                logger.warning(f"⚠️ No CLM mapping for hub: '{hub_name}' (normalized: '{hub_name_normalized}')")
            
            if clm_state_match:
                hub_row['CLM Name'] = clm_state_match['CLM Name']
                hub_row['State'] = clm_state_match['State']
            else:
                hub_row['CLM Name'] = ''
                hub_row['State'] = ''
            
            # Add Volume Weight from mapping (case-insensitive lookup, fixed value with % sign, rounded to whole number)
            volume_weight_match = None
            for mapped_hub, weight in HUB_VOLUME_WEIGHT_MAPPING.items():
                if mapped_hub.lower() == hub_name_normalized:
                    volume_weight_match = weight
                    break
            
            if volume_weight_match is not None:
                hub_row['Volume Weight'] = f"{round(volume_weight_match)}%"
            else:
                hub_row['Volume Weight'] = ''
            
            # Extract NCD % values for each date from pivot_df
            hub_pivot_row = pivot_df[pivot_df['Hub Name'] == hub_name]
            
            if not hub_pivot_row.empty:
                for date_col in formatted_date_cols:
                    if date_col in hub_pivot_row.columns:
                        val = hub_pivot_row[date_col].iloc[0]
                        if pd.notna(val) and val is not None:
                            hub_row[date_col] = f"{round(val, 2)}%" if is_percentage else str(int(round(val, 0)))
                        else:
                            hub_row[date_col] = "N/A"
                    else:
                        hub_row[date_col] = "N/A"
            else:
                # Hub not found in pivot - set all dates to N/A
                for date_col in formatted_date_cols:
                    hub_row[date_col] = "N/A"
            
            ncd_data.append(hub_row)
        
        # Create DataFrame
        ncd_df = pd.DataFrame(ncd_data)
        
        if ncd_df.empty:
            logger.warning("⚠️ No hub data found - DataFrame is empty")
            return pd.DataFrame()
        
        # Log extraction summary
        total_cells = len(unique_hubs) * len(formatted_date_cols)
        na_count = sum(1 for row in ncd_data for col in formatted_date_cols if row.get(col) == "N/A")
        extracted_count = total_cells - na_count
        logger.info(f"📊 Extraction summary: {extracted_count}/{total_cells} values extracted ({extracted_count*100/total_cells:.1f}%), {na_count} N/A values")
        
        # Calculate AVG column (average of last N days) for each hub
        def parse_cell_value(val):
            if pd.isna(val) or val == "N/A" or val == '':
                return None
            try:
                return float(str(val).replace('%', '').replace(',', '').strip())
            except (ValueError, AttributeError):
                return None
        
        avg_values = []
        for idx, row in ncd_df.iterrows():
            date_values = [parse_cell_value(row.get(col)) for col in formatted_date_cols if col in row]
            date_values = [v for v in date_values if v is not None]
            if len(date_values) > 0:
                avg_val = sum(date_values) / len(date_values)
                avg_values.append(f"{round(avg_val, 2)}%" if is_percentage else str(int(round(avg_val, 0))))
            else:
                avg_values.append("N/A")
        
        ncd_df['AVG'] = avg_values
        
        # Reorder columns: Hub Name, CLM Name, State, date columns, AVG, then Volume Weight
        cols = ['Hub Name', 'CLM Name', 'State'] + formatted_date_cols + ['AVG', 'Volume Weight']
        # Filter to only include columns that exist in the DataFrame
        cols = [col for col in cols if col in ncd_df.columns]
        # Create new DataFrame with only the specified columns in the correct order
        ncd_df = ncd_df[cols].copy()
        
        # Sort by AVG column in ascending order (before adding Total row)
        def extract_avg_value(avg_str):
            if pd.isna(avg_str) or avg_str == "N/A" or avg_str == '':
                return float('inf')  # Put N/A values at the end
            try:
                return float(str(avg_str).replace('%', '').replace(',', '').strip())
            except (ValueError, AttributeError):
                return float('inf')  # Put invalid values at the end
        
        ncd_df['_sort_key'] = ncd_df['AVG'].apply(extract_avg_value)
        ncd_df = ncd_df.sort_values('_sort_key', ascending=True).reset_index(drop=True)
        ncd_df = ncd_df.drop(columns=['_sort_key'])  # Remove temporary sort key column
        
        logger.info("✅ Sorted hubs by AVG in ascending order")
        
        # Add Total row: weighted average for %, sum for count (#)
        total_row = {}
        total_row['Hub Name'] = 'Total'
        total_row['CLM Name'] = ''
        total_row['State'] = ''
        total_row['Volume Weight'] = ''
        
        def extract_volume_weight(weight_str):
            if pd.isna(weight_str) or weight_str == '' or weight_str is None:
                return None
            try:
                cleaned = str(weight_str).replace('%', '').strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
            return None
        
        volume_weights = ncd_df['Volume Weight'].apply(extract_volume_weight).values
        total_agg_values = []
        
        for date_col in formatted_date_cols:
            date_values = []
            weights_for_date = []
            for position, (idx, row) in enumerate(ncd_df.iterrows()):
                date_value_str = str(row[date_col]) if date_col in row else ''
                num_val = extract_numeric(date_value_str)
                weight_val = volume_weights[position] if position < len(volume_weights) else None
                if num_val is not None:
                    if is_percentage and weight_val is not None:
                        date_values.append(num_val)
                        weights_for_date.append(weight_val)
                    elif not is_percentage:
                        date_values.append(num_val)
            
            if is_percentage and len(date_values) > 0 and len(weights_for_date) > 0:
                weighted_sum = sum(nv * w for nv, w in zip(date_values, weights_for_date))
                total_weight = sum(weights_for_date)
                if total_weight > 0:
                    weighted_avg = weighted_sum / total_weight
                    total_row[date_col] = f"{round(weighted_avg, 2)}%"
                    total_agg_values.append(weighted_avg)
                else:
                    total_row[date_col] = "N/A"
            elif not is_percentage and len(date_values) > 0:
                total_sum = sum(date_values)
                total_row[date_col] = str(int(round(total_sum, 0)))
                total_agg_values.append(total_sum)
            else:
                total_row[date_col] = "N/A"
        
        if len(total_agg_values) > 0:
            overall = sum(total_agg_values) / len(total_agg_values) if is_percentage else sum(total_agg_values)
            total_row['AVG'] = f"{round(overall, 2)}%" if is_percentage else str(int(round(sum(total_agg_values) / len(total_agg_values), 0)))
        else:
            total_row['AVG'] = "N/A"
        
        # Ensure total_row has all required columns
        required_cols = ['Hub Name', 'CLM Name', 'State'] + formatted_date_cols + ['AVG', 'Volume Weight']
        for col in required_cols:
            if col not in total_row:
                if col in ['CLM Name', 'State', 'Volume Weight']:
                    total_row[col] = ''  # Empty string for these columns
                else:
                    total_row[col] = "N/A"  # N/A for date columns and AVG if missing
        
        # Create Total row DataFrame with ALL columns from ncd_df (in correct order)
        cols = ['Hub Name', 'CLM Name', 'State'] + formatted_date_cols + ['AVG', 'Volume Weight']
        cols = [col for col in cols if col in ncd_df.columns]
        
        # Build a list with values in the same order as columns
        total_row_values = []
        for col in cols:
            total_row_values.append(total_row.get(col, "N/A" if col not in ['CLM Name', 'State', 'Volume Weight'] else ''))
        
        # Create DataFrame with same columns and order as ncd_df
        total_df_row = pd.DataFrame([total_row_values], columns=cols)
        
        # Append Total row
        logger.info(f"📊 Adding Total row. DataFrame has {len(ncd_df)} rows before adding Total")
        ncd_df = pd.concat([ncd_df, total_df_row], ignore_index=True)
        logger.info(f"📊 DataFrame has {len(ncd_df)} rows after concatenation")
        
        # Verify Total row was added
        total_rows_count = len(ncd_df[ncd_df['Hub Name'] == 'Total'])
        logger.info(f"✅ Total row added: {total_rows_count} Total row(s) found in DataFrame")
        
        logger.info(f"✅ Created NCD % trend table by dates: {len(ncd_df)} rows × {len(ncd_df.columns)} columns")
        
        return ncd_df
    
    except Exception as e:
        logger.error(f"❌ Error creating NCD % trend table by dates: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        return pd.DataFrame()


def _format_trend_table(worksheet, start_row, num_rows, num_cols, df_columns):
    """Apply same formatting to a trend table (NCD % or NCD #) at the given start row.
    num_rows = header + data rows (1 + len(df)).
    """
    if num_cols <= 0:
        return
    if num_cols <= 26:
        last_col_letter = string.ascii_uppercase[num_cols - 1]
    else:
        first_letter = string.ascii_uppercase[(num_cols - 1) // 26 - 1]
        second_letter = string.ascii_uppercase[(num_cols - 1) % 26]
        last_col_letter = f"{first_letter}{second_letter}"
    end_row = start_row + num_rows - 1  # last row with data
    header_range = f'A{start_row}:{last_col_letter}{start_row}'
    retry_api_call(worksheet.format, header_range, {
        'backgroundColor': {'red': 0.2, 'green': 0.6, 'blue': 0.9},
        'textFormat': {'bold': True},
        'horizontalAlignment': 'CENTER'
    })
    retry_api_call(worksheet.format, f'A{start_row}:C{start_row}', {'horizontalAlignment': 'LEFT'})
    retry_api_call(worksheet.format, f'A{start_row}:A{end_row}', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })
    retry_api_call(worksheet.format, f'B{start_row}:B{end_row}', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })
    retry_api_call(worksheet.format, f'C{start_row}:C{end_row}', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.9, 'green': 0.9, 'blue': 0.9}
    })
    if 'AVG' in df_columns:
        avg_col_idx = list(df_columns).index('AVG')
        avg_col_letter = string.ascii_uppercase[avg_col_idx] if avg_col_idx < 26 else \
            f"{string.ascii_uppercase[avg_col_idx // 26 - 1]}{string.ascii_uppercase[avg_col_idx % 26]}"
        retry_api_call(worksheet.format, f'{avg_col_letter}{start_row}:{avg_col_letter}{end_row}', {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
        })
    if 'Volume Weight' in df_columns:
        vol_idx = list(df_columns).index('Volume Weight')
        vol_letter = string.ascii_uppercase[vol_idx] if vol_idx < 26 else \
            f"{string.ascii_uppercase[vol_idx // 26 - 1]}{string.ascii_uppercase[vol_idx % 26]}"
        retry_api_call(worksheet.format, f'{vol_letter}{start_row}:{vol_letter}{end_row}', {
            'textFormat': {'bold': True},
            'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
        })
    retry_api_call(worksheet.format, f'A{end_row}:{last_col_letter}{end_row}', {
        'textFormat': {'bold': True},
        'backgroundColor': {'red': 0.95, 'green': 0.95, 'blue': 0.7}
    })


def main():
    """Main function"""
    try:
        logger.info("="*60)
        logger.info("🚀 NCD BREACH % TREND ANALYZER")
        logger.info("="*60)
        
        # Setup Google Sheets client
        client = get_google_sheets_client()
        
        # Read NCD source sheet
        logger.info("\n" + "="*60)
        logger.info("📊 EXTRACTING NCD % AND NCD # TRENDS")
        logger.info("="*60)
        
        ncd_values, ncd_worksheet = read_ncd_source_sheet(client)
        
        if ncd_values is not None and len(ncd_values) > 0:
            # Convert to DataFrame
            ncd_df = convert_ncd_to_dataframe(ncd_values)
            
            if not ncd_df.empty:
                # Create NCD % trend table
                ncd_pct_trend_df = create_ncd_trend_table_by_dates(
                    ncd_df, 
                    target_hub_names=TARGET_HUB_NAMES,
                    days_to_fetch=NCD_DAYS_TO_FETCH,
                    value_column='NCD %'
                )
                # Create NCD # trend table
                ncd_hash_trend_df = create_ncd_trend_table_by_dates(
                    ncd_df, 
                    target_hub_names=TARGET_HUB_NAMES,
                    days_to_fetch=NCD_DAYS_TO_FETCH,
                    value_column='NCD #'
                )
                
                has_pct = not ncd_pct_trend_df.empty
                has_hash = not ncd_hash_trend_df.empty
                if has_pct or has_hash:
                    logger.info("⏳ Waiting 5 seconds before writing to sheet...")
                    time.sleep(5)
                    
                    logger.info(f"📊 Opening destination spreadsheet: {NCD_DEST_SPREADSHEET_ID}")
                    ncd_dest_spreadsheet = client.open_by_key(NCD_DEST_SPREADSHEET_ID)
                    logger.info(f"✅ Opened destination spreadsheet: {ncd_dest_spreadsheet.title}")
                    
                    try:
                        ncd_trend_worksheet = ncd_dest_spreadsheet.worksheet(NCD_DEST_WORKSHEET_NAME)
                        logger.info(f"✅ Found worksheet '{NCD_DEST_WORKSHEET_NAME}'")
                        time.sleep(1)
                    except gspread.WorksheetNotFound:
                        ncd_trend_worksheet = ncd_dest_spreadsheet.add_worksheet(
                            title=NCD_DEST_WORKSHEET_NAME, rows=1000, cols=100
                        )
                        logger.info(f"✅ Created new worksheet '{NCD_DEST_WORKSHEET_NAME}'")
                    
                    def convert_to_serializable(obj):
                        if pd.isna(obj) or obj is None:
                            return None
                        elif isinstance(obj, (np.integer, np.int64, np.int32)):
                            return int(obj)
                        elif isinstance(obj, (np.floating, np.float64, np.float32)):
                            return float(obj)
                        elif isinstance(obj, np.bool_):
                            return bool(obj)
                        return str(obj) if obj is not None else None
                    
                    current_row = 1
                    num_cols = max(
                        len(ncd_pct_trend_df.columns) if has_pct else 0,
                        len(ncd_hash_trend_df.columns) if has_hash else 0
                    )
                    
                    # Clear range covering both tables (approx. 60 rows for 2 tables + gaps)
                    clear_range(ncd_trend_worksheet, 1, 65, num_cols)
                    
                    # Write NCD % table
                    if has_pct:
                        df_ser = ncd_pct_trend_df.map(convert_to_serializable)
                        set_with_dataframe(ncd_trend_worksheet, df_ser, row=current_row, resize=False)
                        ncd_pct_rows = len(ncd_pct_trend_df) + 1
                        _format_trend_table(ncd_trend_worksheet, current_row, ncd_pct_rows, len(ncd_pct_trend_df.columns), ncd_pct_trend_df.columns)
                        current_row += ncd_pct_rows
                        logger.info(f"✅ NCD % table: {len(ncd_pct_trend_df)} rows × {len(ncd_pct_trend_df.columns)} columns")
                    
                    # Gap + section label + NCD # table
                    current_row += 2  # 2 blank rows
                    if has_hash:
                        # Section header
                        ncd_trend_worksheet.update_acell(f'A{current_row}', 'NCD #')
                        retry_api_call(ncd_trend_worksheet.format, f'A{current_row}', {
                            'textFormat': {'bold': True, 'fontSize': 12},
                            'backgroundColor': {'red': 0.85, 'green': 0.85, 'blue': 0.85}
                        })
                        current_row += 1
                        # Write NCD # table
                        df_ser = ncd_hash_trend_df.map(convert_to_serializable)
                        set_with_dataframe(ncd_trend_worksheet, df_ser, row=current_row, resize=False)
                        ncd_hash_rows = len(ncd_hash_trend_df) + 1
                        _format_trend_table(ncd_trend_worksheet, current_row, ncd_hash_rows, len(ncd_hash_trend_df.columns), ncd_hash_trend_df.columns)
                        logger.info(f"✅ NCD # table: {len(ncd_hash_trend_df)} rows × {len(ncd_hash_trend_df.columns)} columns")
                    
                    logger.info("✅ Applied formatting to 'NCD Breach' worksheet")

                    # Send two images to WhatsApp: A1:S23 (NCD % table) and A26:T49 (NCD # table)
                    def _wh_log(msg, level):
                        if level == 'ERROR':
                            logger.error(msg)
                        elif level == 'WARNING':
                            logger.warning(msg)
                        else:
                            logger.info(msg)
                    ts = datetime.now().strftime('%d-%b-%Y %H:%M')
                    try:
                        send_sheet_range_to_whatsapp(
                            ncd_trend_worksheet,
                            range="A1:S23",
                            caption=f"NCD Breach % - {ts}",
                            log_func=_wh_log,
                        )
                        send_sheet_range_to_whatsapp(
                            ncd_trend_worksheet,
                            range="A26:T49",
                            caption=f"NCD Breach # - {ts}",
                            log_func=_wh_log,
                        )
                    except Exception as e:
                        logger.warning(f"WhatsApp send failed (non-fatal): {e}")

                    spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{NCD_DEST_SPREADSHEET_ID}/edit"
                    logger.info(f"\n🔗 Destination Sheet URL: {spreadsheet_url}")
                else:
                    logger.warning("⚠️ Both NCD % and NCD # trend tables are empty")
            else:
                logger.warning("⚠️ NCD DataFrame is empty")
        else:
            logger.warning("⚠️ No data found in NCD source sheet")
        
        logger.info("\n" + "="*60)
        logger.info("✅ Successfully extracted NCD % trend!")
        logger.info("="*60)
    
    except Exception as e:
        logger.error(f"❌ Error in main: {e}")
        import traceback
        logger.error(f"Full traceback:\n{traceback.format_exc()}")
        raise


if __name__ == "__main__":
    main()

