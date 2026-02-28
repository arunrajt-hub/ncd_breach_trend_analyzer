# NCD Breach Trend Analyzer

Extracts NCD % and NCD # breach trends from a source Google Sheet and writes to a destination sheet. Sends two images to WhatsApp: NCD % table (A1:S23) and NCD # table (A26:T49).

## Schedule

Runs **daily at 1 PM IST** via GitHub Actions.

## Setup (GitHub)

1. **Create a new repository** on GitHub (e.g. `ncd-breach-trend-analyzer`).

2. **Push this folder** to the repo:
   ```bash
   cd github_repos/ncd_breach_trend_analyzer
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/YOUR_USERNAME/ncd-breach-trend-analyzer.git
   git push -u origin main
   ```

3. **Add Secrets** in repo Settings → Secrets and variables → Actions:
   - `SERVICE_ACCOUNT_JSON` – Full JSON content of your Google service account key
   - `WHAPI_TOKEN` – Your WHAPI token from https://whapi.cloud/
   - `WHATSAPP_PHONE` – Recipient(s), comma-separated (e.g. `120363320457092145@g.us,919500055366`)

4. **Share the Google Sheets** with the service account email (Editor access):
   - Source sheet: Base Data worksheet
   - Destination sheet: NCD Breach worksheet

## Local Run

```bash
pip install -r requirements.txt
# Place service_account_key.json in this folder
python ncd_breach_trend_analyzer.py
```
