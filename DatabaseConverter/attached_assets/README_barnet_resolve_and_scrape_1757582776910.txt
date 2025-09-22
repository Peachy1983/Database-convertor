
Barnet: Resolve keyVal URL from Reference + Scrape Contacts
===========================================================
This single script converts each Reference into a working Barnet PublicAccess URL (with keyVal)
and then scrapes Applicant/Agent info from the contacts tab.

Usage
-----
1) Install dependencies (same as before):
   pip install -r requirements.txt

2) Run on your export (expects sheet "Export" with columns "Reference" and "URL"):
   python barnet_resolve_and_scrape.py "Nimbus Maps - Planning Data (10) barnet.xlsx" --out-xlsx "barnet_enriched.xlsx"

What it does
------------
- If "URL" is missing or not an applicationDetails link, it searches Idox for the reference and
  builds a stable summary link (activeTab=summary&keyVal=...).
- Tries contacts-like tabs and parses Applicant/Agent blocks into fields.
- Writes a single enriched Excel/CSV with:
  - Resolved URL
  - URL Resolve Status (how it found the link)
  - Scrape Status
  - applicant_* / agent_* fields when available

Notes
-----
- Keep the default delay (or go slower) to be polite to the site.
- If Barnet changes their HTML, adjust parse_contacts_html().
