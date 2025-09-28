# Yelp and Google Business Integration

This project integrates **Yelp Fusion API** and **Google Maps Places API** to fetch enriched restaurant information for **Charlotte, NC**.

## Setup

1. **Clone the repo**
   ```bash
   git clone <repo_url>
   cd paradane-business-bot
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Environment variables**

   Add the following keys in `.env`:
   ```
   YELP_API_KEY=your_yelp_api_key
   GOOGLE_API_KEY=your_google_api_key
   ```

   - Get a Yelp API key here: https://fusion.yelp.com
   - Get a Google API key here: https://console.cloud.google.com/apis/credentials (Enable Maps and Places APIs)

## Running

```bash
python -m project.main
```

## Social media extraction

The crawler extracts social media profile links from each crawled page and stores them in `business_pages.social_links` as a comma-separated list of `platform:url` entries, for example:

```
facebook:https://www.facebook.com/acme,instagram:https://instagram.com/acme
```

- Extraction: see [project/helpers/page_processor.py](project/helpers/page_processor.py:1) `PageProcessor.extract_social_links()`
- Pipeline save: see [project/helpers/pipeline.py](project/helpers/pipeline.py:141)
- Aggregation for report: see [project/reporting/utils/web.py](project/reporting/utils/web.py:75) `collectBusinessSocials()`
- Rendering in Business Report: see [project/reporting/business_report.py](project/reporting/business_report.py:424) and [project/template/business-report.html](project/template/business-report.html:108)

Schema migration:

Run the SQL (idempotent) to add the column if it doesn't exist:
```sql
ALTER TABLE business_pages
ADD COLUMN IF NOT EXISTS social_links TEXT NULL;
```