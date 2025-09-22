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
