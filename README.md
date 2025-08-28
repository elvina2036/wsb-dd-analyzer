# WSB-DD Analyzer (Backend)

A backend system for analyzing Reddit WallStreetBets due diligence posts and extracting ticker mentions.  
The project uses **Google Apps Script (GAS)** to connect Reddit posts, external finance APIs, and Google Sheets as the data store.  

⚠️ This repo currently contains **only the backend**.  
Frontend components (dashboard/visualization) will be added in the future.  

---

## 📂 Project Structure

### BE/ – Backend Apps Script Files

- **sync_fe_fixed.gs** → Syncs the `fe_fixed` sheet with previous trading day’s close prices for tickers.  
- **sync_fe_live.gs** → Updates the `fe_live` sheet with live prices from external APIs.  
- **sync_posts.gs** → Pulls Reddit posts, extracts tickers, and writes to the `posts` sheet.  
- **sync_symbols.gs** → Maintains the `symbols` reference sheet (ticker ↔ company mapping).  
- **sync_ticker_cache.gs** → Manages cached ticker lookups in the `ticker_cache` sheet.  
- **utils_api_for_fe.gs** → Provides API methods for fetching live/close prices used in `fe_live` & `fe_fixed`.  
- **utils_direction_classifier.gs** → Classifies post sentiment/direction (bullish/bearish/neutral).  
- **utils_ext_apis.gs** → Handles communication with external APIs (finance, Reddit, etc.).  
- **utils_sheets.gs** → Google Sheets utility functions (read/write ranges, find headers, etc.).  
- **utils_ticker_extractor.gs** → Extracts ticker symbols from Reddit post titles/content.  
- **utils_triggers.gs** → Manages installable GAS triggers (time-based, onEdit, etc.).  

---

## 📊 Google Sheets Database

This project uses a Google Sheet as the primary datastore.  
For reference, `WSB_DD_DB.xlsx` mirrors the structure:

- **fe_live** → real-time ticker prices (for frontend display).  
- **fe_fixed** → snapshot of previous day’s close prices (used in analysis).  
- **posts** → raw Reddit posts with extracted tickers.  
- **symbols** → reference table of tickers ↔ company names.  
- **ticker_cache** → local cache to reduce redundant API calls.  

---

## ⚙️ How It Works

1. **Collect posts** → `sync_posts.gs` fetches Reddit posts and extracts tickers.  
2. **Normalize symbols** → `sync_symbols.gs` ensures correct ticker-company mapping.  
3. **Cache prices** → `sync_ticker_cache.gs` avoids repeated API lookups.  
4. **Update market data**  
   - `sync_fe_live.gs` → live stock prices  
   - `sync_fe_fixed.gs` → previous day close prices  
5. **Store in Sheets** → Data lands in `WSB_DD_DB` Google Sheet for analysis.  

---

## 🚀 Future Roadmap

### Backend
- Improve ticker extraction (handle false positives, multi-ticker posts).  
- Expand sentiment analysis in `utils_direction_classifier.gs`.  
- Add more external data sources (options flow, short interest, etc.).  

### Frontend *(planned)*
- Web dashboard to visualize trending tickers, sentiment, and price movement.  
- Search & filter for posts by ticker or time range.  
- Interactive charts with live updates.  

---

## 🛠️ Development Notes

- This project runs entirely on **Google Apps Script**.  
- Logging uses `console.log()` (check via Apps Script built-in logger).  
- Trigger management (`utils_triggers.gs`) allows scheduling periodic sync jobs.  
- Sheets act as both **database** and **staging layer** for the frontend.  

---

## 📌 Next Steps

- [ ] Write deployment instructions for frontend + backend integration.  
