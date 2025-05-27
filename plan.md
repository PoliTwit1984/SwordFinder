# SwordFinder Development Plan - May 26, 2025

This document summarizes the development work undertaken and the current state of the SwordFinder project.

## Phase 1: Initial Fixes & Enhancements (Completed)

*   **1. Data Accuracy & Core Logic (Completed)**
    *   **Field Mapping Corrections:** Addressed initial misalignments in `simple_db_swordfinder.py` for `pitcher_name`, `pitch_name`, `batter_id`, `pitcher_id`.
    *   **Batter Name Lookup:** Implemented dynamic fetching of batter names via MLB Stats API in `app.py`.
    *   **`launch_angle`/`launch_speed`:** Confirmed `null` values are expected for swinging strikes.
    *   **Video URL Construction:** Corrected `video_url` (Savant page link) logic in `app.py`.

*   **2. Database Setup & Schema (Completed)**
    *   **PostgreSQL Role:** Created `swordfinder` role with login and password.
    *   **Database Creation:** Created `swordfinder_db` database.
    *   **Permissions:** Granted necessary privileges to `swordfinder` role on the database and public schema.
    *   **Schema Update (`raw_sword_metric`):**
        *   Added `raw_sword_metric` (FLOAT) column to `SwordSwing` model in `models_complete.py`.
        *   Used `temp_update_schema.py` to apply this change to the `sword_swings` table.

*   **3. Data Population (Completed)**
    *   **Database Connection Verification:** Used `diagnose_postgres_data.py` to confirm the `swordfinder_db` (for user `swordfinder`) was initially empty.
    *   **Data Import:**
        *   Refactored `import_data.py` to use `models_complete.py` schema and correct CSV file (`2025_full_statcast_05242025.csv`).
        *   Successfully imported 226,832 pitch records into `statcast_pitches` table.

*   **4. Sword Scoring & Detection Refinement (Completed)**
    *   **True Sword Definition:** Refined SQL query in `simple_db_swordfinder.py` to select only the *final pitch* of an at-bat that *resulted in a strikeout* and was a *swinging strike*. This uses a CTE with `DISTINCT ON (game_pk, at_bat_number)` and `ORDER BY ... pitch_number DESC`.
    *   **Dynamic Zone Penalty:** Implemented `_calculate_dynamic_zone_penalty` in `simple_db_swordfinder.py`.
    *   **Raw Sword Metric:**
        *   Defined `raw_sword_metric` as the unscaled sum of weighted normalized components (bat speed, tilt, intercept, zone penalty).
        *   `simple_db_swordfinder.py` now calculates this for all candidates.
    *   **Scaled Scores:**
        *   `sword_score` (Universal): `raw_sword_metric * 50 + 50`.
        *   `daily_normalized_score`: Min-max normalized against the day's raw metrics, then scaled 50-100.
    *   **Python-Side Sorting:** Candidates are now sorted in Python by `raw_sword_metric` before selecting the top 5 for the daily API.
    *   **Data Persistence:** `app.py` now correctly stores `raw_sword_metric` and the scaled `sword_score` in the `sword_swings` table.

*   **5. All-Time Leaderboard Capability (Completed)**
    *   **Comprehensive Score Population:** Created `populate_all_sword_swing_scores.py` to iterate through all dates in `statcast_pitches`, calculate scores, and create/update `SwordSwing` records. This ensures all historical data is processed.
    *   **Video Metadata for Top Swords:** Created `process_top_swords_videos.py` to fetch/construct video URLs, download MP4s, and update `sword_swings` records for the current top 5 all-time swords.

*   **6. Frontend Enhancements (Completed)**
    *   **New API Endpoint:** Added `/api/top-swords-2025` to `app.py` to serve data for the all-time 2025 leaderboard.
    *   **Dashboard on Main Page:** Updated `templates/home.html` to include a "Top 5 Swords of 2025 (All-Time)" dashboard that fetches data from the new API endpoint and displays swords with embedded videos.

*   **7. Documentation (Completed)**
    *   `README.md` updated to reflect current architecture, scoring logic, new features, and API endpoints.
    *   This `plan.md` updated to summarize completed work.

## Current Project Status

*   The SwordFinder application is fully functional.
*   It accurately identifies true strikeout-ending sword swings.
*   It employs a sophisticated scoring mechanism including a dynamic zone penalty and provides multiple score types (`raw_sword_metric`, `sword_score`, `daily_normalized_score`).
*   All relevant data, including scores and video metadata, is persisted to the PostgreSQL database.
*   The main web page displays both daily top swords and an all-time top 5 leaderboard for 2025.
*   Scripts are in place for initial data import, comprehensive historical score population, and processing video information for top swords.

## Potential Next Steps (Phase 2 - Future Considerations)

*   **Video URL Population for All Swords:** Enhance `populate_all_sword_swing_scores.py` or `process_top_swords_videos.py` to systematically populate `video_url` (Savant page link) for *all* `SwordSwing` records, not just the current top 5. This would make the "Top 5 All-Time" list in `test_query.py` (and any future leaderboard display) show video links for all entries immediately.
*   **Refine MLB Stats API Lookup:** The `play_id` lookup in `process_top_swords_videos.py` (and potentially `app.py` if issues arise) could be made more robust, perhaps by centralizing the complex lookup logic from `app.py`'s `/playid` endpoint into a shared utility function.
*   **Performance Optimization:** For larger datasets or higher traffic, consider database indexing reviews, query optimization, and caching strategies (e.g., Redis for API responses or frequently accessed data).
*   **Error Handling & Logging:** Continue to refine error handling and logging throughout the application for easier debugging and maintenance.
*   **User Interface Enhancements:** Further develop the frontend for more interactive data exploration, filtering, and display options.
*   **Automated Testing:** Implement a more comprehensive suite of unit and integration tests.
