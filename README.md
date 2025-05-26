# SwordFinder ⚔️

A sophisticated Flask API that leverages MLB Statcast data to analyze and score baseball "sword swings" using advanced computational techniques and custom algorithms.

## Overview

SwordFinder identifies and analyzes the most impressive baseball swings that result in swinging strikes - these are called "sword swings" because the batter "swings and misses" at difficult pitches. The system uses authentic MLB Statcast data to find, score, and rank the top sword swings with comprehensive analysis including percentile rankings, expert AI commentary, and video highlights.

## Key Features

- **Authentic MLB Data**: Uses 226,833+ real Statcast records from official MLB sources
- **Advanced Filtering**: Identifies sword swings using multiple swing mechanics criteria
- **Custom Scoring Algorithm**: Proprietary weighted formula scoring swings 50-100 points
- **Percentile Analysis**: Compares each swing against entire season's data
- **AI Expert Commentary**: Claude-powered analysis of what makes each swing special
- **Video Integration**: Direct links to Baseball Savant video highlights
- **Database Caching**: PostgreSQL storage for fast response times
- **Live Patch System**: Browser-based tool to update missing data

## Technical Architecture

### Backend Stack
- **Python 3.11** - Core runtime
- **Flask** - Web framework and API endpoints
- **PostgreSQL** - Primary database with 118+ Statcast fields
- **SQLAlchemy** - Database ORM and query optimization
- **pybaseball** - Official MLB data source integration
- **pandas** - Data processing and analysis
- **NumPy** - Statistical calculations and percentile ranking

### Data Sources
- **MLB Statcast**: Official pitch-by-pitch data via pybaseball
- **Baseball Savant**: Video highlights and play details
- **MLB Stats API**: Game data and player information

### Core Components

#### 1. Sword Swing Detection Engine
**File**: `simple_db_swordfinder.py`

**Filtering Criteria**:
- **Event Type**: `swinging_strike` or `swinging_strike_blocked`
- **Bat Speed**: Less than 60 mph (slower swings indicate difficulty)
- **Intercept Y**: Greater than 14 inches (swing path intersection)
- **Swing Path Tilt**: Greater than 30 degrees (steep swing angle)

**Scoring Formula**:
```python
sword_score = (
    0.35 * (60 - bat_speed) / 60 +      # Bat speed penalty (35% weight)
    0.25 * swing_path_tilt / 60 +       # Swing tilt bonus (25% weight)  
    0.25 * intercept_y / 50 +           # Intercept bonus (25% weight)
    0.15 * zone_penalty                 # Strike zone penalty (15% weight)
) * 100

# Normalized to 50-100 scale
final_score = 50 + (sword_score * 50)
```

#### 2. Database Schema
**File**: `models_complete.py`

**Primary Table**: `statcast_pitches`
- 118+ fields covering all MLB Statcast data
- Optimized indexes on game_date, player_name, pitch_type
- Complete pitch details: velocity, spin rate, location, teams

**Analysis Table**: `sword_swings`
- Sword score calculations and rankings
- Percentile analysis results
- AI expert commentary
- Video URLs and local MP4 storage
- Cached results for performance

**Tracking Table**: `daily_results`
- Processing status by date
- Performance metrics and completion tracking

#### 3. Percentile Analysis Engine
**File**: `percentile_analyzer.py`

Compares each sword swing against season-wide data:
- **Bat Speed Percentile**: How slow compared to all swings
- **Swing Tilt Percentile**: How steep the swing angle
- **Velocity Percentile**: How fast the pitch was
- **Spin Rate Percentile**: How much the ball spun
- **Location Percentile**: Where in strike zone

#### 4. Expert AI Analysis
**Integration**: Claude Sonnet 4.0 via Anthropic API

Generates detailed commentary explaining:
- What made the swing technically difficult
- Pitch characteristics that created the challenge
- Swing mechanics analysis
- Context within the at-bat situation

## API Endpoints

### Core API

#### `POST /swords`
Find and analyze sword swings for a specific date.

**Request**:
```json
{
  "date": "2025-05-24"
}
```

**Response**:
```json
{
  "success": true,
  "data": [
    {
      "player_name": "Martinez, J.D.",
      "pitcher_name": "Clase, Emmanuel", 
      "teams": "BOS @ CLE",
      "pitch_type": "SL",
      "pitch_name": "Slider",
      "velocity": 88.7,
      "spin_rate": 2549,
      "location": "(-0.82, 2.34)",
      "bat_speed": 45.8,
      "swing_path_tilt": 42.3,
      "attack_angle": 18.4,
      "intercept_y": 23.2,
      "sword_score": 96.4,
      "percentile_analysis": {
        "bat_speed_percentile": 15.2,
        "velocity_percentile": 87.3,
        "elite_metrics": ["Low Bat Speed", "High Velocity", "Steep Tilt"]
      },
      "expert_analysis": "This sword swing showcases exceptional difficulty...",
      "play_id": "0c0bea6e-cfce-326c-b224-4840f872c7c8",
      "game_pk": 777788,
      "video_url": "https://baseballsavant.mlb.com/sporty-videos?playId=...",
      "local_mp4": "/static/videos/0c0bea6e-cfce-326c-b224-4840f872c7c8.mp4"
    }
  ],
  "count": 5,
  "date": "2025-05-24",
  "processing_time": 1.23
}
```

#### `POST /playid`
Look up Baseball Savant playId for video access.

**Request**:
```json
{
  "game_pk": 777788,
  "pitch_number": 4,
  "inning": 2
}
```

**Response**:
```json
{
  "success": true,
  "playId": "0c0bea6e-cfce-326c-b224-4840f872c7c8",
  "video_url": "https://baseballsavant.mlb.com/sporty-videos?playId=...&videoType=AWAY"
}
```

#### `GET /health`
System health check and status.

### Admin Tools

#### `GET /patch-monitor`
Browser-based database patch control center for fixing missing data.

#### `POST /start-patch`
Initiate background process to update database with missing MLB data.

## Installation & Setup

### Prerequisites
- Python 3.11+
- PostgreSQL database
- MLB data access (via pybaseball)

### Environment Variables
```bash
DATABASE_URL=postgresql://username:password@host:port/database
SESSION_SECRET=your-flask-session-secret
ANTHROPIC_API_KEY=your-anthropic-api-key  # Optional for AI analysis
```

### Installation Steps

1. **Clone Repository**
```bash
git clone <repository-url>
cd swordfinder
```

2. **Install Dependencies**
```bash
pip install -r requirements.txt
```

3. **Database Setup**
```bash
# PostgreSQL will be created automatically
# Tables are created on first run
```

4. **Import MLB Data**
```bash
# Option 1: Use provided complete dataset
python import_complete_authentic_data.py

# Option 2: Pull fresh data from pybaseball
python fresh_data_pull.py
```

5. **Start Application**
```bash
# Production
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload --timeout 120 main:app

# Development
python app.py
```

## Data Import Process

### Complete Dataset Import
The system includes scripts to import authentic MLB Statcast data:

1. **Complete Import**: `import_complete_authentic_data.py`
   - Imports all 118 Statcast fields
   - Handles 226,833+ pitch records
   - Includes data validation and error handling

2. **Chunked Import**: `chunked_import.py`
   - Processes large datasets in manageable chunks
   - Prevents memory overflow and timeouts
   - Resume capability for interrupted imports

3. **Fresh Data Pull**: `fresh_data_pull.py`
   - Pulls latest data from pybaseball
   - Updates existing records with new information
   - Maintains data freshness

### Database Patch System
**File**: `patch_postgres_from_pybaseball.py`

Browser-based tool for fixing incomplete imports:
- Live progress monitoring
- Batch processing (500 records/batch)
- Smart field matching and updating
- Error handling and recovery
- Real-time statistics and logging

## Performance Optimization

### Database Indexes
```sql
-- Optimized for common queries
CREATE INDEX idx_game_date ON statcast_pitches(game_date);
CREATE INDEX idx_player_name ON statcast_pitches(player_name);
CREATE INDEX idx_pitch_type ON statcast_pitches(pitch_type);
CREATE INDEX idx_sword_criteria ON statcast_pitches(bat_speed, intercept_ball_minus_batter_pos_y_inches, swing_path_tilt);
```

### Caching Strategy
- **Daily Results**: Cached by date to avoid reprocessing
- **Percentile Data**: Pre-calculated season statistics
- **Video URLs**: Stored locally to avoid repeated API calls
- **Database Connections**: Connection pooling with 300s recycle

### Query Optimization
- Filtered queries using indexed columns
- Batch processing for large operations
- Efficient JOIN operations for related data
- LIMIT clauses for paginated results

## Video Integration

### Baseball Savant Integration
- Direct links to official MLB video highlights
- Multiple video types: AWAY, HOME, COMPOSITE
- UUID-based playId resolution
- Fallback handling for missing videos

### Local MP4 Storage
- Downloads videos locally to solve CORS issues
- Enables direct video embedding
- Automatic file management and cleanup
- Efficient bandwidth usage

## Monitoring & Logging

### Application Logging
```python
# Configured logging levels
logging.basicConfig(level=logging.DEBUG)

# Key log points:
- API request/response times
- Database query performance  
- Sword swing detection results
- Video download status
- Error conditions and recovery
```

### Health Monitoring
- `/health` endpoint for system status
- Database connection verification
- Data freshness checks
- Performance metrics tracking

## Development Workflow

### Adding New Features

1. **Database Changes**: Update `models_complete.py`
2. **API Logic**: Modify endpoint handlers in `app.py`
3. **Analysis Engine**: Update filtering/scoring in `simple_db_swordfinder.py`
4. **Testing**: Use built-in diagnostic tools

### Debugging Tools

1. **Field Inspector**: `inspect_pybaseball_columns.py`
   - Verify data source coverage
   - Check field availability
   - Sample data preview

2. **Percentile Tester**: `test_percentiles.py`
   - Validate percentile calculations
   - Test ranking algorithms

3. **Database Diagnostics**: SQL queries for data verification
   ```sql
   -- Check data completeness
   SELECT COUNT(*) as total,
          COUNT(home_team) as has_teams,
          COUNT(release_speed) as has_velocity
   FROM statcast_pitches;
   ```

## Deployment

### Production Configuration
```python
# Gunicorn settings
--bind 0.0.0.0:5000
--reuse-port
--reload
--timeout 120
--workers 4
```

### Environment Setup
- PostgreSQL with connection pooling
- SSL/TLS termination
- Static file serving for videos
- Log aggregation and monitoring

### Scaling Considerations
- Database read replicas for analytics
- Redis caching for frequent queries
- CDN for video content delivery
- Horizontal scaling with load balancers

## Troubleshooting

### Common Issues

**1. Missing Data in Database**
- Use `/patch-monitor` to fix incomplete imports
- Check pybaseball connectivity
- Verify field mappings

**2. Slow Query Performance**
- Check database indexes
- Analyze query execution plans
- Consider query optimization

**3. Video Loading Issues**
- Verify playId resolution
- Check Baseball Savant connectivity
- Use local MP4 fallbacks

**4. Import Failures**
- Use chunked import for large datasets
- Check memory and disk space
- Verify database connection limits

### Debug Commands
```bash
# Check database status
python -c "from models_complete import *; check_database_status()"

# Test pybaseball connection
python inspect_pybaseball_columns.py

# Verify sword swing detection
python simple_db_swordfinder.py
```

## Contributing

### Code Style
- Follow PEP 8 Python style guidelines
- Use type hints where appropriate
- Include comprehensive docstrings
- Add unit tests for new features

### Pull Request Process
1. Fork the repository
2. Create feature branch
3. Add tests for new functionality
4. Update documentation
5. Submit pull request with detailed description

## License

This project uses authentic MLB data through official APIs and adheres to all terms of service for data usage.

## Support

For technical issues or questions:
1. Check troubleshooting section
2. Review application logs
3. Use built-in diagnostic tools
4. Consult API documentation

---

**SwordFinder** - Cutting through baseball data to find the sharpest swings ⚔️