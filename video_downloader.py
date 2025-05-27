#!/usr/bin/env python3
"""
Video downloader for SwordFinder - downloads MP4 clips from Baseball Savant
and stores them locally to avoid CORS issues
"""

import os
import logging
import requests
from datetime import datetime
from models_complete import get_db, SwordSwing, StatcastPitch
from sqlalchemy import and_
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_video_url_from_sporty_page(play_id, max_retries=3):
    """
    Extract the direct MP4 download URL from a Baseball Savant sporty-videos page
    
    Args:
        play_id (str): The UUID playId for the pitch
        max_retries (int): Number of retry attempts
        
    Returns:
        str: Direct MP4 URL if found, None otherwise
    """
    attempt = 0
    while attempt < max_retries:
        try:
            page_url = f"https://baseballsavant.mlb.com/sporty-videos?playId={play_id}"
            logger.debug(f"Extracting MP4 from: {page_url} (attempt {attempt + 1})")
            
            response = requests.get(page_url, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            video_container = soup.find('div', class_='video-box')
            
            if video_container:
                video_tag = video_container.find('video')
                if video_tag:
                    source_tag = video_tag.find('source', {'type': 'video/mp4'})
                    if source_tag and source_tag.get('src'):
                        mp4_url = source_tag.get('src')
                        logger.info(f"Found MP4 URL for playId {play_id}: {mp4_url}")
                        return mp4_url
            
            logger.warning(f"No video URL found for playId {play_id} on attempt {attempt + 1}")
            attempt += 1
            if attempt < max_retries:
                import time
                time.sleep(2)  # Wait before retry
                
        except Exception as e:
            logger.warning(f"Error extracting MP4 from sporty page for playId {play_id} on attempt {attempt + 1}: {str(e)}")
            attempt += 1
            if attempt < max_retries:
                import time
                time.sleep(2)
    
    return None

def download_sword_clip(play_id, download_url, save_dir="static/videos"):
    """
    Download an MP4 video clip for a sword swing
    
    Args:
        play_id: The play ID for the video
        download_url: Direct MP4 URL from Baseball Savant
        save_dir: Directory to save videos (relative to app root)
        
    Returns:
        dict with path and file_size, or None if download failed
    """
    if not download_url:
        logger.warning(f"No download URL provided for play_id {play_id}")
        return None
        
    filename = f"{play_id}.mp4"
    path = os.path.join(save_dir, filename)
    
    # Create directory if it doesn't exist
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        logger.info(f"Created video directory: {save_dir}")
    
    # Skip if file already exists
    if os.path.exists(path):
        file_size = os.path.getsize(path)
        logger.info(f"Video already exists: {path} ({file_size} bytes)")
        return {
            "path": f"/{path}",  # Web-accessible path
            "file_size": file_size
        }
    
    try:
        logger.info(f"Downloading video for {play_id} from {download_url}")
        
        # Download with streaming to handle large files
        response = requests.get(download_url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Write to file
        total_size = 0
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
        
        logger.info(f"Successfully downloaded {filename} ({total_size} bytes)")
        
        return {
            "path": f"/{path}",  # Web-accessible path
            "file_size": total_size
        }
        
    except Exception as e:
        logger.error(f"Failed to download video for {play_id}: {str(e)}")
        # Clean up partial download
        if os.path.exists(path):
            os.remove(path)
        return None

def process_sword_videos(date_str=None, limit=None):
    """
    Download videos for sword swings that don't have local copies
    
    Args:
        date_str: Optional date to filter by (YYYY-MM-DD)
        limit: Optional limit on number of videos to download
        
    Returns:
        dict with processing results
    """
    results = {
        "processed": 0,
        "downloaded": 0,
        "failed": 0,
        "skipped": 0,
        "errors": []
    }
    
    with get_db() as db:
        # Query for sword swings without local videos
        query = db.query(SwordSwing, StatcastPitch).join(
            StatcastPitch, SwordSwing.pitch_id == StatcastPitch.id
        ).filter(
            SwordSwing.mp4_downloaded == False
        )
        
        # Filter by date if provided
        if date_str:
            query = query.filter(StatcastPitch.game_date == date_str)
        
        # Apply limit if provided
        if limit:
            query = query.limit(limit)
        
        sword_swings = query.all()
        logger.info(f"Found {len(sword_swings)} sword swings without local videos")
        
        for sword_swing, pitch in sword_swings:
            results["processed"] += 1
            
            # Use play_id from pitch record
            play_id = pitch.sv_id
            if not play_id:
                logger.warning(f"No play_id for sword swing {sword_swing.id}")
                results["skipped"] += 1
                continue
            
            # Get the MP4 download URL
            try:
                download_url = get_video_url_from_sporty_page(play_id)
                if not download_url:
                    logger.warning(f"Could not extract MP4 URL for play_id {play_id}")
                    results["failed"] += 1
                    results["errors"].append(f"No MP4 URL found for {play_id}")
                    continue
                
                # Download the video
                download_result = download_sword_clip(play_id, download_url)
                
                if download_result:
                    # Update database with local path
                    sword_swing.local_mp4_path = download_result["path"]
                    sword_swing.mp4_file_size = download_result["file_size"]
                    sword_swing.mp4_downloaded = True
                    sword_swing.download_url = download_url
                    sword_swing.updated_at = datetime.utcnow()
                    
                    db.commit()
                    logger.info(f"Updated database for play_id {play_id}")
                    results["downloaded"] += 1
                else:
                    results["failed"] += 1
                    results["errors"].append(f"Download failed for {play_id}")
                    
            except Exception as e:
                logger.error(f"Error processing play_id {play_id}: {str(e)}")
                results["failed"] += 1
                results["errors"].append(f"Error for {play_id}: {str(e)}")
                db.rollback()
    
    logger.info(f"Video processing complete: {results}")
    return results

def get_download_stats():
    """Get statistics about video downloads"""
    with get_db() as db:
        total_swords = db.query(SwordSwing).count()
        downloaded = db.query(SwordSwing).filter(SwordSwing.mp4_downloaded == True).count()
        total_size = db.query(db.func.sum(SwordSwing.mp4_file_size)).filter(
            SwordSwing.mp4_downloaded == True
        ).scalar() or 0
        
        return {
            "total_sword_swings": total_swords,
            "videos_downloaded": downloaded,
            "videos_pending": total_swords - downloaded,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2) if total_size else 0
        }

if __name__ == "__main__":
    # Test the downloader
    import sys
    
    if len(sys.argv) > 1:
        # Process specific date
        date = sys.argv[1]
        print(f"Processing videos for date: {date}")
        results = process_sword_videos(date_str=date)
    else:
        # Process all pending videos (limit to 10 for testing)
        print("Processing up to 10 pending videos...")
        results = process_sword_videos(limit=10)
    
    print(f"\nResults: {results}")
    print(f"\nStats: {get_download_stats()}")
