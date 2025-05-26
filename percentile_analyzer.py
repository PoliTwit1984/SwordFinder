import pandas as pd
import numpy as np
from typing import Dict, Optional, List

class PercentileAnalyzer:
    """
    Analyzes pitch percentiles based on the full 2025 Statcast dataset
    """
    
    def __init__(self, csv_path: str = 'attached_assets/statcast_2025.csv'):
        """
        Initialize with the Statcast dataset
        
        Args:
            csv_path (str): Path to the Statcast CSV file
        """
        print("Loading Statcast dataset...")
        self.data = pd.read_csv(csv_path)
        print(f"Loaded {len(self.data):,} pitch records")
        
        # Cache percentile data for faster lookups
        self._percentile_cache = {}
        self._precompute_percentiles()
    
    def _precompute_percentiles(self):
        """
        Precompute percentile distributions for each pitch type and metric
        This makes individual lookups much faster
        """
        print("Precomputing percentile distributions...")
        
        # Key metrics to analyze
        metrics = [
            'release_speed',
            'release_spin_rate', 
            'pfx_x',  # horizontal movement
            'pfx_z',  # vertical movement
            'release_extension',
            'effective_speed'
        ]
        
        # Group by pitch type
        for pitch_type in self.data['pitch_type'].dropna().unique():
            pitch_data = self.data[self.data['pitch_type'] == pitch_type]
            self._percentile_cache[pitch_type] = {}
            
            for metric in metrics:
                if metric in pitch_data.columns:
                    # Remove null values
                    values = pitch_data[metric].dropna()
                    if len(values) > 0:
                        self._percentile_cache[pitch_type][metric] = {
                            'values': values.sort_values(),
                            'count': len(values)
                        }
        
        print("Percentile distributions ready!")
    
    def get_pitch_percentile(self, pitch_type: str, metric: str, value: float) -> Optional[float]:
        """
        Get the percentile ranking for a specific pitch metric
        
        Args:
            pitch_type (str): Pitch type (e.g., 'FF', 'SL', 'CU')
            metric (str): Metric name (e.g., 'release_speed', 'release_spin_rate')
            value (float): The value to compare
            
        Returns:
            float: Percentile (0-100), or None if data not available
        """
        if pitch_type not in self._percentile_cache:
            return None
            
        if metric not in self._percentile_cache[pitch_type]:
            return None
            
        cached_data = self._percentile_cache[pitch_type][metric]
        values = cached_data['values']
        
        # Calculate percentile using numpy
        percentile = (values < value).sum() / len(values) * 100
        
        return round(percentile, 1)
    
    def analyze_pitch_percentiles(self, pitch_data: Dict) -> Dict:
        """
        Analyze all available percentiles for a single pitch
        
        Args:
            pitch_data (dict): Dictionary containing pitch information
            
        Returns:
            dict: Percentile analysis results
        """
        pitch_type = pitch_data.get('pitch_type')
        if not pitch_type:
            return {}
        
        analysis = {
            'pitch_type': pitch_type,
            'pitch_name': pitch_data.get('pitch_name', ''),
            'percentiles': {}
        }
        
        # Analyze key metrics
        metrics_to_check = {
            'release_speed': 'Velocity',
            'release_spin_rate': 'Spin Rate', 
            'pfx_x': 'Horizontal Movement',
            'pfx_z': 'Vertical Movement',
            'release_extension': 'Extension',
            'effective_speed': 'Perceived Velocity'
        }
        
        for metric, display_name in metrics_to_check.items():
            if metric in pitch_data and pitch_data[metric] is not None:
                percentile = self.get_pitch_percentile(
                    pitch_type, 
                    metric, 
                    float(pitch_data[metric])
                )
                
                if percentile is not None:
                    analysis['percentiles'][display_name] = {
                        'value': pitch_data[metric],
                        'percentile': percentile,
                        'metric': metric
                    }
        
        return analysis
    
    def get_pitch_type_stats(self, pitch_type: str) -> Dict:
        """
        Get summary statistics for a specific pitch type
        
        Args:
            pitch_type (str): Pitch type to analyze
            
        Returns:
            dict: Summary stats including count, avg velocity, etc.
        """
        if pitch_type not in self._percentile_cache:
            return {}
        
        pitch_data = self.data[self.data['pitch_type'] == pitch_type]
        
        stats = {
            'pitch_type': pitch_type,
            'total_pitches': len(pitch_data),
            'unique_pitchers': pitch_data['pitcher'].nunique() if 'pitcher' in pitch_data.columns else 0
        }
        
        # Add metric summaries
        if 'release_speed' in self._percentile_cache[pitch_type]:
            speeds = self._percentile_cache[pitch_type]['release_speed']['values']
            stats['velocity'] = {
                'avg': round(speeds.mean(), 1),
                'min': round(speeds.min(), 1),
                'max': round(speeds.max(), 1),
                'median': round(speeds.median(), 1)
            }
        
        if 'release_spin_rate' in self._percentile_cache[pitch_type]:
            spins = self._percentile_cache[pitch_type]['release_spin_rate']['values']
            stats['spin_rate'] = {
                'avg': round(spins.mean(), 0),
                'min': round(spins.min(), 0),
                'max': round(spins.max(), 0),
                'median': round(spins.median(), 0)
            }
        
        return stats
    
    def compare_sword_swing_percentiles(self, sword_swings: List[Dict]) -> List[Dict]:
        """
        Add percentile analysis to sword swing results
        
        Args:
            sword_swings (list): List of sword swing dictionaries
            
        Returns:
            list: Enhanced sword swings with percentile data
        """
        enhanced_swings = []
        
        for swing in sword_swings:
            enhanced_swing = swing.copy()
            
            # Add percentile analysis
            percentile_analysis = self.analyze_pitch_percentiles(swing)
            enhanced_swing['percentile_analysis'] = percentile_analysis
            
            # Add some key percentile highlights
            if percentile_analysis.get('percentiles'):
                highlights = []
                
                for metric_name, data in percentile_analysis['percentiles'].items():
                    percentile = data['percentile']
                    if percentile >= 95:
                        highlights.append(f"Elite {metric_name} ({percentile}th percentile)")
                    elif percentile >= 90:
                        highlights.append(f"Excellent {metric_name} ({percentile}th percentile)")
                    elif percentile <= 10:
                        highlights.append(f"Poor {metric_name} ({percentile}th percentile)")
                
                enhanced_swing['percentile_highlights'] = highlights
            
            enhanced_swings.append(enhanced_swing)
        
        return enhanced_swings

    def get_percentile_description(self, percentile: float) -> str:
        """
        Get a descriptive label for a percentile
        
        Args:
            percentile (float): Percentile value (0-100)
            
        Returns:
            str: Description like "Elite", "Above Average", etc.
        """
        if percentile >= 95:
            return "Elite"
        elif percentile >= 90:
            return "Excellent" 
        elif percentile >= 75:
            return "Above Average"
        elif percentile >= 60:
            return "Good"
        elif percentile >= 40:
            return "Average"
        elif percentile >= 25:
            return "Below Average"
        elif percentile >= 10:
            return "Poor"
        else:
            return "Very Poor"