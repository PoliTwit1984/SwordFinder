#!/usr/bin/env python3

from percentile_analyzer import PercentileAnalyzer
import json

def test_percentile_analysis():
    """
    Test the percentile analysis with some example sword swing data
    """
    # Initialize the analyzer
    analyzer = PercentileAnalyzer()
    
    # Example sword swing data (based on your recent results)
    example_swing = {
        'pitch_type': 'CU',  # Curveball
        'pitch_name': 'Curveball',
        'release_speed': 79.6,
        'release_spin_rate': 2408,
        'pfx_x': 0.19,
        'pfx_z': -0.75,
        'release_extension': 7.1,
        'effective_speed': 86.8,
        'batter_name': 'Justyn-Henry Malloy',
        'pitcher_name': 'Herrin, Tim'
    }
    
    print("🎯 ANALYZING SWORD SWING PITCH PERCENTILES")
    print("=" * 50)
    
    # Analyze this pitch
    analysis = analyzer.analyze_pitch_percentiles(example_swing)
    
    print(f"Pitcher: {example_swing['pitcher_name']}")
    print(f"Batter: {example_swing['batter_name']}")
    print(f"Pitch: {analysis['pitch_name']} ({analysis['pitch_type']})")
    print()
    
    if analysis['percentiles']:
        print("📊 PERCENTILE BREAKDOWN:")
        print("-" * 30)
        
        for metric_name, data in analysis['percentiles'].items():
            percentile = data['percentile']
            value = data['value']
            description = analyzer.get_percentile_description(percentile)
            
            print(f"{metric_name:20}: {value:8} ({percentile:5.1f}th percentile - {description})")
        
        print()
        
        # Show pitch type stats for context
        print("📈 CURVEBALL LEAGUE CONTEXT:")
        print("-" * 30)
        stats = analyzer.get_pitch_type_stats('CU')
        
        if 'velocity' in stats:
            vel_stats = stats['velocity']
            print(f"Velocity Range: {vel_stats['min']}-{vel_stats['max']} mph (avg: {vel_stats['avg']})")
        
        if 'spin_rate' in stats:
            spin_stats = stats['spin_rate']
            print(f"Spin Rate Range: {spin_stats['min']:,.0f}-{spin_stats['max']:,.0f} rpm (avg: {spin_stats['avg']:,.0f})")
        
        print(f"Total Curveballs: {stats['total_pitches']:,}")
        print(f"Pitchers Who Throw It: {stats['unique_pitchers']}")
    
    print("\n" + "=" * 50)
    print("✨ This curveball that fooled Malloy was...")
    
    # Generate insights
    insights = []
    for metric_name, data in analysis['percentiles'].items():
        percentile = data['percentile']
        if percentile >= 90:
            insights.append(f"• Elite {metric_name.lower()} ({percentile:.1f}th percentile)")
        elif percentile <= 10:
            insights.append(f"• Poor {metric_name.lower()} ({percentile:.1f}th percentile)")
    
    if insights:
        for insight in insights:
            print(insight)
    else:
        print("• A fairly typical curveball by 2025 standards")

if __name__ == "__main__":
    test_percentile_analysis()