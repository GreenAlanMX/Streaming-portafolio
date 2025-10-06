#!/usr/bin/env python3
"""
Generate Enhanced Performance Plots for ETL Pipeline
===================================================
Creates professional, detailed performance visualization with corrected memory data.
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Set style for professional plots
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def create_enhanced_performance_plots():
    """Create enhanced performance visualization plots"""
    
    # Load benchmark results
    results_file = Path("benchmarking/results/final_benchmark_results.csv")
    if not results_file.exists():
        print("Error: Benchmark results file not found")
        return
    
    df = pd.read_csv(results_file)
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    fig.suptitle('ETL Pipeline Performance Analysis - Enhanced Visualization', 
                 fontsize=24, fontweight='bold', y=0.98)
    
    # Define colors for consistency
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
    
    # 1. Execution Time Analysis (Top Left)
    ax1 = plt.subplot(3, 3, 1)
    bars1 = ax1.bar(df['data_size'], df['execution_time'], color=colors[0], alpha=0.8, edgecolor='black', linewidth=1)
    ax1.set_title('Execution Time by Dataset Size', fontsize=14, fontweight='bold', pad=20)
    ax1.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars1, df['execution_time']):
        ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                f'{value:.2f}s', ha='center', va='bottom', fontweight='bold')
    
    # 2. Throughput Analysis (Top Center)
    ax2 = plt.subplot(3, 3, 2)
    bars2 = ax2.bar(df['data_size'], df['throughput'], color=colors[1], alpha=0.8, edgecolor='black', linewidth=1)
    ax2.set_title('Throughput Performance', fontsize=14, fontweight='bold', pad=20)
    ax2.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Throughput (records/second)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    # Add value labels on bars
    for bar, value in zip(bars2, df['throughput']):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 20, 
                f'{value:.0f}', ha='center', va='bottom', fontweight='bold')
    
    # 3. Memory Usage Analysis (Top Right)
    ax3 = plt.subplot(3, 3, 3)
    x = np.arange(len(df))
    width = 0.35
    
    bars3a = ax3.bar(x - width/2, df['memory_peak_mb'], width, label='Peak Memory', 
                     color=colors[2], alpha=0.8, edgecolor='black', linewidth=1)
    bars3b = ax3.bar(x + width/2, df['memory_avg_mb'], width, label='Average Memory', 
                     color=colors[3], alpha=0.8, edgecolor='black', linewidth=1)
    
    ax3.set_title('Memory Usage Analysis (Corrected)', fontsize=14, fontweight='bold', pad=20)
    ax3.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Memory Usage (MB)', fontsize=12, fontweight='bold')
    ax3.set_xticks(x)
    ax3.set_xticklabels(df['data_size'])
    ax3.legend(fontsize=10)
    ax3.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bar, value in zip(bars3a, df['memory_peak_mb']):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 10, 
                f'{value:.0f}MB', ha='center', va='bottom', fontsize=9, fontweight='bold')
    
    # 4. Scalability Analysis (Middle Left)
    ax4 = plt.subplot(3, 3, 4)
    ax4.plot(df['total_records'], df['execution_time'], 'o-', linewidth=3, markersize=10, 
             color=colors[0], markerfacecolor='white', markeredgewidth=2, markeredgecolor=colors[0])
    ax4.set_title('Scalability: Execution Time vs Data Size', fontsize=14, fontweight='bold', pad=20)
    ax4.set_xlabel('Total Records', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3)
    
    # Add trend line
    z = np.polyfit(df['total_records'], df['execution_time'], 1)
    p = np.poly1d(z)
    ax4.plot(df['total_records'], p(df['total_records']), "--", alpha=0.8, color='red', linewidth=2)
    
    # 5. Throughput Scaling (Middle Center)
    ax5 = plt.subplot(3, 3, 5)
    ax5.plot(df['total_records'], df['throughput'], 'o-', linewidth=3, markersize=10, 
             color=colors[1], markerfacecolor='white', markeredgewidth=2, markeredgecolor=colors[1])
    ax5.set_title('Throughput Scaling Analysis', fontsize=14, fontweight='bold', pad=20)
    ax5.set_xlabel('Total Records', fontsize=12, fontweight='bold')
    ax5.set_ylabel('Throughput (records/second)', fontsize=12, fontweight='bold')
    ax5.grid(True, alpha=0.3)
    
    # Add trend line
    z = np.polyfit(df['total_records'], df['throughput'], 1)
    p = np.poly1d(z)
    ax5.plot(df['total_records'], p(df['total_records']), "--", alpha=0.8, color='red', linewidth=2)
    
    # 6. Memory Efficiency (Middle Right)
    ax6 = plt.subplot(3, 3, 6)
    ax6.plot(df['total_records'], df['memory_peak_mb'], 'o-', linewidth=3, markersize=10, 
             color=colors[2], markerfacecolor='white', markeredgewidth=2, markeredgecolor=colors[2])
    ax6.set_title('Memory Efficiency Analysis', fontsize=14, fontweight='bold', pad=20)
    ax6.set_xlabel('Total Records', fontsize=12, fontweight='bold')
    ax6.set_ylabel('Peak Memory (MB)', fontsize=12, fontweight='bold')
    ax6.grid(True, alpha=0.3)
    
    # 7. Performance Summary Heatmap (Bottom Left)
    ax7 = plt.subplot(3, 3, 7)
    
    # Create normalized performance matrix
    perf_data = df[['execution_time', 'throughput', 'memory_peak_mb', 'memory_avg_mb']].copy()
    perf_data['execution_time_norm'] = 1 - (perf_data['execution_time'] - perf_data['execution_time'].min()) / (perf_data['execution_time'].max() - perf_data['execution_time'].min())
    perf_data['throughput_norm'] = (perf_data['throughput'] - perf_data['throughput'].min()) / (perf_data['throughput'].max() - perf_data['throughput'].min())
    perf_data['memory_peak_norm'] = 1 - (perf_data['memory_peak_mb'] - perf_data['memory_peak_mb'].min()) / (perf_data['memory_peak_mb'].max() - perf_data['memory_peak_mb'].min())
    perf_data['memory_avg_norm'] = 1 - (perf_data['memory_avg_mb'] - perf_data['memory_avg_mb'].min()) / (perf_data['memory_avg_mb'].max() - perf_data['memory_avg_mb'].min())
    
    heatmap_data = perf_data[['execution_time_norm', 'throughput_norm', 'memory_peak_norm', 'memory_avg_norm']].T
    heatmap_data.columns = df['data_size']
    heatmap_data.index = ['Time Efficiency', 'Throughput', 'Memory Peak', 'Memory Avg']
    
    sns.heatmap(heatmap_data, annot=True, cmap='RdYlGn', center=0.5, 
                ax=ax7, cbar_kws={'label': 'Performance Score'}, fmt='.2f')
    ax7.set_title('Performance Heatmap', fontsize=14, fontweight='bold', pad=20)
    
    # 8. Resource Utilization (Bottom Center)
    ax8 = plt.subplot(3, 3, 8)
    
    # Calculate efficiency metrics
    efficiency_metrics = {
        'Memory Efficiency': df['memory_peak_mb'].max() / df['memory_peak_mb'],
        'Time Efficiency': df['execution_time'].max() / df['execution_time'],
        'Throughput Efficiency': df['throughput'] / df['throughput'].max()
    }
    
    x = np.arange(len(df))
    width = 0.25
    
    for i, (metric, values) in enumerate(efficiency_metrics.items()):
        ax8.bar(x + i*width, values, width, label=metric, alpha=0.8, edgecolor='black', linewidth=1)
    
    ax8.set_title('Resource Utilization Efficiency', fontsize=14, fontweight='bold', pad=20)
    ax8.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax8.set_ylabel('Efficiency Score', fontsize=12, fontweight='bold')
    ax8.set_xticks(x + width)
    ax8.set_xticklabels(df['data_size'])
    ax8.legend(fontsize=10)
    ax8.grid(True, alpha=0.3, axis='y')
    
    # 9. Performance Trends (Bottom Right)
    ax9 = plt.subplot(3, 3, 9)
    
    # Create trend analysis
    records_growth = df['total_records'].pct_change().fillna(0) * 100
    time_growth = df['execution_time'].pct_change().fillna(0) * 100
    throughput_growth = df['throughput'].pct_change().fillna(0) * 100
    
    x = df['data_size'][1:]  # Skip first element for percentage change
    
    ax9.plot(x, records_growth[1:], 'o-', label='Data Growth %', linewidth=2, markersize=8)
    ax9.plot(x, time_growth[1:], 's-', label='Time Growth %', linewidth=2, markersize=8)
    ax9.plot(x, throughput_growth[1:], '^-', label='Throughput Growth %', linewidth=2, markersize=8)
    
    ax9.set_title('Performance Growth Trends', fontsize=14, fontweight='bold', pad=20)
    ax9.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax9.set_ylabel('Growth Percentage (%)', fontsize=12, fontweight='bold')
    ax9.legend(fontsize=10)
    ax9.grid(True, alpha=0.3)
    ax9.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    
    # Adjust layout
    plt.tight_layout()
    plt.subplots_adjust(top=0.95, hspace=0.3, wspace=0.3)
    
    # Save the plot
    output_file = Path("benchmarking/results/enhanced_performance_analysis.png")
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print(f"✅ Enhanced performance plots saved: {output_file}")
    
    plt.close()
    
    # Create a second plot focused on memory analysis
    create_memory_analysis_plot(df)
    
    # Create a third plot for scalability analysis
    create_scalability_analysis_plot(df)

def create_memory_analysis_plot(df):
    """Create detailed memory analysis plot"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('ETL Pipeline Memory Analysis - Detailed View', fontsize=20, fontweight='bold')
    
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
    
    # 1. Memory Usage Comparison
    ax1 = axes[0, 0]
    x = np.arange(len(df))
    width = 0.35
    
    bars1 = ax1.bar(x - width/2, df['memory_peak_mb'], width, label='Peak Memory', 
                    color=colors[0], alpha=0.8, edgecolor='black', linewidth=1)
    bars2 = ax1.bar(x + width/2, df['memory_avg_mb'], width, label='Average Memory', 
                    color=colors[1], alpha=0.8, edgecolor='black', linewidth=1)
    
    ax1.set_title('Memory Usage: Peak vs Average', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Memory (MB)', fontsize=12, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels(df['data_size'])
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    # Add value labels
    for bars in [bars1, bars2]:
        for bar in bars:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2, height + 5, 
                    f'{height:.0f}MB', ha='center', va='bottom', fontweight='bold')
    
    # 2. Memory Efficiency per Record
    ax2 = axes[0, 1]
    memory_per_record = df['memory_peak_mb'] / df['total_records'] * 1000  # KB per record
    bars = ax2.bar(df['data_size'], memory_per_record, color=colors[2], alpha=0.8, 
                   edgecolor='black', linewidth=1)
    ax2.set_title('Memory Efficiency per Record', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Memory per Record (KB)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3, axis='y')
    
    for bar, value in zip(bars, memory_per_record):
        ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                f'{value:.1f}KB', ha='center', va='bottom', fontweight='bold')
    
    # 3. Memory Scaling Trend
    ax3 = axes[1, 0]
    ax3.plot(df['total_records'], df['memory_peak_mb'], 'o-', linewidth=3, markersize=10, 
             color=colors[0], markerfacecolor='white', markeredgewidth=2, markeredgecolor=colors[0])
    ax3.set_title('Memory Scaling with Data Size', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Total Records', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Peak Memory (MB)', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    
    # Add trend line
    z = np.polyfit(df['total_records'], df['memory_peak_mb'], 1)
    p = np.poly1d(z)
    ax3.plot(df['total_records'], p(df['total_records']), "--", alpha=0.8, color='red', linewidth=2)
    
    # 4. Memory Utilization Ratio
    ax4 = axes[1, 1]
    utilization_ratio = df['memory_avg_mb'] / df['memory_peak_mb'] * 100
    bars = ax4.bar(df['data_size'], utilization_ratio, color=colors[3], alpha=0.8, 
                   edgecolor='black', linewidth=1)
    ax4.set_title('Memory Utilization Ratio', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Avg/Peak Ratio (%)', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    
    for bar, value in zip(bars, utilization_ratio):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    
    # Save the plot
    output_file = Path("benchmarking/results/memory_analysis_detailed.png")
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print(f"✅ Memory analysis plot saved: {output_file}")
    
    plt.close()

def create_scalability_analysis_plot(df):
    """Create scalability analysis plot"""
    
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('ETL Pipeline Scalability Analysis', fontsize=20, fontweight='bold')
    
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
    
    # 1. Linear Scaling Analysis
    ax1 = axes[0, 0]
    ax1.plot(df['total_records'], df['execution_time'], 'o-', linewidth=3, markersize=10, 
             color=colors[0], markerfacecolor='white', markeredgewidth=2, markeredgecolor=colors[0])
    ax1.set_title('Execution Time Scaling', fontsize=14, fontweight='bold')
    ax1.set_xlabel('Total Records', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    
    # Add trend line and R²
    z = np.polyfit(df['total_records'], df['execution_time'], 1)
    p = np.poly1d(z)
    ax1.plot(df['total_records'], p(df['total_records']), "--", alpha=0.8, color='red', linewidth=2)
    
    # Calculate R²
    y_pred = p(df['total_records'])
    ss_res = np.sum((df['execution_time'] - y_pred) ** 2)
    ss_tot = np.sum((df['execution_time'] - np.mean(df['execution_time'])) ** 2)
    r_squared = 1 - (ss_res / ss_tot)
    ax1.text(0.05, 0.95, f'R² = {r_squared:.3f}', transform=ax1.transAxes, 
             bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8), fontweight='bold')
    
    # 2. Throughput Scaling
    ax2 = axes[0, 1]
    ax2.plot(df['total_records'], df['throughput'], 'o-', linewidth=3, markersize=10, 
             color=colors[1], markerfacecolor='white', markeredgewidth=2, markeredgecolor=colors[1])
    ax2.set_title('Throughput Scaling', fontsize=14, fontweight='bold')
    ax2.set_xlabel('Total Records', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Throughput (records/second)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Add trend line
    z = np.polyfit(df['total_records'], df['throughput'], 1)
    p = np.poly1d(z)
    ax2.plot(df['total_records'], p(df['total_records']), "--", alpha=0.8, color='red', linewidth=2)
    
    # 3. Performance Index
    ax3 = axes[1, 0]
    # Create a composite performance index
    performance_index = (df['throughput'] / df['throughput'].max()) * 100
    bars = ax3.bar(df['data_size'], performance_index, color=colors[2], alpha=0.8, 
                   edgecolor='black', linewidth=1)
    ax3.set_title('Performance Index (Normalized)', fontsize=14, fontweight='bold')
    ax3.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax3.set_ylabel('Performance Index', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3, axis='y')
    
    for bar, value in zip(bars, performance_index):
        ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2, 
                f'{value:.1f}', ha='center', va='bottom', fontweight='bold')
    
    # 4. Scaling Efficiency
    ax4 = axes[1, 1]
    # Calculate scaling efficiency (throughput improvement vs data increase)
    data_increase = df['total_records'].pct_change().fillna(0) * 100
    throughput_increase = df['throughput'].pct_change().fillna(0) * 100
    scaling_efficiency = throughput_increase / data_increase.replace(0, np.inf) * 100
    
    bars = ax4.bar(df['data_size'][1:], scaling_efficiency[1:], color=colors[3], alpha=0.8, 
                   edgecolor='black', linewidth=1)
    ax4.set_title('Scaling Efficiency', fontsize=14, fontweight='bold')
    ax4.set_xlabel('Dataset Size', fontsize=12, fontweight='bold')
    ax4.set_ylabel('Efficiency (%)', fontsize=12, fontweight='bold')
    ax4.grid(True, alpha=0.3, axis='y')
    
    for bar, value in zip(bars, scaling_efficiency[1:]):
        ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 5, 
                f'{value:.1f}%', ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    
    # Save the plot
    output_file = Path("benchmarking/results/scalability_analysis.png")
    plt.savefig(output_file, dpi=300, bbox_inches='tight', facecolor='white', edgecolor='none')
    print(f"✅ Scalability analysis plot saved: {output_file}")
    
    plt.close()

if __name__ == "__main__":
    create_enhanced_performance_plots()
    print("🎉 All enhanced performance plots generated successfully!")
