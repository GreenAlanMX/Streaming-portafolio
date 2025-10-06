#!/usr/bin/env python3
"""
Performance Benchmarking Script for ETL Pipeline
===============================================
Runs ETL pipeline with different data sizes and measures performance metrics.
Generates performance curves and identifies bottlenecks.
"""

import os
import time
import json
import subprocess
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any
import psutil
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@dataclass
class BenchmarkResult:
    """Store benchmark results for a single run"""
    data_size: str
    users_count: int
    sessions_count: int
    content_count: int
    total_records: int
    execution_time: float
    memory_peak: float
    cpu_avg: float
    throughput: float  # records per second
    stage_times: Dict[str, float]
    success: bool
    error_message: str = ""

class PerformanceBenchmark:
    """Main benchmarking class"""
    
    def __init__(self):
        self.results: List[BenchmarkResult] = []
        self.base_path = Path(__file__).parent.parent
        self.etl_script = self.base_path / "etl" / "etl_pipeline_enhanced.py"
        self.output_dir = self.base_path / "benchmarking" / "results"
        self.output_dir.mkdir(exist_ok=True)
        
        # Data size configurations
        self.data_sizes = {
            "small": {"users": 1000, "sessions": 5000, "content": 100},
            "medium": {"users": 5000, "sessions": 25000, "content": 500},
            "large": {"users": 10000, "sessions": 50000, "content": 1000},
            "xlarge": {"users": 20000, "sessions": 100000, "content": 2000},
            "xxlarge": {"users": 50000, "sessions": 250000, "content": 5000}
        }
    
    def generate_test_data(self, size_config: Dict[str, int]) -> None:
        """Generate test data for benchmarking"""
        logger.info(f"Generating test data: {size_config}")
        
        # Create data generator script
        generator_script = f"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Configuration
users_count = {size_config['users']}
sessions_count = {size_config['sessions']}
content_count = {size_config['content']}

# Generate users
users_data = []
for i in range(users_count):
    user_id = f"U{{i+1:05d}}"
    age = np.random.randint(18, 80)
    subscription_types = ['Basic', 'Standard', 'Premium']
    subscription_type = np.random.choice(subscription_types, p=[0.4, 0.4, 0.2])
    countries = ['Argentina', 'Mexico', 'Brazil', 'Chile', 'Colombia', 'Peru']
    country = np.random.choice(countries)
    registration_date = datetime.now() - timedelta(days=np.random.randint(30, 365))
    
    users_data.append({{
        'user_id': user_id,
        'age': age,
        'subscription_type': subscription_type,
        'country': country,
        'registration_date': registration_date.strftime('%Y-%m-%d')
    }})

users_df = pd.DataFrame(users_data)
users_df.to_csv('etl/data/raw/users.csv', index=False)

# Generate content
content_data = []
genres = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi', 'Romance', 'Thriller', 'Documentary']
content_types = ['Movie', 'Series']

for i in range(content_count):
    content_id = f"C{{i+1:05d}}"
    title = f"Content {{i+1}}"
    genre = np.random.choice(genres)
    content_type = np.random.choice(content_types)
    release_year = np.random.randint(1990, 2024)
    duration = np.random.randint(60, 180) if content_type == 'Movie' else np.random.randint(20, 60)
    
    content_data.append({{
        'content_id': content_id,
        'title': title,
        'genre': genre,
        'content_type': content_type,
        'release_year': release_year,
        'duration': duration
    }})

content_df = pd.DataFrame(content_data)
content_df.to_csv('etl/data/raw/content.csv', index=False)

# Generate viewing sessions
sessions_data = []
for i in range(sessions_count):
    session_id = f"S{{i+1:08d}}"
    user_id = f"U{{np.random.randint(1, users_count+1):05d}}"
    content_id = f"C{{np.random.randint(1, content_count+1):05d}}"
    watch_date = datetime.now() - timedelta(days=np.random.randint(1, 30))
    duration_watched = np.random.randint(1, 120)
    completion_rate = min(100, (duration_watched / 90) * 100)
    
    sessions_data.append({{
        'session_id': session_id,
        'user_id': user_id,
        'content_id': content_id,
        'watch_date': watch_date.strftime('%Y-%m-%d'),
        'duration_watched': duration_watched,
        'completion_rate': completion_rate
    }})

sessions_df = pd.DataFrame(sessions_data)
sessions_df.to_csv('etl/data/raw/viewing_sessions.csv', index=False)

print(f"Generated: {{users_count}} users, {{sessions_count}} sessions, {{content_count}} content items")
"""
        
        # Write and execute generator script
        with open("temp_data_generator.py", "w") as f:
            f.write(generator_script)
        
        try:
            subprocess.run(["python", "temp_data_generator.py"], check=True, cwd=self.base_path)
        finally:
            if os.path.exists("temp_data_generator.py"):
                os.remove("temp_data_generator.py")
    
    def run_etl_benchmark(self, size_name: str, size_config: Dict[str, int]) -> BenchmarkResult:
        """Run ETL pipeline and measure performance"""
        logger.info(f"Running benchmark for {size_name} dataset")
        
        # Generate test data
        self.generate_test_data(size_config)
        
        # Set environment to use files instead of database
        env = os.environ.copy()
        env['SOURCE_MODE'] = 'files'
        
        # Monitor system resources
        process = psutil.Process()
        memory_before = process.memory_info().rss / 1024 / 1024  # MB
        cpu_before = process.cpu_percent()
        
        start_time = time.time()
        stage_times = {}
        
        try:
            # Run ETL pipeline
            result = subprocess.run(
                ["python", str(self.etl_script)],
                cwd=self.base_path,
                env=env,
                capture_output=True,
                text=True,
                timeout=1800  # 30 minutes timeout
            )
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Get final memory usage
            memory_after = process.memory_info().rss / 1024 / 1024  # MB
            memory_peak = max(0, memory_after - memory_before)
            
            # Calculate throughput
            total_records = sum(size_config.values())
            throughput = total_records / execution_time if execution_time > 0 else 0
            
            # Parse stage times from ETL output (if available)
            stage_times = self._parse_stage_times(result.stdout)
            
            success = result.returncode == 0
            error_message = result.stderr if not success else ""
            
            return BenchmarkResult(
                data_size=size_name,
                users_count=size_config['users'],
                sessions_count=size_config['sessions'],
                content_count=size_config['content'],
                total_records=total_records,
                execution_time=execution_time,
                memory_peak=memory_peak,
                cpu_avg=cpu_before,  # Simplified for now
                throughput=throughput,
                stage_times=stage_times,
                success=success,
                error_message=error_message
            )
            
        except subprocess.TimeoutExpired:
            logger.error(f"ETL pipeline timed out for {size_name}")
            return BenchmarkResult(
                data_size=size_name,
                users_count=size_config['users'],
                sessions_count=size_config['sessions'],
                content_count=size_config['content'],
                total_records=sum(size_config.values()),
                execution_time=1800,
                memory_peak=0,
                cpu_avg=0,
                throughput=0,
                stage_times={},
                success=False,
                error_message="Timeout after 30 minutes"
            )
        except Exception as e:
            logger.error(f"Error running ETL for {size_name}: {e}")
            return BenchmarkResult(
                data_size=size_name,
                users_count=size_config['users'],
                sessions_count=size_config['sessions'],
                content_count=size_config['content'],
                total_records=sum(size_config.values()),
                execution_time=0,
                memory_peak=0,
                cpu_avg=0,
                throughput=0,
                stage_times={},
                success=False,
                error_message=str(e)
            )
    
    def _parse_stage_times(self, output: str) -> Dict[str, float]:
        """Parse stage execution times from ETL output"""
        stage_times = {}
        # This would need to be implemented based on actual ETL output format
        # For now, return empty dict
        return stage_times
    
    def run_all_benchmarks(self) -> None:
        """Run benchmarks for all data sizes"""
        logger.info("Starting performance benchmarking")
        
        for size_name, size_config in self.data_sizes.items():
            result = self.run_etl_benchmark(size_name, size_config)
            self.results.append(result)
            
            # Save intermediate results
            self.save_results()
            
            logger.info(f"Completed {size_name}: {result.execution_time:.2f}s, "
                       f"Throughput: {result.throughput:.2f} records/s")
    
    def save_results(self) -> None:
        """Save benchmark results to CSV"""
        if not self.results:
            return
        
        # Convert results to DataFrame
        data = []
        for result in self.results:
            data.append({
                'data_size': result.data_size,
                'users_count': result.users_count,
                'sessions_count': result.sessions_count,
                'content_count': result.content_count,
                'total_records': result.total_records,
                'execution_time': result.execution_time,
                'memory_peak_mb': result.memory_peak,
                'cpu_avg': result.cpu_avg,
                'throughput': result.throughput,
                'success': result.success,
                'error_message': result.error_message
            })
        
        df = pd.DataFrame(data)
        output_file = self.output_dir / "performance_benchmark_results.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
    
    def generate_performance_plots(self) -> None:
        """Generate performance analysis plots"""
        if not self.results:
            logger.warning("No results to plot")
            return
        
        # Set up the plotting style
        plt.style.use('seaborn-v0_8')
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('ETL Pipeline Performance Analysis', fontsize=16, fontweight='bold')
        
        # Filter successful results
        successful_results = [r for r in self.results if r.success]
        if not successful_results:
            logger.warning("No successful results to plot")
            return
        
        # Extract data for plotting
        sizes = [r.data_size for r in successful_results]
        records = [r.total_records for r in successful_results]
        times = [r.execution_time for r in successful_results]
        throughputs = [r.throughput for r in successful_results]
        memories = [r.memory_peak for r in successful_results]
        
        # 1. Execution Time vs Data Size
        axes[0, 0].plot(records, times, 'bo-', linewidth=2, markersize=8)
        axes[0, 0].set_xlabel('Total Records')
        axes[0, 0].set_ylabel('Execution Time (seconds)')
        axes[0, 0].set_title('Execution Time vs Data Size')
        axes[0, 0].grid(True, alpha=0.3)
        
        # 2. Throughput vs Data Size
        axes[0, 1].plot(records, throughputs, 'go-', linewidth=2, markersize=8)
        axes[0, 1].set_xlabel('Total Records')
        axes[0, 1].set_ylabel('Throughput (records/second)')
        axes[0, 1].set_title('Throughput vs Data Size')
        axes[0, 1].grid(True, alpha=0.3)
        
        # 3. Memory Usage vs Data Size
        axes[1, 0].plot(records, memories, 'ro-', linewidth=2, markersize=8)
        axes[1, 0].set_xlabel('Total Records')
        axes[1, 0].set_ylabel('Memory Peak (MB)')
        axes[1, 0].set_title('Memory Usage vs Data Size')
        axes[1, 0].grid(True, alpha=0.3)
        
        # 4. Performance Summary
        axes[1, 1].bar(sizes, times, alpha=0.7, color='skyblue')
        axes[1, 1].set_xlabel('Data Size')
        axes[1, 1].set_ylabel('Execution Time (seconds)')
        axes[1, 1].set_title('Execution Time by Data Size')
        axes[1, 1].tick_params(axis='x', rotation=45)
        
        plt.tight_layout()
        
        # Save plot
        plot_file = self.output_dir / "performance_analysis.png"
        plt.savefig(plot_file, dpi=300, bbox_inches='tight')
        logger.info(f"Performance plots saved to {plot_file}")
        
        # Also save as PDF for better quality
        plot_pdf = self.output_dir / "performance_analysis.pdf"
        plt.savefig(plot_pdf, bbox_inches='tight')
        
        plt.show()
    
    def identify_bottlenecks(self) -> Dict[str, Any]:
        """Identify performance bottlenecks based on results"""
        if not self.results:
            return {}
        
        successful_results = [r for r in self.results if r.success]
        if len(successful_results) < 2:
            return {"error": "Need at least 2 successful runs to identify bottlenecks"}
        
        # Calculate scaling factors
        scaling_analysis = {}
        
        # Linear scaling analysis
        for i in range(1, len(successful_results)):
            prev = successful_results[i-1]
            curr = successful_results[i]
            
            data_ratio = curr.total_records / prev.total_records
            time_ratio = curr.execution_time / prev.execution_time
            
            scaling_factor = time_ratio / data_ratio
            
            scaling_analysis[f"{prev.data_size}_to_{curr.data_size}"] = {
                "data_ratio": data_ratio,
                "time_ratio": time_ratio,
                "scaling_factor": scaling_factor,
                "is_linear": abs(scaling_factor - 1.0) < 0.2,
                "is_quadratic": abs(scaling_factor - 2.0) < 0.2
            }
        
        # Memory analysis
        memory_growth = []
        for i in range(1, len(successful_results)):
            prev = successful_results[i-1]
            curr = successful_results[i]
            memory_ratio = curr.memory_peak / prev.memory_peak if prev.memory_peak > 0 else 0
            memory_growth.append(memory_ratio)
        
        # Throughput analysis
        throughput_trend = []
        for i in range(1, len(successful_results)):
            prev = successful_results[i-1]
            curr = successful_results[i]
            throughput_ratio = curr.throughput / prev.throughput if prev.throughput > 0 else 0
            throughput_trend.append(throughput_ratio)
        
        bottlenecks = {
            "scaling_analysis": scaling_analysis,
            "memory_growth": memory_growth,
            "throughput_trend": throughput_trend,
            "recommendations": self._generate_recommendations(scaling_analysis, memory_growth, throughput_trend)
        }
        
        return bottlenecks
    
    def _generate_recommendations(self, scaling_analysis: Dict, memory_growth: List[float], 
                                throughput_trend: List[float]) -> List[str]:
        """Generate performance recommendations"""
        recommendations = []
        
        # Check for linear scaling issues
        non_linear_scaling = [k for k, v in scaling_analysis.items() 
                            if not v["is_linear"] and v["scaling_factor"] > 1.5]
        if non_linear_scaling:
            recommendations.append(
                f"Non-linear scaling detected in: {', '.join(non_linear_scaling)}. "
                "Consider optimizing data processing algorithms or implementing parallel processing."
            )
        
        # Check memory growth
        if any(growth > 2.0 for growth in memory_growth):
            recommendations.append(
                "High memory growth detected. Consider implementing data streaming "
                "or chunked processing to reduce memory footprint."
            )
        
        # Check throughput degradation
        if any(trend < 0.8 for trend in throughput_trend):
            recommendations.append(
                "Throughput degradation detected with larger datasets. "
                "Consider database indexing, query optimization, or caching strategies."
            )
        
        # General recommendations
        recommendations.extend([
            "Consider implementing parallel processing for independent operations",
            "Add database indexes on frequently queried columns",
            "Implement data partitioning for very large datasets",
            "Consider using more efficient data formats (Parquet, Arrow)",
            "Monitor and optimize I/O operations"
        ])
        
        return recommendations
    
    def generate_report(self) -> None:
        """Generate comprehensive performance report"""
        bottlenecks = self.identify_bottlenecks()
        
        report = f"""
# ETL Pipeline Performance Benchmark Report

## Executive Summary
- Total benchmark runs: {len(self.results)}
- Successful runs: {len([r for r in self.results if r.success])}
- Failed runs: {len([r for r in self.results if not r.success])}

## Performance Results
"""
        
        for result in self.results:
            status = "✅ SUCCESS" if result.success else "❌ FAILED"
            report += f"""
### {result.data_size.upper()} Dataset
- **Status**: {status}
- **Records**: {result.total_records:,}
- **Execution Time**: {result.execution_time:.2f} seconds
- **Throughput**: {result.throughput:.2f} records/second
- **Memory Peak**: {result.memory_peak:.2f} MB
"""
            if not result.success:
                report += f"- **Error**: {result.error_message}\n"
        
        if bottlenecks and "recommendations" in bottlenecks:
            report += "\n## Performance Bottlenecks & Recommendations\n"
            for i, rec in enumerate(bottlenecks["recommendations"], 1):
                report += f"{i}. {rec}\n"
        
        # Save report
        report_file = self.output_dir / "performance_report.md"
        with open(report_file, 'w') as f:
            f.write(report)
        
        logger.info(f"Performance report saved to {report_file}")
        print(report)

def main():
    """Main function to run the benchmark"""
    benchmark = PerformanceBenchmark()
    
    try:
        # Run all benchmarks
        benchmark.run_all_benchmarks()
        
        # Generate analysis
        benchmark.generate_performance_plots()
        benchmark.generate_report()
        
        logger.info("Benchmarking completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Benchmarking interrupted by user")
    except Exception as e:
        logger.error(f"Benchmarking failed: {e}")
        raise

if __name__ == "__main__":
    main()
