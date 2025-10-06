#!/usr/bin/env python3
"""
Unit Tests for ETL Pipeline - Fixed Version
===========================================
Comprehensive test suite for the streaming portfolio ETL pipeline.
"""

import unittest
import pandas as pd
import numpy as np
import tempfile
import os
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
import json

# Add the project root to the path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Import ETL functions
from etl.etl_pipeline_enhanced import (
    extract_users, extract_sessions, extract_content,
    validate_users, validate_sessions, validate_content,
    aggregate_user_metrics, cluster_users, load_incremental,
    transform
)

class TestDataExtraction(unittest.TestCase):
    """Test data extraction functions"""
    
    def setUp(self):
        """Set up test data"""
        self.temp_dir = tempfile.mkdtemp()
        self.raw_path = Path(self.temp_dir) / "raw"
        self.raw_path.mkdir()
        
        # Create test data
        self.users_data = pd.DataFrame({
            'user_id': ['U001', 'U002', 'U003'],
            'age': [25, 30, 35],
            'subscription_type': ['Basic', 'Standard', 'Premium'],
            'country': ['Argentina', 'Mexico', 'Brazil'],
            'registration_date': ['2023-01-01', '2023-02-01', '2023-03-01']
        })
        
        self.sessions_data = pd.DataFrame({
            'session_id': ['S001', 'S002', 'S003'],
            'user_id': ['U001', 'U002', 'U003'],
            'content_id': ['C001', 'C002', 'C003'],
            'watch_date': ['2023-01-15', '2023-02-15', '2023-03-15'],
            'duration_watched': [60, 90, 120],
            'completion_rate': [80.0, 90.0, 100.0]
        })
        
        self.content_data = pd.DataFrame({
            'content_id': ['C001', 'C002', 'C003'],
            'title': ['Movie 1', 'Series 1', 'Movie 2'],
            'genre': ['Action', 'Drama', 'Comedy'],
            'content_type': ['Movie', 'Series', 'Movie'],
            'release_year': [2020, 2021, 2022],
            'duration': [120, 45, 90]
        })
        
        # Save test data
        self.users_data.to_csv(self.raw_path / "users.csv", index=False)
        self.sessions_data.to_csv(self.raw_path / "viewing_sessions.csv", index=False)
        self.content_data.to_csv(self.raw_path / "content.csv", index=False)
    
    def tearDown(self):
        """Clean up test data"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('etl.etl_pipeline_enhanced.RAW_PATH')
    def test_extract_users_from_files(self, mock_raw_path):
        """Test user extraction from CSV files"""
        mock_raw_path.return_value = self.raw_path
        
        with patch.dict(os.environ, {'SOURCE_MODE': 'files'}):
            result = extract_users()
            
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertIn('user_id', result.columns)
        self.assertIn('age', result.columns)
    
    @patch('etl.etl_pipeline_enhanced.RAW_PATH')
    def test_extract_sessions_from_files(self, mock_raw_path):
        """Test session extraction from CSV files"""
        mock_raw_path.return_value = self.raw_path
        
        with patch.dict(os.environ, {'SOURCE_MODE': 'files'}):
            result = extract_sessions()
            
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertIn('session_id', result.columns)
        self.assertIn('duration_watched', result.columns)
    
    @patch('etl.etl_pipeline_enhanced.RAW_PATH')
    def test_extract_content_from_files(self, mock_raw_path):
        """Test content extraction from CSV files"""
        mock_raw_path.return_value = self.raw_path
        
        with patch.dict(os.environ, {'SOURCE_MODE': 'files'}):
            result = extract_content()
            
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)
        self.assertIn('content_id', result.columns)
        self.assertIn('title', result.columns)

class TestDataValidation(unittest.TestCase):
    """Test data validation and cleaning functions"""
    
    def setUp(self):
        """Set up test data with various issues"""
        self.test_users = pd.DataFrame({
            'user_id': ['U001', 'U002', 'U003', 'U004'],
            'age': [25, 30, -5, 200],  # Invalid ages
            'subscription_type': ['Basic', 'Standard', 'Invalid', 'Premium'],
            'country': ['Argentina', 'Mexico', 'Brazil', 'Unknown'],
            'registration_date': ['2023-01-01', 'invalid-date', '2023-03-01', '2023-04-01']
        })
        
        self.test_sessions = pd.DataFrame({
            'session_id': ['S001', 'S002', 'S003', 'S004'],
            'user_id': ['U001', 'U002', 'U003', 'U004'],
            'content_id': ['C001', 'C002', 'C003', 'C004'],
            'watch_date': ['2023-01-15', 'invalid-date', '2023-03-15', '2023-04-15'],
            'duration_watched': [60, 90, -10, 120],  # Invalid duration
            'completion_rate': [80.0, 90.0, 150.0, 100.0]  # Invalid completion rate
        })
        
        self.test_content = pd.DataFrame({
            'content_id': ['C001', 'C002', 'C003', 'C004'],
            'title': ['Movie 1', 'Series 1', 'Movie 2', 'Series 2'],
            'genre': ['Action', 'Drama', 'Comedy', 'Invalid'],
            'content_type': ['Movie', 'Series', 'Movie', 'Invalid'],
            'release_year': [2020, 2021, 2022, 2030],  # Future year
            'duration': [120, 45, 90, -30]  # Invalid duration
        })
    
    def test_validate_users(self):
        """Test user data validation"""
        result = validate_users(self.test_users)
        
        self.assertIsInstance(result, pd.DataFrame)
        # Check that invalid ages are cleaned
        self.assertTrue((result['age'] >= 0).all())
        self.assertTrue((result['age'] <= 120).all())
    
    def test_validate_sessions(self):
        """Test session data validation"""
        result = validate_sessions(self.test_sessions)
        
        self.assertIsInstance(result, pd.DataFrame)
        # Check that invalid completion rates are cleaned
        self.assertTrue((result['completion_rate'] >= 0).all())
        self.assertTrue((result['completion_rate'] <= 100).all())
    
    def test_validate_content(self):
        """Test content data validation"""
        result = validate_content(self.test_content)
        
        self.assertIsInstance(result, pd.DataFrame)
        # Check that invalid release years are cleaned
        current_year = pd.Timestamp.now().year
        self.assertTrue((result['release_year'] <= current_year).all())

class TestDataTransformation(unittest.TestCase):
    """Test data transformation functions"""
    
    def setUp(self):
        """Set up test data for transformation"""
        self.users = pd.DataFrame({
            'user_id': ['U001', 'U002', 'U003'],
            'age': [25, 30, 35],
            'subscription_type': ['Basic', 'Standard', 'Premium'],
            'country': ['Argentina', 'Mexico', 'Brazil'],
            'registration_date': ['2023-01-01', '2023-02-01', '2023-03-01']
        })
        
        self.sessions = pd.DataFrame({
            'session_id': ['S001', 'S002', 'S003'],
            'user_id': ['U001', 'U002', 'U003'],
            'content_id': ['C001', 'C002', 'C003'],
            'watch_date': ['2023-01-15', '2023-02-15', '2023-03-15'],
            'duration_watched': [60, 90, 120],
            'completion_rate': [80.0, 90.0, 100.0]
        })
        
        self.content = pd.DataFrame({
            'content_id': ['C001', 'C002', 'C003'],
            'title': ['Movie 1', 'Series 1', 'Movie 2'],
            'genre': ['Action', 'Drama', 'Comedy'],
            'content_type': ['Movie', 'Series', 'Movie'],
            'release_year': [2020, 2021, 2022],
            'duration': [120, 45, 90]
        })
    
    def test_transform_function(self):
        """Test data transformation"""
        result = transform(self.users, self.sessions, self.content)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        # Check that merged data has expected columns
        expected_columns = ['user_id', 'session_id', 'content_id', 'age', 'subscription_type']
        for col in expected_columns:
            self.assertIn(col, result.columns)

class TestDataAggregation(unittest.TestCase):
    """Test data aggregation functions"""
    
    def setUp(self):
        """Set up test data for aggregation"""
        self.test_data = pd.DataFrame({
            'user_id': ['U001', 'U001', 'U002', 'U002', 'U003'],
            'session_id': ['S001', 'S002', 'S003', 'S004', 'S005'],
            'content_id': ['C001', 'C002', 'C001', 'C003', 'C002'],
            'duration_watched': [60, 90, 45, 120, 75],
            'completion_rate': [80.0, 90.0, 60.0, 100.0, 85.0],
            'age': [25, 25, 30, 30, 35],
            'subscription_type': ['Basic', 'Basic', 'Standard', 'Standard', 'Premium'],
            'country': ['Argentina', 'Argentina', 'Mexico', 'Mexico', 'Brazil']
        })
    
    def test_aggregate_user_metrics(self):
        """Test user-level aggregation"""
        result = aggregate_user_metrics(self.test_data)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)  # 3 unique users
        
        # Check required columns
        required_columns = [
            'user_id', 'sessions_count', 'avg_duration', 'duration_std',
            'avg_completion', 'completion_std', 'unique_content'
        ]
        for col in required_columns:
            self.assertIn(col, result.columns)
        
        # Check aggregation logic
        user_001 = result[result['user_id'] == 'U001'].iloc[0]
        self.assertEqual(user_001['sessions_count'], 2)
        self.assertEqual(user_001['unique_content'], 2)

class TestClustering(unittest.TestCase):
    """Test clustering functionality"""
    
    def setUp(self):
        """Set up test data for clustering"""
        self.user_agg_data = pd.DataFrame({
            'user_id': ['U001', 'U002', 'U003', 'U004', 'U005'],
            'sessions_count': [10, 20, 15, 25, 30],
            'avg_duration': [60, 90, 75, 120, 100],
            'avg_completion': [80, 85, 90, 95, 88],
            'unique_content': [5, 10, 8, 15, 12],
            'subscription_numeric': [1, 2, 1, 3, 2]
        })
    
    def test_cluster_users(self):
        """Test clustering functionality"""
        result = cluster_users(self.user_agg_data)
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('cluster_kmeans', result.columns)
        self.assertEqual(len(result), 5)
        
        # Check that clusters are assigned
        self.assertTrue(result['cluster_kmeans'].notna().all())
        self.assertTrue((result['cluster_kmeans'] >= 0).all())

class TestDataLoading(unittest.TestCase):
    """Test data loading functions"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.processed_path = Path(self.temp_dir) / "processed"
        self.processed_path.mkdir()
        
        self.test_data = pd.DataFrame({
            'user_id': ['U001', 'U002'],
            'sessions_count': [10, 20],
            'avg_duration': [60, 90],
            'watch_date': ['2023-01-01', '2023-01-02']
        })
    
    def tearDown(self):
        """Clean up test data"""
        import shutil
        shutil.rmtree(self.temp_dir)
    
    @patch('etl.etl_pipeline_enhanced.PROCESSED_PATH')
    def test_load_incremental_new_file(self, mock_processed_path):
        """Test incremental loading with new file"""
        mock_processed_path.return_value = self.processed_path
        
        # Test with new file
        result = load_incremental(self.test_data)
        
        # Check that file was created
        output_file = self.processed_path / "streaming_data.parquet"
        self.assertTrue(output_file.exists())

class TestErrorHandling(unittest.TestCase):
    """Test error handling and edge cases"""
    
    def test_empty_dataframe_handling(self):
        """Test handling of empty dataframes"""
        empty_df = pd.DataFrame()
        
        # Should handle empty dataframes gracefully
        result = validate_users(empty_df)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)
    
    def test_invalid_data_types(self):
        """Test handling of invalid data types"""
        invalid_data = pd.DataFrame({
            'user_id': [1, 2, 3],  # Should be strings
            'age': ['invalid', 'also_invalid', 25],  # Mixed types
            'completion_rate': ['not_a_number', 80.0, 90.0]
        })
        
        result = validate_users(invalid_data)
        self.assertIsInstance(result, pd.DataFrame)

class TestPerformanceMetrics(unittest.TestCase):
    """Test performance monitoring"""
    
    def test_execution_time_measurement(self):
        """Test execution time measurement"""
        import time
        
        start_time = time.time()
        time.sleep(0.1)  # Simulate work
        end_time = time.time()
        
        execution_time = end_time - start_time
        self.assertGreater(execution_time, 0.09)
        self.assertLess(execution_time, 0.2)
    
    def test_memory_usage_measurement(self):
        """Test memory usage measurement"""
        import psutil
        
        process = psutil.Process()
        memory_before = process.memory_info().rss
        
        # Create some data to use memory
        test_data = pd.DataFrame({
            'col1': range(1000),
            'col2': range(1000)
        })
        
        memory_after = process.memory_info().rss
        memory_used = memory_after - memory_before
        
        self.assertGreaterEqual(memory_used, 0)

def run_unit_tests():
    """Run all unit tests and return results"""
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestDataExtraction,
        TestDataValidation,
        TestDataTransformation,
        TestDataAggregation,
        TestClustering,
        TestDataLoading,
        TestErrorHandling,
        TestPerformanceMetrics
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    return result

if __name__ == "__main__":
    result = run_unit_tests()
    
    # Print summary
    print(f"\n{'='*50}")
    print("UNIT TEST SUMMARY")
    print(f"{'='*50}")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    if result.failures:
        print("\nFAILURES:")
        for test, traceback in result.failures:
            print(f"- {test}: {traceback}")
    
    if result.errors:
        print("\nERRORS:")
        for test, traceback in result.errors:
            print(f"- {test}: {traceback}")
