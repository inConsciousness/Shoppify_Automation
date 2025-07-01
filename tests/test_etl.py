import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import tempfile
import os
import sys
from pathlib import Path

# Add the project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import the functions to test
from spark_jobs.example_etl import clean_product_data, validate_data, create_spark_session

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestShopifyETL") \
        .master("local[2]") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    yield spark
    spark.stop()

@pytest.fixture
def sample_data(spark):
    """Create sample product data for testing"""
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("tags", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("description", StringType(), True)
    ])
    
    data = [
        ("Product 1", "SKU001", "Vendor A", "Electronics", "tag1,tag2", 99.99, "Great product"),
        ("Product 2", "SKU002", "", "Clothing", "tag3", 49.99, None),
        ("Product 3", "SKU003", "Vendor B", "", "tag4", 0.0, "Another product"),
        ("Product 1", "SKU001", "Vendor A", "Electronics", "tag1,tag2", 99.99, "Great product"),  # Duplicate
        ("", "SKU004", "Vendor C", "Home", "tag5", 29.99, "Home product"),
        ("Product 5", "SKU005", "Vendor D", "Sports", "tag6", -10.0, "Sports item")
    ]
    
    return spark.createDataFrame(data, schema)

def test_create_spark_session():
    """Test Spark session creation"""
    spark = create_spark_session()
    assert spark is not None
    assert spark.appName == "ShopifyETL"
    spark.stop()

def test_clean_product_data(spark, sample_data):
    """Test data cleaning functionality"""
    # Clean the data
    cleaned_df = clean_product_data(sample_data)
    
    # Check that duplicates are removed
    assert cleaned_df.count() == 5  # Should be 5 unique records (1 duplicate removed)
    
    # Check that empty vendors are replaced
    empty_vendors = cleaned_df.filter(cleaned_df.vendor == "Unknown Vendor").count()
    assert empty_vendors == 1
    
    # Check that empty product types are replaced
    empty_types = cleaned_df.filter(cleaned_df.product_type == "General").count()
    assert empty_types == 1
    
    # Check that negative prices are set to 0
    zero_prices = cleaned_df.filter(cleaned_df.price == 0.0).count()
    assert zero_prices == 2  # One original 0.0 and one negative price converted to 0.0
    
    # Check that features column is added
    assert "features" in cleaned_df.columns

def test_validate_data(spark, sample_data):
    """Test data validation functionality"""
    # Clean data first
    cleaned_df = clean_product_data(sample_data)
    
    # Validate data
    validated_df = validate_data(cleaned_df)
    
    # Should return the same dataframe
    assert validated_df.count() == cleaned_df.count()
    
    # Check that validation doesn't change data
    assert validated_df.collect() == cleaned_df.collect()

def test_data_quality_checks(spark):
    """Test specific data quality scenarios"""
    # Test with completely empty dataframe
    empty_schema = StructType([
        StructField("title", StringType(), True),
        StructField("vendor", StringType(), True)
    ])
    empty_df = spark.createDataFrame([], empty_schema)
    
    # Should handle empty dataframe gracefully
    cleaned_empty = clean_product_data(empty_df)
    assert cleaned_empty.count() == 0

def test_duplicate_removal(spark):
    """Test duplicate removal functionality"""
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("vendor", StringType(), True)
    ])
    
    duplicate_data = [
        ("Product A", "SKU1", "Vendor 1"),
        ("Product A", "SKU1", "Vendor 1"),  # Exact duplicate
        ("Product B", "SKU2", "Vendor 2"),
        ("Product A", "SKU3", "Vendor 1"),  # Same title, different SKU
    ]
    
    df = spark.createDataFrame(duplicate_data, schema)
    cleaned_df = clean_product_data(df)
    
    # Should remove exact duplicates but keep different SKUs
    assert cleaned_df.count() == 3

def test_price_cleaning(spark):
    """Test price data cleaning"""
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("price", DoubleType(), True)
    ])
    
    price_data = [
        ("Product 1", "SKU1", 100.0),
        ("Product 2", "SKU2", -50.0),  # Negative price
        ("Product 3", "SKU3", None),   # Null price
        ("Product 4", "SKU4", 0.0),    # Zero price
    ]
    
    df = spark.createDataFrame(price_data, schema)
    cleaned_df = clean_product_data(df)
    
    # Check that negative and null prices are converted to 0
    zero_prices = cleaned_df.filter(cleaned_df.price == 0.0).count()
    assert zero_prices == 3  # Negative, null, and original zero

def test_text_cleaning(spark):
    """Test text field cleaning"""
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("vendor", StringType(), True),
        StructField("product_type", StringType(), True)
    ])
    
    text_data = [
        ("  Product A  ", "  Vendor 1  ", "  Electronics  "),
        ("Product B", "", "Clothing"),
        ("", "Vendor 2", ""),
    ]
    
    df = spark.createDataFrame(text_data, schema)
    cleaned_df = clean_product_data(df)
    
    # Check that whitespace is trimmed
    trimmed_titles = cleaned_df.filter(cleaned_df.title == "Product A").count()
    assert trimmed_titles == 1
    
    # Check that empty fields are handled
    empty_vendors = cleaned_df.filter(cleaned_df.vendor == "Unknown Vendor").count()
    assert empty_vendors == 1
    
    empty_types = cleaned_df.filter(cleaned_df.product_type == "General").count()
    assert empty_types == 2

if __name__ == "__main__":
    pytest.main([__file__]) 