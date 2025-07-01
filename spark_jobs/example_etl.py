import os
from dotenv import load_dotenv

# Try to import PySpark
try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
    from pyspark.sql.functions import col, when, lit, trim, lower
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    SparkDataFrame = None
    print("PySpark not available, using pandas fallback")

# Try to import pandas/numpy
try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Pandas not available")

# Load environment variables
load_dotenv()

def create_spark_session():
    """Create and configure Spark session"""
    if not PYSPARK_AVAILABLE:
        print("PySpark not available, using pandas fallback")
        return None
    try:
        return SparkSession.builder \
            .appName("ShopifyETL") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
    except Exception as e:
        print(f"PySpark session creation failed: {e}")
        print("Falling back to pandas for local processing")
        return None

def clean_product_data_pandas(df):
    if not PANDAS_AVAILABLE:
        raise ImportError("Pandas is not available for fallback ETL.")
    # Remove duplicates
    df_clean = df.drop_duplicates(subset=['title', 'sku'])
    # Clean text fields
    for colname in ['title', 'vendor', 'product_type', 'tags']:
        if colname in df_clean:
            df_clean[colname] = df_clean[colname].astype(str).str.strip()
    # Standardize vendor names
    if 'vendor' in df_clean:
        df_clean['vendor'] = df_clean['vendor'].replace('', 'Unknown Vendor').fillna('Unknown Vendor')
    # Standardize product types
    if 'product_type' in df_clean:
        df_clean['product_type'] = df_clean['product_type'].replace('', 'General').fillna('General')
    # Clean price data
    if 'price' in df_clean:
        df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce').fillna(0)
        df_clean.loc[df_clean['price'] <= 0, 'price'] = 0.0
    # Add SEO-friendly features column
    if 'description' in df_clean:
        df_clean['features'] = df_clean['description'].fillna('High-quality product with excellent features')
    else:
        df_clean['features'] = 'High-quality product with excellent features'
    return df_clean

def clean_product_data(df):
    """Clean and transform product data"""
    # PySpark DataFrame
    if PYSPARK_AVAILABLE and SparkDataFrame and isinstance(df, SparkDataFrame):
        df_clean = df.dropDuplicates(['title', 'sku'])
        df_clean = df_clean.withColumn("title", trim(col("title"))) \
            .withColumn("vendor", trim(col("vendor"))) \
            .withColumn("product_type", trim(col("product_type"))) \
            .withColumn("tags", trim(col("tags")))
        df_clean = df_clean.withColumn("vendor", 
            when(col("vendor").isNull() | (col("vendor") == ""), "Unknown Vendor")
            .otherwise(col("vendor")))
        df_clean = df_clean.withColumn("product_type",
            when(col("product_type").isNull() | (col("product_type") == ""), "General")
            .otherwise(col("product_type")))
        df_clean = df_clean.withColumn("price",
            when(col("price").isNull() | (col("price") <= 0), 0.0)
            .otherwise(col("price")))
        df_clean = df_clean.withColumn("features",
            when(col("description").isNotNull(), col("description"))
            .otherwise(lit("High-quality product with excellent features")))
        return df_clean
    # Pandas DataFrame
    elif PANDAS_AVAILABLE and isinstance(df, pd.DataFrame):
        return clean_product_data_pandas(df)
    else:
        raise ImportError("Input must be a PySpark or pandas DataFrame, and at least one must be available.")

def validate_data(df):
    """Validate data quality"""
    # PySpark DataFrame
    if PYSPARK_AVAILABLE and SparkDataFrame and isinstance(df, SparkDataFrame):
        total_rows = df.count()
        null_titles = df.filter(col("title").isNull() | (col("title") == "")).count()
        null_vendors = df.filter(col("vendor").isNull() | (col("vendor") == "")).count()
    # Pandas DataFrame
    elif PANDAS_AVAILABLE and isinstance(df, pd.DataFrame):
        total_rows = len(df)
        null_titles = df['title'].isna().sum() + (df['title'] == '').sum() if 'title' in df else 0
        null_vendors = df['vendor'].isna().sum() + (df['vendor'] == '').sum() if 'vendor' in df else 0
    else:
        raise ImportError("Input must be a PySpark or pandas DataFrame, and at least one must be available.")
    print(f"Data Quality Report:")
    print(f"Total products: {total_rows}")
    print(f"Products with missing titles: {null_titles}")
    print(f"Products with missing vendors: {null_vendors}")
    if null_titles > 0 or null_vendors > 0:
        print("Warning: Some products have missing required fields")
    return df

def main():
    """Main ETL function"""
    try:
        spark = create_spark_session()
        input_path = os.environ.get('INPUT_PATH', 'data/sample_products.csv')
        output_path = os.environ.get('OUTPUT_PATH', 'data/clean_products.parquet')
        s3_bucket = os.environ.get('S3_BUCKET_NAME', 'my-shopify-ai-project')
        s3_output_key = os.environ.get('S3_OUTPUT_KEY', 'data/clean_products.parquet')
        print(f"Starting ETL process...")
        print(f"Input: {input_path}")
        print(f"Output: {output_path}")
        # Read raw CSV data
        print("Reading input data...")
        if spark:
            df = spark.read.csv(input_path, header=True, inferSchema=True)
            print("Data schema:")
            df.printSchema()
            print("Sample data:")
            df.show(5)
        elif PANDAS_AVAILABLE:
            df = pd.read_csv(input_path)
            print("Data schema:")
            print(df.dtypes)
            print("Sample data:")
            print(df.head())
        else:
            raise ImportError("Neither PySpark nor pandas are available for ETL.")
        # Clean and transform data
        print("Cleaning and transforming data...")
        df_clean = clean_product_data(df)
        # Validate data
        print("Validating data quality...")
        df_clean = validate_data(df_clean)
        # Write to local Parquet file
        print(f"Writing to local Parquet: {output_path}")
        if spark:
            df_clean.write.mode("overwrite").parquet(output_path)
        elif PANDAS_AVAILABLE:
            df_clean.to_parquet(output_path, index=False)
        else:
            raise ImportError("Neither PySpark nor pandas are available for writing output.")
        # Write to S3 (if configured)
        if s3_bucket and s3_bucket != 'my-shopify-ai-project':
            s3_output_path = f"s3a://{s3_bucket}/{s3_output_key}"
            print(f"Writing to S3: {s3_output_path}")
            if spark:
                df_clean.write.mode("overwrite").parquet(s3_output_path)
            elif PANDAS_AVAILABLE:
                print("S3 upload with pandas requires boto3 - skipping for now")
        # Show final statistics
        if spark:
            final_count = df_clean.count()
            print("Sample of cleaned data:")
            df_clean.select("title", "vendor", "product_type", "price").show(5)
        elif PANDAS_AVAILABLE:
            final_count = len(df_clean)
            print("Sample of cleaned data:")
            print(df_clean[["title", "vendor", "product_type", "price"]].head())
        else:
            final_count = 0
        print(f"ETL completed successfully!")
        print(f"Final product count: {final_count}")
    except Exception as e:
        print(f"ETL process failed: {str(e)}")
        raise
    finally:
        if PYSPARK_AVAILABLE and 'spark' in locals() and spark:
            spark.stop()

if __name__ == "__main__":
    main() 