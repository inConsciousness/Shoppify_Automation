import boto3
import openai
import deepseek
import shopify
import pyarrow.parquet as pq
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def lambda_handler(event, context):
    """
    AWS Lambda handler for processing products with AI-generated descriptions
    """
    try:
        # Initialize AWS S3 client
        s3 = boto3.client('s3')
        
        # Configuration from environment variables
        bucket_name = os.environ.get('S3_BUCKET_NAME', 'my-shopify-ai-project')
        parquet_key = os.environ.get('PARQUET_KEY', 'data/clean_products.parquet')
        openai_api_key = os.environ.get('OPENAI_API_KEY')
        deepseek_api_key = os.environ.get('DEEPSEEK_API_KEY')
        
        # Initialize Shopify API
        shopify.ShopifyResource.set_site(os.environ.get('SHOPIFY_API_URL'))
        shopify.ShopifyResource.set_headers({'X-Shopify-Access-Token': os.environ.get('SHOPIFY_API_KEY')})
        
        # Download Parquet file from S3
        local_parquet_path = '/tmp/clean_products.parquet'
        s3.download_file(bucket_name, parquet_key, local_parquet_path)
        
        # Read Parquet file
        table = pq.read_table(local_parquet_path)
        products = table.to_pylist()
        
        processed_count = 0
        
        for product in products:
            try:
                # Generate SEO description with OpenAI (fallback to DeepSeek)
                description = generate_seo_description(product, openai_api_key, deepseek_api_key)
                
                # Create product in Shopify
                shopify_product = shopify.Product()
                shopify_product.title = product.get('title', 'Untitled Product')
                shopify_product.body_html = description
                shopify_product.vendor = product.get('vendor', 'Default Vendor')
                shopify_product.product_type = product.get('product_type', 'General')
                shopify_product.tags = product.get('tags', '')
                shopify_product.published = True
                
                # Set price if available
                if 'price' in product:
                    variant = shopify.Variant()
                    variant.price = str(product['price'])
                    shopify_product.variants = [variant]
                
                # Save product to Shopify
                if shopify_product.save():
                    processed_count += 1
                    print(f"Successfully created product: {shopify_product.title}")
                else:
                    print(f"Failed to create product: {shopify_product.title}")
                    
            except Exception as e:
                print(f"Error processing product {product.get('title', 'Unknown')}: {str(e)}")
                continue
        
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Successfully processed {processed_count} products",
                "total_products": len(products),
                "processed_count": processed_count
            })
        }
        
    except Exception as e:
        print(f"Lambda execution error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({
                "error": str(e)
            })
        }

def generate_seo_description(product, openai_api_key, deepseek_api_key):
    """
    Generate SEO-optimized description using OpenAI or DeepSeek as fallback
    """
    product_info = f"""
    Product: {product.get('title', 'Unknown Product')}
    Category: {product.get('product_type', 'General')}
    Vendor: {product.get('vendor', 'Unknown Vendor')}
    Features: {product.get('features', 'N/A')}
    """
    
    prompt = f"""
    Create a compelling, SEO-optimized product description for the following product:
    {product_info}
    
    The description should be:
    - 150-200 words
    - Include relevant keywords naturally
    - Highlight benefits and features
    - Be engaging and conversion-focused
    - Include a call-to-action
    """
    
    # Try OpenAI first
    if openai_api_key:
        try:
            openai.api_key = openai_api_key
            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a professional e-commerce copywriter specializing in SEO-optimized product descriptions."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.7
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"OpenAI API error: {str(e)}")
    
    # Fallback to DeepSeek
    if deepseek_api_key:
        try:
            deepseek.api_key = deepseek_api_key
            response = deepseek.ChatCompletion.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": "You are a professional e-commerce copywriter specializing in SEO-optimized product descriptions."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=300,
                temperature=0.7
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            print(f"DeepSeek API error: {str(e)}")
    
    # Default description if both APIs fail
    return f"""
    Discover the amazing {product.get('title', 'product')} from {product.get('vendor', 'our trusted vendor')}. 
    This high-quality item offers exceptional value and performance. Perfect for your needs, 
    this product combines style, functionality, and reliability. Don't miss out on this 
    incredible opportunity - order yours today and experience the difference!
    """ 