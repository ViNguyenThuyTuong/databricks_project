# End-to-End Data Platform Implementation
# Layers: Source → Ingestion → Bronze → Silver → Gold → Serving → ML → APIs

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta
import json

# ==============================================================================
# LAYER 1: SOURCE SYSTEMS SIMULATION
# ==============================================================================

class SourceSystems:
    """Simulate various source systems"""
    
    def __init__(self, spark):
        self.spark = spark
    
    def generate_customer_data(self, num_records=10000):
        """Generate sample customer data from CRM system"""
        
        customer_schema = StructType([
            StructField("customer_id", StringType(), False),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("phone", StringType(), True),
            StructField("registration_date", DateType(), True),
            StructField("country", StringType(), True),
            StructField("customer_segment", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("last_updated", TimestampType(), True)
        ])
        
        # Generate sample data
        from random import randint, choice
        countries = ['USA', 'UK', 'Canada', 'Germany', 'France']
        segments = ['Premium', 'Standard', 'Basic']
        
        data = [
            (
                f"CUST{str(i).zfill(6)}",
                f"FirstName{i}",
                f"LastName{i}",
                f"customer{i}@example.com",
                f"+1-555-{str(randint(1000000, 9999999))}",
                (datetime.now() - timedelta(days=randint(1, 1000))).date(),
                choice(countries),
                choice(segments),
                choice([True, False]),
                datetime.now()
            )
            for i in range(num_records)
        ]
        
        return self.spark.createDataFrame(data, customer_schema)
    
    def generate_transaction_data(self, num_records=50000):
        """Generate sample transaction data from payment system"""
        
        transaction_schema = StructType([
            StructField("transaction_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("transaction_date", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("category", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        from random import randint, choice, uniform
        payment_methods = ['CREDIT_CARD', 'DEBIT_CARD', 'PAYPAL', 'BANK_TRANSFER']
        categories = ['Retail', 'Food', 'Travel', 'Entertainment', 'Healthcare']
        statuses = ['COMPLETED', 'PENDING', 'FAILED', 'REFUNDED']
        
        data = [
            (
                f"TXN{str(i).zfill(8)}",
                f"CUST{str(randint(0, 9999)).zfill(6)}",
                datetime.now() - timedelta(hours=randint(0, 720)),
                round(uniform(10.0, 1000.0), 2),
                'USD',
                choice(payment_methods),
                f"MERCH{str(randint(1, 500)).zfill(4)}",
                choice(categories),
                choice(statuses)
            )
            for i in range(num_records)
        ]
        
        return self.spark.createDataFrame(data, transaction_schema)
    
    def generate_product_data(self, num_records=1000):
        """Generate sample product catalog data"""
        
        product_schema = StructType([
            StructField("product_id", StringType(), False),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("supplier_id", StringType(), True),
            StructField("created_date", DateType(), True)
        ])
        
        from random import randint, choice, uniform
        categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Home']
        
        data = [
            (
                f"PROD{str(i).zfill(5)}",
                f"Product Name {i}",
                choice(categories),
                round(uniform(5.0, 500.0), 2),
                randint(0, 1000),
                f"SUPP{str(randint(1, 50)).zfill(3)}",
                (datetime.now() - timedelta(days=randint(1, 365))).date()
            )
            for i in range(num_records)
        ]
        
        return self.spark.createDataFrame(data, product_schema)

# ==============================================================================
# LAYER 2: INGESTION LAYER
# ==============================================================================

class IngestionLayer:
    """Handle data ingestion from various sources"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path
        self.landing_path = f"{base_path}/landing"
    
    def ingest_batch_data(self, df: DataFrame, source_name: str, batch_id: str):
        """Ingest batch data to landing zone"""
        
        landing_path = f"{self.landing_path}/{source_name}/batch_id={batch_id}"
        
        # Add ingestion metadata
        df_with_metadata = df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("ingestion_batch_id", lit(batch_id)) \
            .withColumn("source_system", lit(source_name)) \
            .withColumn("ingestion_type", lit("BATCH"))
        
        # Write to landing zone as JSON
        df_with_metadata.write \
            .format("json") \
            .mode("overwrite") \
            .save(landing_path)
        
        print(f"✓ Ingested {df.count()} records from {source_name} to {landing_path}")
        return landing_path
    
    def ingest_streaming_data(self, stream_df: DataFrame, source_name: str):
        """Set up streaming ingestion"""
        
        checkpoint_path = f"{self.base_path}/checkpoints/ingestion/{source_name}"
        output_path = f"{self.landing_path}/{source_name}/streaming"
        
        # Add streaming metadata
        stream_with_metadata = stream_df \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_system", lit(source_name)) \
            .withColumn("ingestion_type", lit("STREAMING"))
        
        # Write stream to landing zone
        query = stream_with_metadata.writeStream \
            .format("json") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("path", output_path) \
            .start()
        
        print(f"✓ Started streaming ingestion for {source_name}")
        return query
    
    def read_from_api(self, api_endpoint: str, source_name: str):
        """Simulate API data ingestion"""
        
        # In production, use requests library to fetch from actual API
        # For demo, returning simulated data structure
        
        print(f"✓ Fetching data from API: {api_endpoint}")
        
        # Simulate API response
        api_data = {
            "status": "success",
            "timestamp": str(datetime.now()),
            "data": [],
            "metadata": {
                "source": source_name,
                "endpoint": api_endpoint
            }
        }
        
        return api_data

# ==============================================================================
# LAYER 3: BRONZE LAYER (RAW DATA LAKE)
# ==============================================================================

class BronzeLayer:
    """Bronze layer - Raw data ingestion with minimal transformation"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.bronze_path = f"{base_path}/bronze"
    
    def process_to_bronze(self, landing_path: str, table_name: str):
        """Process landing data to bronze layer"""
        
        bronze_table_path = f"{self.bronze_path}/{table_name}"
        
        # Read from landing zone
        df_raw = self.spark.read.format("json").load(landing_path)
        
        # Add bronze layer metadata
        df_bronze = df_raw \
            .withColumn("bronze_insert_timestamp", current_timestamp()) \
            .withColumn("bronze_file_path", input_file_name()) \
            .withColumn("bronze_processing_date", current_date())
        
        # Write to Delta Lake bronze layer
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(bronze_table_path)
        
        # Create table if not exists
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS bronze_{table_name}
            USING DELTA
            LOCATION '{bronze_table_path}'
        """)
        
        record_count = df_bronze.count()
        print(f"✓ Processed {record_count} records to bronze.{table_name}")
        
        return bronze_table_path
    
    def setup_autoloader(self, source_path: str, table_name: str):
        """Set up Auto Loader for continuous ingestion"""
        
        bronze_table_path = f"{self.bronze_path}/{table_name}"
        checkpoint_path = f"{self.bronze_path}/_checkpoints/{table_name}"
        
        df_stream = self.spark.readStream \
            .format("cloudFiles") \
            .option("cloudFiles.format", "json") \
            .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema") \
            .option("cloudFiles.inferColumnTypes", "true") \
            .load(source_path)
        
        # Add metadata
        df_with_metadata = df_stream \
            .withColumn("bronze_insert_timestamp", current_timestamp()) \
            .withColumn("bronze_file_path", input_file_name())
        
        # Write stream
        query = df_with_metadata.writeStream \
            .format("delta") \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_path) \
            .option("mergeSchema", "true") \
            .start(bronze_table_path)
        
        print(f"✓ Auto Loader started for bronze.{table_name}")
        return query

# ==============================================================================
# LAYER 4: SILVER LAYER (CLEANED & CONFORMED)
# ==============================================================================

class SilverLayer:
    """Silver layer - Cleaned, validated, and conformed data"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.silver_path = f"{base_path}/silver"
        self.quarantine_path = f"{base_path}/quarantine"
    
    def process_customers_to_silver(self, bronze_table_path: str):
        """Process customer data to silver layer with data quality checks"""
        
        silver_table_path = f"{self.silver_path}/customers"
        
        # Read from bronze
        df_bronze = self.spark.read.format("delta").load(bronze_table_path)
        
        # Data quality checks and transformations
        df_silver = df_bronze \
            .filter(col("customer_id").isNotNull()) \
            .filter(col("email").isNotNull()) \
            .filter(col("email").rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')) \
            .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name"))) \
            .withColumn("phone_cleaned", regexp_replace(col("phone"), r'[^\d]', '')) \
            .withColumn("email_domain", split(col("email"), "@").getItem(1)) \
            .withColumn("customer_age_days", 
                       datediff(current_date(), col("registration_date"))) \
            .withColumn("is_recent_customer", 
                       when(col("customer_age_days") <= 90, True).otherwise(False)) \
            .withColumn("data_quality_score", lit(1.0)) \
            .withColumn("silver_insert_timestamp", current_timestamp()) \
            .withColumn("silver_update_timestamp", current_timestamp()) \
            .dropDuplicates(["customer_id"])
        
        # Write to silver using merge for upserts
        if DeltaTable.isDeltaTable(self.spark, silver_table_path):
            silver_table = DeltaTable.forPath(self.spark, silver_table_path)
            
            silver_table.alias("target").merge(
                df_silver.alias("source"),
                "target.customer_id = source.customer_id"
            ).whenMatchedUpdate(set={
                "first_name": "source.first_name",
                "last_name": "source.last_name",
                "email": "source.email",
                "phone": "source.phone",
                "full_name": "source.full_name",
                "phone_cleaned": "source.phone_cleaned",
                "email_domain": "source.email_domain",
                "customer_segment": "source.customer_segment",
                "is_active": "source.is_active",
                "silver_update_timestamp": "source.silver_update_timestamp"
            }).whenNotMatchedInsert(values={
                "customer_id": "source.customer_id",
                "first_name": "source.first_name",
                "last_name": "source.last_name",
                "email": "source.email",
                "phone": "source.phone",
                "registration_date": "source.registration_date",
                "country": "source.country",
                "customer_segment": "source.customer_segment",
                "is_active": "source.is_active",
                "full_name": "source.full_name",
                "phone_cleaned": "source.phone_cleaned",
                "email_domain": "source.email_domain",
                "customer_age_days": "source.customer_age_days",
                "is_recent_customer": "source.is_recent_customer",
                "data_quality_score": "source.data_quality_score",
                "silver_insert_timestamp": "source.silver_insert_timestamp",
                "silver_update_timestamp": "source.silver_update_timestamp"
            }).execute()
        else:
            df_silver.write.format("delta").mode("overwrite").save(silver_table_path)
        
        # Create table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS silver_customers
            USING DELTA
            LOCATION '{silver_table_path}'
        """)
        
        print(f"✓ Processed customers to silver layer: {df_silver.count()} records")
        return silver_table_path
    
    def process_transactions_to_silver(self, bronze_table_path: str):
        """Process transaction data to silver layer"""
        
        silver_table_path = f"{self.silver_path}/transactions"
        
        df_bronze = self.spark.read.format("delta").load(bronze_table_path)
        
        # Clean and validate transactions
        df_silver = df_bronze \
            .filter(col("transaction_id").isNotNull()) \
            .filter(col("customer_id").isNotNull()) \
            .filter(col("amount") > 0) \
            .filter(col("status").isin(['COMPLETED', 'PENDING', 'REFUNDED'])) \
            .withColumn("transaction_date_only", to_date(col("transaction_date"))) \
            .withColumn("transaction_hour", hour(col("transaction_date"))) \
            .withColumn("transaction_day_of_week", dayofweek(col("transaction_date"))) \
            .withColumn("is_weekend", 
                       when(col("transaction_day_of_week").isin([1, 7]), True).otherwise(False)) \
            .withColumn("is_high_value", 
                       when(col("amount") > 500, True).otherwise(False)) \
            .withColumn("amount_category",
                       when(col("amount") < 50, "LOW")
                       .when((col("amount") >= 50) & (col("amount") < 200), "MEDIUM")
                       .when((col("amount") >= 200) & (col("amount") < 500), "HIGH")
                       .otherwise("VERY_HIGH")) \
            .withColumn("silver_insert_timestamp", current_timestamp()) \
            .dropDuplicates(["transaction_id"])
        
        # Write to silver
        df_silver.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("transaction_date_only") \
            .save(silver_table_path)
        
        # Create table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS silver_transactions
            USING DELTA
            LOCATION '{silver_table_path}'
        """)
        
        # Optimize
        self.spark.sql(f"OPTIMIZE delta.`{silver_table_path}` ZORDER BY (customer_id, transaction_date)")
        
        print(f"✓ Processed transactions to silver layer: {df_silver.count()} records")
        return silver_table_path
    
    def implement_data_quality_framework(self, table_path: str, table_name: str):
        """Comprehensive data quality checks"""
        
        df = self.spark.read.format("delta").load(table_path)
        
        quality_checks = {
            "total_records": df.count(),
            "null_checks": {},
            "duplicate_checks": {},
            "value_range_checks": {}
        }
        
        # Null checks
        for column in df.columns:
            null_count = df.filter(col(column).isNull()).count()
            quality_checks["null_checks"][column] = {
                "null_count": null_count,
                "null_percentage": (null_count / quality_checks["total_records"]) * 100
            }
        
        print(f"✓ Data quality checks completed for {table_name}")
        return quality_checks

# ==============================================================================
# LAYER 5: GOLD LAYER (BUSINESS AGGREGATIONS)
# ==============================================================================

class GoldLayer:
    """Gold layer - Business-ready aggregations and analytics"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.gold_path = f"{base_path}/gold"
    
    def create_customer_360_view(self, customers_path: str, transactions_path: str):
        """Create comprehensive customer 360 view"""
        
        gold_table_path = f"{self.gold_path}/customer_360"
        
        # Read silver tables
        df_customers = self.spark.read.format("delta").load(customers_path)
        df_transactions = self.spark.read.format("delta").load(transactions_path)
        
        # Calculate customer metrics
        customer_metrics = df_transactions \
            .filter(col("status") == "COMPLETED") \
            .groupBy("customer_id") \
            .agg(
                count("transaction_id").alias("total_transactions"),
                sum("amount").alias("lifetime_value"),
                avg("amount").alias("avg_transaction_amount"),
                max("amount").alias("max_transaction_amount"),
                min("transaction_date").alias("first_transaction_date"),
                max("transaction_date").alias("last_transaction_date"),
                countDistinct("category").alias("distinct_categories"),
                countDistinct("merchant_id").alias("distinct_merchants")
            ) \
            .withColumn("days_since_last_transaction", 
                       datediff(current_date(), col("last_transaction_date"))) \
            .withColumn("customer_tenure_days",
                       datediff(col("last_transaction_date"), col("first_transaction_date"))) \
            .withColumn("avg_monthly_spend",
                       col("lifetime_value") / (col("customer_tenure_days") / 30))
        
        # Join with customer data
        customer_360 = df_customers.alias("c").join(
            customer_metrics.alias("m"),
            col("c.customer_id") == col("m.customer_id"),
            "left"
        ).select(
            col("c.customer_id"),
            col("c.full_name"),
            col("c.email"),
            col("c.country"),
            col("c.customer_segment"),
            col("c.registration_date"),
            col("c.is_active"),
            coalesce(col("m.total_transactions"), lit(0)).alias("total_transactions"),
            coalesce(col("m.lifetime_value"), lit(0.0)).alias("lifetime_value"),
            coalesce(col("m.avg_transaction_amount"), lit(0.0)).alias("avg_transaction_amount"),
            col("m.first_transaction_date"),
            col("m.last_transaction_date"),
            col("m.days_since_last_transaction"),
            col("m.distinct_categories"),
            col("m.distinct_merchants"),
            col("m.avg_monthly_spend")
        ).withColumn("customer_value_tier",
            when(col("lifetime_value") > 10000, "PLATINUM")
            .when(col("lifetime_value") > 5000, "GOLD")
            .when(col("lifetime_value") > 1000, "SILVER")
            .otherwise("BRONZE")
        ).withColumn("churn_risk",
            when(col("days_since_last_transaction") > 90, "HIGH")
            .when(col("days_since_last_transaction") > 30, "MEDIUM")
            .otherwise("LOW")
        ).withColumn("gold_update_timestamp", current_timestamp())
        
        # Write to gold
        customer_360.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_table_path)
        
        # Create table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold_customer_360
            USING DELTA
            LOCATION '{gold_table_path}'
        """)
        
        print(f"✓ Created customer 360 view: {customer_360.count()} records")
        return gold_table_path
    
    def create_daily_sales_summary(self, transactions_path: str):
        """Create daily sales aggregations"""
        
        gold_table_path = f"{self.gold_path}/daily_sales_summary"
        
        df_transactions = self.spark.read.format("delta").load(transactions_path)
        
        daily_summary = df_transactions \
            .filter(col("status") == "COMPLETED") \
            .groupBy(
                col("transaction_date_only").alias("date"),
                col("category"),
                col("payment_method")
            ).agg(
                count("transaction_id").alias("transaction_count"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_transaction_value"),
                countDistinct("customer_id").alias("unique_customers"),
                countDistinct("merchant_id").alias("unique_merchants")
            ) \
            .withColumn("revenue_per_customer",
                       col("total_revenue") / col("unique_customers")) \
            .withColumn("gold_update_timestamp", current_timestamp())
        
        # Write partitioned by date
        daily_summary.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("date") \
            .save(gold_table_path)
        
        # Create table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold_daily_sales_summary
            USING DELTA
            LOCATION '{gold_table_path}'
        """)
        
        print(f"✓ Created daily sales summary: {daily_summary.count()} records")
        return gold_table_path
    
    def create_product_performance(self, transactions_path: str):
        """Create product performance metrics"""
        
        gold_table_path = f"{self.gold_path}/product_performance"
        
        df_transactions = self.spark.read.format("delta").load(transactions_path)
        
        # Calculate product metrics
        product_metrics = df_transactions \
            .filter(col("status") == "COMPLETED") \
            .groupBy("category") \
            .agg(
                count("transaction_id").alias("sales_count"),
                sum("amount").alias("total_revenue"),
                avg("amount").alias("avg_sale_price"),
                countDistinct("customer_id").alias("unique_buyers")
            ) \
            .withColumn("revenue_rank",
                       dense_rank().over(Window.orderBy(desc("total_revenue")))) \
            .withColumn("gold_update_timestamp", current_timestamp())
        
        # Write to gold
        product_metrics.write \
            .format("delta") \
            .mode("overwrite") \
            .save(gold_table_path)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS gold_product_performance
            USING DELTA
            LOCATION '{gold_table_path}'
        """)
        
        print(f"✓ Created product performance metrics")
        return gold_table_path

# ==============================================================================
# LAYER 6: SERVING LAYER
# ==============================================================================

class ServingLayer:
    """Serving layer - Optimized for consumption by BI tools and applications"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.serving_path = f"{base_path}/serving"
    
    def create_bi_views(self, gold_path: str):
        """Create optimized views for BI tools"""
        
        # Customer Analytics View
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW serving_customer_analytics AS
            SELECT 
                customer_id,
                full_name,
                email,
                country,
                customer_segment,
                customer_value_tier,
                total_transactions,
                lifetime_value,
                avg_transaction_amount,
                churn_risk,
                days_since_last_transaction,
                CASE 
                    WHEN total_transactions >= 50 THEN 'Power User'
                    WHEN total_transactions >= 20 THEN 'Regular User'
                    WHEN total_transactions >= 5 THEN 'Occasional User'
                    ELSE 'New User'
                END as user_type
            FROM delta.`{gold_path}/customer_360`
            WHERE is_active = true
        """)
        
        # Sales Dashboard View
        self.spark.sql(f"""
            CREATE OR REPLACE VIEW serving_sales_dashboard AS
            SELECT 
                date,
                category,
                payment_method,
                transaction_count,
                total_revenue,
                avg_transaction_value,
                unique_customers,
                revenue_per_customer,
                ROUND(total_revenue / SUM(total_revenue) OVER (PARTITION BY date) * 100, 2) as revenue_percentage
            FROM delta.`{gold_path}/daily_sales_summary`
            ORDER BY date DESC, total_revenue DESC
        """)
        
        print("✓ Created BI views for serving layer")
    
    def create_materialized_views(self, gold_customer_360_path: str):
        """Create materialized views for high-performance queries"""
        
        serving_table_path = f"{self.serving_path}/customer_summary_mv"
        
        # Read from gold and create aggregated view
        df_gold = self.spark.read.format("delta").load(gold_customer_360_path)
        
        materialized_view = df_gold \
            .groupBy("country", "customer_value_tier", "churn_risk") \
            .agg(
                count("customer_id").alias("customer_count"),
                sum("lifetime_value").alias("total_ltv"),
                avg("avg_transaction_amount").alias("avg_transaction"),
                avg("days_since_last_transaction").alias("avg_days_since_purchase")
            ) \
            .withColumn("last_refresh_timestamp", current_timestamp())
        
        # Write materialized view
        materialized_view.write \
            .format("delta") \
            .mode("overwrite") \
            .save(serving_table_path)
        
        # Create table
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS serving_customer_summary_mv
            USING DELTA
            LOCATION '{serving_table_path}'
        """)
        
        # Optimize for fast queries
        self.spark.sql(f"OPTIMIZE delta.`{serving_table_path}`")
        
        print("✓ Created materialized view for serving layer")
        return serving_table_path
    
    def create_real_time_dashboard_cache(self, gold_path: str):
        """Create cached tables for real-time dashboards"""
        
        # Cache key metrics
        self.spark.sql(f"""
            CREATE OR REPLACE TEMP VIEW realtime_metrics AS
            SELECT 
                COUNT(DISTINCT customer_id) as active_customers,
                SUM(lifetime_value) as total_revenue,
                AVG(lifetime_value) as avg_customer_value,
                COUNT(CASE WHEN churn_risk = 'HIGH' THEN 1 END) as high_risk_customers,
                COUNT(CASE WHEN customer_value_tier = 'PLATINUM' THEN 1 END) as platinum_customers
            FROM delta.`{gold_path}/customer_360`
            WHERE is_active = true
        """)
        
        metrics = self.spark.sql("SELECT * FROM realtime_metrics").collect()[0].asDict()
        
        print("✓ Real-time metrics cache created")
        return metrics

# ==============================================================================
# LAYER 7: MACHINE LEARNING LAYER
# ==============================================================================

class MLLayer:
    """Machine Learning feature engineering and model serving"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.ml_path = f"{base_path}/ml"
        self.feature_store_path = f"{self.ml_path}/feature_store"
        self.model_path = f"{self.ml_path}/models"
    
    def create_ml_features(self, customer_360_path: str, transactions_path: str):
        """Create ML features for customer churn prediction"""
        
        feature_table_path = f"{self.feature_store_path}/customer_churn_features"
        
        # Read data
        df_customers = self.spark.read.format("delta").load(customer_360_path)
        df_transactions = self.spark.read.format("delta").load(transactions_path)
        
        # Time-based features
        window_30d = Window.partitionBy("customer_id").orderBy("transaction_date").rangeBetween(-30 * 86400, 0)
        window_90d = Window.partitionBy("customer_id").orderBy("transaction_date").rangeBetween(-90 * 86400, 0)
        
        transaction_features = df_transactions \
            .filter(col("status") == "COMPLETED") \
            .withColumn("txn_count_30d", count("*").over(window_30d)) \
            .withColumn("txn_amount_30d", sum("amount").over(window_30d)) \
            .withColumn("txn_count_90d", count("*").over(window_90d)) \
            .withColumn("txn_amount_90d", sum("amount").over(window_90d)) \
            .groupBy("customer_id") \
            .agg(
                max("txn_count_30d").alias("transactions_last_30d"),
                max("txn_amount_30d").alias("spend_last_30d"),
                max("txn_count_90d").alias("transactions_last_90d"),
                max("txn_amount_90d").alias("spend_last_90d"),
                stddev("amount").alias("transaction_amount_stddev"),
                skewness("amount").alias("transaction_amount_skewness")
            )
        
        # Combine with customer features
        ml_features = df_customers.join(transaction_features, "customer_id", "left") \
            .select(
                "customer_id",
                "customer_segment",
                "country",
                "total_transactions",
                "lifetime_value",
                "avg_transaction_amount",
                "days_since_last_transaction",
                "distinct_categories",
                coalesce("transactions_last_30d", lit(0)).alias("transactions_last_30d"),
                coalesce("spend_last_30d", lit(0.0)).alias("spend_last_30d"),
                coalesce("transactions_last_90d", lit(0)).alias("transactions_last_90d"),
                coalesce("spend_last_90d", lit(0.0)).alias("spend_last_90d"),
                coalesce("transaction_amount_stddev", lit(0.0)).alias("transaction_amount_stddev"),
                # Target variable: churned if no transaction in 90 days
                when(col("days_since_last_transaction") > 90, 1).otherwise(0).alias("is_churned")
            ) \
            .withColumn("avg_daily_spend",
                       col("lifetime_value") / greatest(col("days_since_last_transaction"), lit(1))) \
            .withColumn("transaction_frequency",
                       col("total_transactions") / greatest(col("days_since_last_transaction"), lit(1))) \
            .withColumn("feature_timestamp", current_timestamp())
        
        # Write to feature store
        ml_features.write \
            .format("delta") \
            .mode("overwrite") \
            .save(feature_table_path)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS ml_customer_churn_features
            USING DELTA
            LOCATION '{feature_table_path}'
        """)
        
        print(f"✓ Created ML features for {ml_features.count()} customers")
        return feature_table_path
    
    def train_churn_model(self, feature_table_path: str):
        """Train churn prediction model"""
        
        from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
        from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        from pyspark.ml import Pipeline
        
        # Read features
        df_features = self.spark.read.format("delta").load(feature_table_path)
        
        # Prepare features
        feature_cols = [
            "total_transactions", "lifetime_value", "avg_transaction_amount",
            "days_since_last_transaction", "distinct_categories",
            "transactions_last_30d", "spend_last_30d",
            "transactions_last_90d", "spend_last_90d",
            "transaction_amount_stddev", "avg_daily_spend", "transaction_frequency"
        ]
        
        # Split data
        train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
        
        # Create pipeline
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features")
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="is_churned",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, rf])
        
        # Train model
        print("🔄 Training churn prediction model...")
        model = pipeline.fit(train_df)
        
        # Evaluate
        predictions = model.transform(test_df)
        evaluator = BinaryClassificationEvaluator(labelCol="is_churned", metricName="areaUnderROC")
        auc = evaluator.evaluate(predictions)
        
        print(f"✓ Model trained - AUC: {auc:.4f}")
        
        # Save model
        model_save_path = f"{self.model_path}/churn_prediction_model"
        model.write().overwrite().save(model_save_path)
        
        return model, auc
    
    def score_customers(self, model, feature_table_path: str):
        """Score all customers for churn risk"""
        
        scoring_path = f"{self.ml_path}/customer_scores"
        
        # Read features
        df_features = self.spark.read.format("delta").load(feature_table_path)
        
        # Score
        predictions = model.transform(df_features)
        
        # Extract predictions
        scored_customers = predictions.select(
            "customer_id",
            col("prediction").alias("churn_prediction"),
            col("probability").getItem(1).alias("churn_probability")
        ).withColumn("risk_score",
            when(col("churn_probability") > 0.7, "HIGH")
            .when(col("churn_probability") > 0.4, "MEDIUM")
            .otherwise("LOW")
        ).withColumn("scoring_timestamp", current_timestamp())
        
        # Save scores
        scored_customers.write \
            .format("delta") \
            .mode("overwrite") \
            .save(scoring_path)
        
        print(f"✓ Scored {scored_customers.count()} customers for churn risk")
        return scored_customers

# ==============================================================================
# LAYER 8: API SERVING LAYER
# ==============================================================================

class APILayer:
    """API endpoints for serving data and predictions"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path
    
    def get_customer_by_id(self, customer_id: str, customer_360_path: str):
        """API endpoint: Get customer details by ID"""
        
        df = self.spark.read.format("delta").load(customer_360_path)
        
        customer = df.filter(col("customer_id") == customer_id).collect()
        
        if len(customer) == 0:
            return {"error": "Customer not found", "customer_id": customer_id}
        
        customer_dict = customer[0].asDict()
        
        # Convert to JSON-serializable format
        result = {
            "customer_id": customer_dict["customer_id"],
            "full_name": customer_dict["full_name"],
            "email": customer_dict["email"],
            "country": customer_dict["country"],
            "segment": customer_dict["customer_segment"],
            "metrics": {
                "lifetime_value": float(customer_dict["lifetime_value"]) if customer_dict["lifetime_value"] else 0.0,
                "total_transactions": int(customer_dict["total_transactions"]) if customer_dict["total_transactions"] else 0,
                "avg_transaction": float(customer_dict["avg_transaction_amount"]) if customer_dict["avg_transaction_amount"] else 0.0
            },
            "status": {
                "is_active": customer_dict["is_active"],
                "value_tier": customer_dict["customer_value_tier"],
                "churn_risk": customer_dict["churn_risk"]
            }
        }
        
        return result
    
    def get_customer_recommendations(self, customer_id: str, transactions_path: str):
        """API endpoint: Get product recommendations for customer"""
        
        df_transactions = self.spark.read.format("delta").load(transactions_path)
        
        # Get customer's purchase history
        customer_categories = df_transactions \
            .filter(col("customer_id") == customer_id) \
            .filter(col("status") == "COMPLETED") \
            .groupBy("category") \
            .count() \
            .orderBy(desc("count")) \
            .limit(3) \
            .collect()
        
        top_categories = [row["category"] for row in customer_categories]
        
        # Get popular items in those categories from other customers
        recommendations = df_transactions \
            .filter(col("category").isin(top_categories)) \
            .filter(col("customer_id") != customer_id) \
            .groupBy("category") \
            .agg(
                count("*").alias("popularity"),
                avg("amount").alias("avg_price")
            ) \
            .orderBy(desc("popularity")) \
            .limit(5) \
            .collect()
        
        result = {
            "customer_id": customer_id,
            "recommendations": [
                {
                    "category": row["category"],
                    "popularity_score": int(row["popularity"]),
                    "avg_price": float(row["avg_price"])
                }
                for row in recommendations
            ]
        }
        
        return result
    
    def get_sales_metrics(self, date: str, daily_summary_path: str):
        """API endpoint: Get sales metrics for a specific date"""
        
        df = self.spark.read.format("delta").load(daily_summary_path)
        
        metrics = df.filter(col("date") == date) \
            .agg(
                sum("transaction_count").alias("total_transactions"),
                sum("total_revenue").alias("total_revenue"),
                avg("avg_transaction_value").alias("avg_order_value"),
                sum("unique_customers").alias("unique_customers")
            ) \
            .collect()
        
        if len(metrics) == 0:
            return {"error": "No data for date", "date": date}
        
        row = metrics[0]
        
        result = {
            "date": date,
            "metrics": {
                "total_transactions": int(row["total_transactions"]) if row["total_transactions"] else 0,
                "total_revenue": float(row["total_revenue"]) if row["total_revenue"] else 0.0,
                "avg_order_value": float(row["avg_order_value"]) if row["avg_order_value"] else 0.0,
                "unique_customers": int(row["unique_customers"]) if row["unique_customers"] else 0
            }
        }
        
        return result
    
    def get_churn_predictions(self, ml_scores_path: str, risk_level: str = "HIGH"):
        """API endpoint: Get customers at risk of churn"""
        
        df = self.spark.read.format("delta").load(ml_scores_path)
        
        at_risk_customers = df \
            .filter(col("risk_score") == risk_level) \
            .orderBy(desc("churn_probability")) \
            .limit(100) \
            .collect()
        
        result = {
            "risk_level": risk_level,
            "count": len(at_risk_customers),
            "customers": [
                {
                    "customer_id": row["customer_id"],
                    "churn_probability": float(row["churn_probability"]),
                    "risk_score": row["risk_score"]
                }
                for row in at_risk_customers
            ]
        }
        
        return result

# ==============================================================================
# ORCHESTRATION: END-TO-END PIPELINE
# ==============================================================================

class DataPlatformOrchestrator:
    """Orchestrate the entire data platform pipeline"""
    
    def __init__(self, spark, base_path):
        self.spark = spark
        self.base_path = base_path
        
        # Initialize all layers
        self.source = SourceSystems(spark)
        self.ingestion = IngestionLayer(spark, base_path)
        self.bronze = BronzeLayer(spark, base_path)
        self.silver = SilverLayer(spark, base_path)
        self.gold = GoldLayer(spark, base_path)
        self.serving = ServingLayer(spark, base_path)
        self.ml = MLLayer(spark, base_path)
        self.api = APILayer(spark, base_path)
    
    def run_full_pipeline(self):
        """Execute the complete end-to-end pipeline"""
        
        print("=" * 80)
        print(" STARTING END-TO-END DATA PLATFORM PIPELINE")
        print("=" * 80)
        
        # Step 1: Generate source data
        print("\n STEP 1: GENERATING SOURCE DATA")
        df_customers = self.source.generate_customer_data(10000)
        df_transactions = self.source.generate_transaction_data(50000)
        df_products = self.source.generate_product_data(1000)
        
        # Step 2: Ingestion
        print("\n STEP 2: INGESTION LAYER")
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        customer_landing = self.ingestion.ingest_batch_data(df_customers, "crm_customers", batch_id)
        transaction_landing = self.ingestion.ingest_batch_data(df_transactions, "payment_transactions", batch_id)
        product_landing = self.ingestion.ingest_batch_data(df_products, "product_catalog", batch_id)
        
        # Step 3: Bronze layer
        print("\n STEP 3: BRONZE LAYER (RAW)")
        customer_bronze = self.bronze.process_to_bronze(customer_landing, "customers")
        transaction_bronze = self.bronze.process_to_bronze(transaction_landing, "transactions")
        product_bronze = self.bronze.process_to_bronze(product_landing, "products")
        
        # Step 4: Silver layer
        print("\n STEP 4: SILVER LAYER (CLEANED)")
        customer_silver = self.silver.process_customers_to_silver(customer_bronze)
        transaction_silver = self.silver.process_transactions_to_silver(transaction_bronze)
        
        # Data quality checks
        self.silver.implement_data_quality_framework(customer_silver, "customers")
        
        # Step 5: Gold layer
        print("\n STEP 5: GOLD LAYER (AGGREGATED)")
        customer_360 = self.gold.create_customer_360_view(customer_silver, transaction_silver)
        daily_sales = self.gold.create_daily_sales_summary(transaction_silver)
        product_perf = self.gold.create_product_performance(transaction_silver)
        
        # Step 6: Serving layer
        print("\n STEP 6: SERVING LAYER")
        self.serving.create_bi_views(self.gold.gold_path)
        self.serving.create_materialized_views(customer_360)
        metrics = self.serving.create_real_time_dashboard_cache(self.gold.gold_path)
        print(f"   Real-time metrics: {metrics}")
        
        # Step 7: ML layer
        print("\n STEP 7: MACHINE LEARNING LAYER")
        feature_table = self.ml.create_ml_features(customer_360, transaction_silver)
        model, auc = self.ml.train_churn_model(feature_table)
        scored_customers = self.ml.score_customers(model, feature_table)
        
        # Step 8: API examples
        print("\n🔌 STEP 8: API LAYER - SAMPLE RESPONSES")
        
        # Get a sample customer
        sample_customer = df_customers.select("customer_id").first()["customer_id"]
        
        customer_api_response = self.api.get_customer_by_id(sample_customer, customer_360)
        print(f"   Customer API: {json.dumps(customer_api_response, indent=2)}")
        
        recommendations = self.api.get_customer_recommendations(sample_customer, transaction_silver)
        print(f"   Recommendations: {json.dumps(recommendations, indent=2)}")
        
        # Pipeline summary
        print("\n" + "=" * 80)
        print("PIPELINE EXECUTION COMPLETED SUCCESSFULLY")
        print("=" * 80)
        print(f"\n Pipeline Summary:")
        print(f"   Customers processed: {df_customers.count():,}")
        print(f"   Transactions processed: {df_transactions.count():,}")
        print(f"   Products processed: {df_products.count():,}")
        print(f"   ML Model AUC: {auc:.4f}")
        print(f"   Churn predictions generated: {scored_customers.count():,}")
        print(f"\n Data stored in: {self.base_path}")
        
        return {
            "status": "success",
            "layers": {
                "bronze": [customer_bronze, transaction_bronze, product_bronze],
                "silver": [customer_silver, transaction_silver],
                "gold": [customer_360, daily_sales, product_perf],
                "ml": [feature_table],
            },
            "model_auc": auc,
            "metrics": metrics
        }

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    
    # Initialize Spark Session
    spark = SparkSession.builder \
        .appName("End-to-End Data Platform") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.optimizeWrite.enabled", "true") \
        .config("spark.databricks.delta.autoCompact.enabled", "true") \
        .getOrCreate()
    
    # Set base path (adjust for your environment)
    base_path = "/tmp/data_platform"  # Change to your storage path
    # For Azure: base_path = "abfss://container@storage.dfs.core.windows.net/data_platform"
    # For AWS: base_path = "s3://bucket/data_platform"
    
    # Create orchestrator and run pipeline
    orchestrator = DataPlatformOrchestrator(spark, base_path)
    result = orchestrator.run_full_pipeline()
    
    print("\n Data platform is ready for use!")
    print("=" * 80)
