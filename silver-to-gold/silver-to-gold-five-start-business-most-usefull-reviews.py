# Databricks notebook source
import logging

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number

# COMMAND ----------

# Mount the Azure Yelp DataLake into Databricks environment as "/mnt/yelp"
try:
    # Retrieve secrets from Azure KeyVault, connected to the scope "adls-scope"
    application_id = dbutils.secrets.get(scope="adls-scope", key="application-id")
    tenant_id = dbutils.secrets.get(scope="adls-scope", key="tenant-id")
    secret = dbutils.secrets.get(scope="adls-scope", key="service-credential-adsl")

    # Define information of the mounting point
    container_name = "yelp"
    mount_point = "/mnt/yelp"
    storage_account = "eraneosdatalake"

    # Define the configuration options for mounting the container
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": application_id,
        "fs.azure.account.oauth2.client.secret": secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    # Get a list of all currently mounted directories
    mount_points = [mount.mountPoint for mount in dbutils.fs.mounts()]

    # Check if the specified mount point is already in the list of mount points
    if mount_point in mount_points:
        # If the mount point is already mounted, print a message indicating that
        print(f"{mount_point} is already mounted.")
    else:
        # If the mount point is not mounted, mount it using the specified source, mount point, and extra configurations
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/",
            mount_point=mount_point,
            extra_configs=configs,
        )
except Exception as e:
    # If an error occurs while retrieving secrets or mounting the directory, print an error message
    print("An error occurred while retrieving secrets or mounting the directory.")
    print(str(e))

# COMMAND ----------

# Load data from Delta tables
try:
    df_top_rated_business = spark.read\
                .format("delta")\
                .load("/mnt/yelp/silver/top_rated_business")
    df_engaged_users = spark.read\
                .format("delta")\
                .load("/mnt/yelp/silver/engaged_users")
    df_user_review = spark.read.\
                format("delta")\
                .load("/mnt/yelp/silver/user_review")
except Exception as e:
    logging.error(f"An error occurred while loading the data: {e}")
    raise

# COMMAND ----------

# Join DataFrames and filter for 5-star reviews
df_engaged_users_reviews = df_user_review.join(
    df_engaged_users,
    df_engaged_users.user_id.eqNullSafe(df_user_review.user_id)
).filter(col("stars") == 5).select(
    df_user_review.business_id,
    df_engaged_users.name.alias("username"),
    df_engaged_users.average_stars.alias("user_average_stars"),
    df_engaged_users.useful,
    df_user_review.review_text,
    df_user_review.review_id,
    df_user_review.date.alias("review_date")
)

# COMMAND ----------

# Apply window function to identify the most useful review for each business
windowReview = Window.partitionBy("business_id").orderBy(col("useful").desc())
max_useful_reviews = (
    df_engaged_users_reviews.withColumn("row", row_number().over(windowReview))
    .filter(col("row") == 1)
    .drop("row")
)

# Create table useful_reviews_five_stars_business joining top rated business
# with most useful reviews
df_max_rated_business = df_top_rated_business.join(
    max_useful_reviews, on="business_id", how="left"
)
df_max_rated_business = df_max_rated_business.na.drop("any")

# COMMAND ----------

# Write the resulting DataFrame to a Unmanaged Delta Lake table, partitioned by 'state'
try:
    df_max_rated_business.write\
            .format("delta")\
            .partitionBy("state")\
            .mode("overwrite")\
            .option("mergeSchema", "true")\
            .save("/mnt/yelp/gold/useful_reviews_five_stars_business")

    logging.info("Data has been successfully written to Delta Lake.")
except Exception as e:
    logging.error(f"An error occurred while writing the data: {e}")
    raise
