# Databricks notebook source
import logging
from pyspark.sql.functions import col

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

df_top_rated_business = spark.read.format('delta').load("/mnt/yelp/silver/top_rated_business")
df_engaged_users = spark.read.format('delta').load("/mnt/yelp/silver/engaged_users")
df_user_review = spark.read.format('delta').load("/mnt/yelp/silver/user_review")

# COMMAND ----------

df_engaged_users.createOrReplaceTempView("engaged_users")
df_top_rated_business.createOrReplaceTempView("top_rated_business")
df_user_review.createOrReplaceTempView('user_review')

# COMMAND ----------

# Create a temporary view of 5-star reviews from engaged users
try:
    spark.sql(
        """
        SELECT user_review.business_id,
                engaged_users.name,
                engaged_users.average_stars,
                engaged_users.useful,
                user_review.review_text,
                user_review.review_id,
                user_review.date
        FROM user_review
        JOIN engaged_users
        USING (user_id)
        WHERE user_review.stars = 5
        """
    ).createOrReplaceTempView("engaged_users_reviews")

    # Cache the temporary view in memory
    spark.catalog.cacheTable("engaged_users_reviews")  
except Exception as e:
    # Log the exception message and traceback
    logging.error(f'Error creating or caching temporary view: {e}')
    

# COMMAND ----------

# Create a Delta table from the temporary view
try:
    spark.sql("""
        CREATE OR REPLACE TABLE top_rated_business_reviewed_by_engaged_users_delta
        USING delta
        OPTIONS ('path' '/mnt/yelp/gold/top_rated_business_reviewed_by_engaged_users_delta')
        AS
        SELECT top_rated_business.business_id,
                top_rated_business.name, 
                top_rated_business.city, 
                top_rated_business.state, 
                top_rated_business.stars
        FROM top_rated_business
        WHERE top_rated_business.business_id IN (
            SELECT DISTINCT(business_id)
            FROM engaged_users_reviews
        )
    """)
except Exception as e:
    # Log the exception message and traceback
    logging.error(f'Error creating or caching Table: {e}')

# COMMAND ----------


