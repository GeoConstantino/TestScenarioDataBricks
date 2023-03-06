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

# MAGIC %md
# MAGIC #### Reviews
# MAGIC Contains full review text data including the user_id that wrote the review and the business_id the review is written for.

# COMMAND ----------

# Read the Yelp users reviews written for business data from the JSON file from Azure Data Lake: Yelp
# If an error occurs while reading the file, print an error message and log the error
try:
    df_yelp_academic_dataset_review = spark.read.json(
        "/mnt/yelp/bronze/yelp_academic_dataset_review.json"
    )
except Exception as e:
    logging.error(f"An error occurred while reading the file: {e}")
    raise

# Drop the column "funny" for not being considered relevant
user_review = df_yelp_academic_dataset_review.drop("funny")

# Rename column "Text" to not use reserved word
user_review = user_review.withColumnRenamed("text", "review_text")

# Write the resulting DataFrame to a Delta Lake table, partitioned by 'stars'
try:
    user_review.write.format("delta")\
                    .partitionBy("stars")\
                    .mode("overwrite")\
                    .option("mergeSchema", "true")\
                    .save("/mnt/yelp/silver/user_review")
    logging.info("Data has been successfully written to Delta Lake.")
except Exception as e:
    logging.error(f"An error occurred while writing the data: {e}")
    raise

# COMMAND ----------


