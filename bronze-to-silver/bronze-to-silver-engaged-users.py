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
# MAGIC #### Engaged Users
# MAGIC Every user that have: 
# MAGIC - More than 25 reviews, 
# MAGIC - With an avarage of given stars between 2.5 and 4 (including)
# MAGIC - Product of Review Count / Useful Review is less than 2

# COMMAND ----------

# Read the Yelp users data from the JSON file from Azure Data Lake: Yelp
# If an error occurs while reading the file, print an error message and log the error
try:
    df_yelp_academic_dataset_user = spark.read.json(
        "/mnt/yelp/bronze/yelp_academic_dataset_user.json"
    )
except Exception as e:
    logging.error(f"An error occurred while reading the file: {e}")
    raise

# Select the columns we consider relevant
engaged_users = df_yelp_academic_dataset_user[
    ["user_id", "name", "yelping_since", "review_count", "average_stars", "useful"]
]

# Filter the Yelp user based on criteria defined for Engaged Users
engaged_users = (
    engaged_users.filter(col("review_count") > 25)
    .filter(col("average_stars").between(2.49, 4.01))
    .filter(col("review_count") / col("useful") < 2)
)


# Write the resulting DataFrame to a Delta Lake table, partitioned by 'average_stars'
try:
    engaged_users.write.format("delta").partitionBy("average_stars").mode(
        "overwrite"
    ).save("/mnt/yelp/silver/engaged_users")

    logging.info("Data has been successfully written to Delta Lake.")
except Exception as e:
    logging.error(f"An error occurred while writing the data: {e}")
    raise

# COMMAND ----------


