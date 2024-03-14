# Databricks notebook source
# MAGIC %md
# MAGIC ##Primary and Foreign key

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE fact_sales(  
# MAGIC   transaction_id BIGINT PRIMARY KEY,
# MAGIC   date_id BIGINT NOT NULL CONSTRAINT dim_date_fk FOREIGN KEY REFERENCES dim_date,
# MAGIC   customer_id BIGINT NOT NULL CONSTRAINT dim_customer_fk FOREIGN KEY REFERENCES dim_customer,
# MAGIC   product_id BIGINT NOT NULL CONSTRAINT dim_product_fk FOREIGN KEY REFERENCES dim_product,
# MAGIC   store_id BIGINT NOT NULL CONSTRAINT dim_store_fk FOREIGN KEY REFERENCES dim_store,
# MAGIC   store_business_key STRING,
# MAGIC   sales_amount DOUBLE
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Surrogate Key

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customer dimension
# MAGIC CREATE OR REPLACE TABLE dim_customer(
# MAGIC   customer_id BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1) PRIMARY KEY,
# MAGIC   name STRING,
# MAGIC   email STRING,
# MAGIC   address STRING,
# MAGIC   created_date TIMESTAMP,
# MAGIC   updated_date TIMESTAMP,
# MAGIC   start_at TIMESTAMP,
# MAGIC   end_at TIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ##Adding Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add constraint to dim_store to make sure column store_id is between 1 and 9998
# MAGIC ALTER TABLE US_Stores.Sales_DW.dim_store ADD CONSTRAINT valid_store_id CHECK (store_id > 0 and store_id < 9999);
# MAGIC
# MAGIC -- Add constraint to fact_sales to make sure column sales_amount has a valid value
# MAGIC ALTER TABLE US_Stores.Sales_DW.fact_sales ADD CONSTRAINT valid_sales_amount CHECK (sales_amount > 0);

# COMMAND ----------

