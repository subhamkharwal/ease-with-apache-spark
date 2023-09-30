from pyspark.sql.functions import col, regexp_replace

# Remove additional spaces in name
def remove_extra_spaces(df, column_name):
    df_transformed = df.withColumn(column_name, regexp_replace(col(column_name), "\\s+", " "))
    return df_transformed

# Filter Senior Citizen
def filter_senior_citizen(df, column_name):
    df_filtered = df.filter(col(column_name) >= 60)
    return df_filtered
