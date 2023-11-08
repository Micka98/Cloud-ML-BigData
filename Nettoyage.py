# Databricks notebook source
df = spark.read.csv("/mnt/mntgroupe10/train.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import col, sum

# Compter le nombre de valeurs nulles par colonne
null_counts = df.select([sum(col(c).isNull().cast("int")).alias(c) for c in df.columns])

# Afficher le résultat
null_counts.show()


# COMMAND ----------

# Extraire les noms de colonnes et leurs comptages de valeurs nulles
null_counts = null_counts.collect()[0]

# Afficher les noms de colonnes avec les comptages de valeurs nulles
for column_name, null_count in null_counts.asDict().items():
    print(f"Colonne : {column_name}, Nombre de valeurs nulles : {null_count}")


# COMMAND ----------



# COMMAND ----------

# Filtrez les colonnes ayant moins de 400 valeurs nulles
selected_columns = [column_name for column_name, null_count in null_counts.asDict().items() if null_count < 200]

# Créez un nouveau DataFrame avec les colonnes sélectionnées
filtered_df = df.select(selected_columns)

# Affichez le schéma du nouveau DataFrame pour vérifier les colonnes sélectionnées
filtered_df.printSchema()

# Affichez les premières lignes du nouveau DataFrame
filtered_df.display()

