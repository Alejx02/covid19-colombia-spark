import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F

# ---------------- Configuración ----------------
RUTA_CSV = os.path.expanduser("~/proyecto_batch/data/covid_colombia.csv")
RUTA_PARQUET = os.path.expanduser("~/proyecto_batch/data/covid_limpio.parquet")
SALIDA = os.path.expanduser("~/proyecto_batch/output")
FRACCION = float(os.environ.get("FRACCION", "1.0"))  # 1.0 = todos los datos
os.makedirs(SALIDA, exist_ok=True)

spark = (SparkSession.builder
         .appName("COVID19_Colombia_Batch")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "2g")
         .config("spark.driver.bindAddress", "0.0.0.0")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")


def guardar(fig, nombre):
    ruta = os.path.join(SALIDA, nombre)
    fig.tight_layout()
    fig.savefig(ruta, dpi=120)
    plt.close(fig)
    print("Gráfica guardada:", ruta)


# ---------------- 1. Carga desde la fuente original ----------------
print("\n===== 1. CARGA DEL DATASET =====")
raw = (spark.read
       .option("header", True)
       .option("encoding", "UTF-8")
       .csv(RUTA_CSV))

nombres = ["fecha_reporte", "id_caso", "fecha_notificacion", "cod_depto",
           "departamento", "cod_municipio", "municipio", "edad", "unidad_edad",
           "sexo", "tipo_contagio", "ubicacion", "estado", "cod_pais", "pais",
           "recuperado", "fecha_inicio_sintomas", "fecha_muerte",
           "fecha_diagnostico", "fecha_recuperacion", "tipo_recuperacion",
           "pertenencia_etnica", "grupo_etnico"]
assert len(raw.columns) == len(nombres), "El CSV no tiene 23 columnas"
raw = raw.toDF(*nombres)

if FRACCION < 1.0:
    raw = raw.sample(False, FRACCION, seed=42)
    print("MODO PRUEBA: usando una muestra de", FRACCION)

filas_originales = raw.count()
print("Filas originales:", filas_originales)
print("Columnas:", len(raw.columns))
raw.printSchema()
print("Unidades de medida de edad (1=años, 2=meses, 3=días):")
raw.groupBy("unidad_edad").count().orderBy("unidad_edad").show()

# ---------------- 2. Limpieza y transformación ----------------
print("\n===== 2. LIMPIEZA Y TRANSFORMACIÓN =====")


def a_fecha(nombre_col):
    return F.to_date(F.to_timestamp(F.col(nombre_col), "yyyy-MM-dd HH:mm:ss"))


df = (raw
      .withColumn("edad", F.col("edad").cast("int"))
      .withColumn("unidad_edad", F.col("unidad_edad").cast("int"))
      .withColumn("edad_anios",
                  F.when(F.col("unidad_edad") == 1, F.col("edad"))
                   .when(F.col("unidad_edad").isin(2, 3), F.lit(0))
                   .otherwise(F.lit(None).cast("int")))
      .withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
      .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
      .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
      .withColumn("estado", F.upper(F.trim(F.col("estado"))))
      .withColumn("recuperado", F.upper(F.trim(F.col("recuperado"))))
      .withColumn("tipo_contagio", F.trim(F.col("tipo_contagio")))
      .withColumn("ubicacion", F.trim(F.col("ubicacion")))
      .withColumn("fecha_notificacion", a_fecha("fecha_notificacion"))
      .withColumn("fecha_inicio_sintomas", a_fecha("fecha_inicio_sintomas"))
      .withColumn("fecha_muerte", a_fecha("fecha_muerte")))

df = df.withColumn(
    "fallecido",
    F.when(F.col("fecha_muerte").isNotNull()
           | (F.col("recuperado") == "FALLECIDO")
           | (F.col("estado") == "FALLECIDO"), 1).otherwise(0))

base_decada = (F.floor(F.col("edad_anios") / 10) * 10).cast("int")
df = (df
      .withColumn("grupo_edad",
                  F.when(F.col("edad_anios") >= 80, F.lit("80+"))
                   .otherwise(F.concat(base_decada.cast("string"), F.lit("-"),
                                       (base_decada + 9).cast("string"))))
      .withColumn("anio_mes", F.date_format(F.col("fecha_notificacion"), "yyyy-MM")))

# Depuración: duplicados, edades imposibles, sexo inválido, datos clave vacíos
df = (df.filter(F.col("id_caso").isNotNull())
        .dropDuplicates(["id_caso"])
        .filter(F.col("edad_anios").between(0, 110))
        .filter(F.col("sexo").isin("F", "M"))
        .filter(F.col("departamento").isNotNull() & (F.col("departamento") != ""))
        .filter(F.col("fecha_notificacion").isNotNull()))

columnas_finales = ["id_caso", "fecha_notificacion", "departamento", "municipio",
                    "edad_anios", "grupo_edad", "sexo", "tipo_contagio",
                    "ubicacion", "estado", "recuperado", "fecha_inicio_sintomas",
                    "fecha_muerte", "fallecido", "anio_mes"]
df.select(*columnas_finales).write.mode("overwrite").parquet(RUTA_PARQUET)
print("Dataset limpio guardado en formato Parquet:", RUTA_PARQUET)

# ---------------- 3. Análisis exploratorio (EDA) ----------------
limpio = spark.read.parquet(RUTA_PARQUET)
total = limpio.count()
print("\n===== 3. ANÁLISIS EXPLORATORIO =====")
print("Filas originales :", filas_originales)
print("Filas después de limpiar:", total)
print("Filas descartadas:", filas_originales - total)

print("\n--- Estadísticas de la edad (años) ---")
limpio.select("edad_anios").describe().show()

print("--- Valores nulos por columna ---")
limpio.select([F.sum(F.col(c).isNull().cast("int")).alias(c)
               for c in limpio.columns]).show(vertical=True)

casos_depto = (limpio.groupBy("departamento")
               .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
               .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
               .orderBy(F.desc("casos")))
print("--- Casos por departamento (Top 15) ---")
casos_depto.show(15, truncate=False)

casos_sexo = limpio.groupBy("sexo").agg(F.count("*").alias("casos"))
print("--- Casos por sexo ---")
casos_sexo.show()

por_edad = (limpio.groupBy("grupo_edad")
            .agg(F.count("*").alias("casos"), F.sum("fallecido").alias("fallecidos"))
            .withColumn("letalidad_pct", F.round(F.col("fallecidos") / F.col("casos") * 100, 2))
            .orderBy("grupo_edad"))
print("--- Casos y letalidad por grupo de edad ---")
por_edad.show(truncate=False)

por_mes = limpio.groupBy("anio_mes").agg(F.count("*").alias("casos")).orderBy("anio_mes")
print("--- Casos por mes de notificación ---")
por_mes.show(100, truncate=False)

por_estado = limpio.groupBy("estado").agg(F.count("*").alias("casos")).orderBy(F.desc("casos"))
print("--- Casos por gravedad (estado) ---")
por_estado.show()

top_municipios = (limpio.groupBy("municipio")
                  .agg(F.count("*").alias("casos"))
                  .orderBy(F.desc("casos")))
print("--- Top 10 municipios ---")
top_municipios.show(10, truncate=False)

# ---------------- 4. Visualización ----------------
print("\n===== 4. VISUALIZACIÓN =====")

pdf = casos_depto.limit(15).toPandas().sort_values("casos")
fig, ax = plt.subplots(figsize=(9, 6))
ax.barh(pdf["departamento"], pdf["casos"], color="#1f77b4")
ax.set_title("Casos de COVID-19 por departamento (Top 15)")
ax.set_xlabel("Número de casos")
guardar(fig, "01_casos_por_departamento.png")

pdf = casos_sexo.toPandas()
fig, ax = plt.subplots(figsize=(5, 5))
ax.pie(pdf["casos"], labels=pdf["sexo"], autopct="%1.1f%%", startangle=90)
ax.set_title("Casos de COVID-19 por sexo")
guardar(fig, "02_casos_por_sexo.png")

pdf = por_edad.toPandas()
pdf = pdf.sort_values("grupo_edad", key=lambda s: s.str.extract(r"(\d+)")[0].astype(int))
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
ax1.bar(pdf["grupo_edad"], pdf["casos"], color="#1f77b4")
ax1.set_title("Casos por grupo de edad")
ax1.set_xlabel("Grupo de edad (años)")
ax1.set_ylabel("Casos")
ax2.bar(pdf["grupo_edad"], pdf["letalidad_pct"], color="#d62728")
ax2.set_title("Letalidad por grupo de edad (%)")
ax2.set_xlabel("Grupo de edad (años)")
ax2.set_ylabel("Fallecidos / casos (%)")
guardar(fig, "03_edad_casos_y_letalidad.png")

pdf = por_mes.toPandas().sort_values("anio_mes")
fig, ax = plt.subplots(figsize=(11, 5))
ax.plot(pdf["anio_mes"], pdf["casos"], color="#2ca02c")
ax.set_xticks(range(0, len(pdf), 3))
ax.set_xticklabels(pdf["anio_mes"].iloc[::3], rotation=60)
ax.set_title("Casos de COVID-19 por mes de notificación")
ax.set_ylabel("Casos")
guardar(fig, "04_casos_por_mes.png")

pdf = por_estado.toPandas()
fig, ax = plt.subplots(figsize=(7, 5))
ax.bar(pdf["estado"], pdf["casos"], color="#9467bd")
ax.set_title("Casos por gravedad")
ax.set_ylabel("Casos")
guardar(fig, "05_casos_por_gravedad.png")

pdf = top_municipios.limit(10).toPandas().sort_values("casos")
fig, ax = plt.subplots(figsize=(9, 5))
ax.barh(pdf["municipio"], pdf["casos"], color="#ff7f0e")
ax.set_title("Top 10 municipios con más casos")
ax.set_xlabel("Número de casos")
guardar(fig, "06_top_municipios.png")

print("\nProceso terminado. Gráficas en:", SALIDA)
spark.stop()
