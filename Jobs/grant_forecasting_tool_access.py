# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Script 1 — Guglielmo (Sales) + Cascella (FE)
# Script 1: Guglielmo (Sales, 22 users) + Cascella (FE, 36 users) = 58 unique
# Run this cell to grant access to the Forecasting Tool dashboard

tables = [
    "home_luca_bolognesi.forecasting_tool.org_forecast",
    "home_luca_bolognesi.forecasting_tool.account_forecast",
    "home_luca_bolognesi.forecasting_tool.forecast_quarterly_summary",
]

users = [
    # Michele Guglielmo org (Sales)
    ("andrea.lupi@databricks.com",         "Andrea Lupi"),
    ("antonio.carrozzo@databricks.com",    "Antonio Carrozzo"),
    ("carlo.restelli@databricks.com",      "Carlo Restelli"),
    ("chiara.fumagalli@databricks.com",    "Chiara Fumagalli"),
    ("curzio.trezzani@databricks.com",     "Curzio Trezzani"),
    ("daniele.piacentini@databricks.com",  "Daniele Piacentini"),
    ("dario.mangani@databricks.com",       "Dario Mangani"),
    ("devrim.difinizio@databricks.com",    "Devrim Di Finizio"),
    ("eddy.amarouche@databricks.com",      "Eddy Amarouche"),
    ("fabio.caccia@databricks.com",        "Fabio Caccia"),
    ("fabio.gerosa@databricks.com",        "Fabio Gerosa"),
    ("fabrizio.amadio@databricks.com",     "Fabrizio Amadio"),
    ("francesco.vitti@databricks.com",     "Francesco Vitti"),
    ("giampaolo.pascali@databricks.com",   "Giampaolo Pascali"),
    ("guido.zanetti@databricks.com",       "Guido Zanetti"),
    ("lorenzo.tagliaferri@databricks.com", "Lorenzo Tagliaferri"),
    ("marco.bonetto@databricks.com",       "Marco Bonetto"),
    ("marialice.pasquini@databricks.com",  "Marialice Pasquini"),
    ("marina.stocchi@databricks.com",      "Marina Stocchi"),
    ("mauro.cenerelli@databricks.com",     "Mauro Cenerelli"),
    ("michele.guglielmo@databricks.com",   "Michele Guglielmo"),
    ("paolo.garzone@databricks.com",       "Paolo Garzone"),
    # Arduino Cascella org (FE)
    ("alessandro.gandini@databricks.com",       "Alessandro Gandini"),
    ("andrea.calegari@databricks.com",           "Andrea Calegari"),
    ("andrea.carubelli@databricks.com",          "Andrea Carubelli"),
    ("andrea.picasso@databricks.com",            "Andrea Picasso Ratto"),
    ("angel.zarramerairazabal@databricks.com",   "Angel Zarramera"),
    ("ariel.hdez@databricks.com",                "Ariel Hernandez Saa"),
    ("beatriz.cinos@databricks.com",             "Beatriz Martin Cinos"),
    ("caio.moreno@databricks.com",               "Caio Moreno"),
    ("cesar.cordoba@databricks.com",             "Cesar Restrepo"),
    ("christian.pascarella@databricks.com",      "Christian Pascarella"),
    ("daniel.entin@databricks.com",              "Daniel Entin"),
    ("davide.coccia@databricks.com",             "Davide Coccia"),
    ("davide.diblasi@databricks.com",            "Davide Di Blasi"),
    ("davide.veneziano@databricks.com",          "Davide Veneziano"),
    ("edoardo.schepis@databricks.com",           "Edoardo Schepis"),
    ("federico.rizzo@databricks.com",            "Federico Rizzo"),
    ("gianluca.vegetti@databricks.com",          "Gianluca Vegetti"),
    ("ignacio.arrieta@databricks.com",           "Ignacio Arrieta Perna"),
    ("jose.alfonso@databricks.com",              "Jose Alfonso"),
    ("julio.granados@databricks.com",            "Julio Granados"),
    ("laia.icardo@databricks.com",               "Laia Icardo"),
    ("luca.borin@databricks.com",                "Luca Borin"),
    ("luca.bolognesi@databricks.com",            "Luca Bolognesi"),
    ("lucas.ihnen@databricks.com",               "Lucas Ihnen"),
    ("lucas.romeo@databricks.com",               "Lucas Romeo"),
    ("mariajose.catullo@databricks.com",         "Maria Jose Catullo Gentilcore"),
    ("marino.aresi@databricks.com",              "Marino Aresi"),
    ("mattia.zeni@databricks.com",               "Mattia Zeni"),
    ("michele.lamarca@databricks.com",           "Michele Lamarca"),
    ("miguel.peralvo@databricks.com",            "Miguel Angel Peralvo Munoz"),
    ("mirco.meazzo@databricks.com",              "Mirco Meazzo"),
    ("miriana.mancini@databricks.com",           "Miriana Mancini"),
    ("nereida.aguera@databricks.com",            "Nereida Aguera Lopez"),
    ("pablo.monteagudo@databricks.com",          "Pablo Monteagudo"),
    ("pedro.algaba@databricks.com",              "Pedro Antonio Algaba Montes"),
    ("rafael.arana@databricks.com",              "Rafael Arana"),
    #MEA
    ("ariel.hadar@databricks.com",              "Ariel Adar"),
]

for email, name in users:
    try:
        spark.sql(f"GRANT USE CATALOG ON CATALOG home_luca_bolognesi TO `{email}`")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA home_luca_bolognesi.forecasting_tool TO `{email}`")
        for tbl in tables:
            spark.sql(f"GRANT SELECT ON TABLE {tbl} TO `{email}`")
        print(f"OK  {name} ({email})")
    except Exception as e:
        print(f"ERR {name} ({email}): {e}")

print(f"\nDone — {len(users)} users processed.")


# COMMAND ----------

# DBTITLE 1,Script 2 — Nicolas Maillard full org (Sales + FE)
# Script 2: Nicolas Maillard full org — 143 unique users (FE only, no Sales overlap)
# Pulled from main.gtm_silver.individual_hierarchy_field
# Run this cell to grant access to the Forecasting Tool dashboard

tables = [
    "home_luca_bolognesi.forecasting_tool.org_forecast",
    "home_luca_bolognesi.forecasting_tool.account_forecast",
    "home_luca_bolognesi.forecasting_tool.forecast_quarterly_summary",
]

maillard_users_df = spark.sql("""
    SELECT DISTINCT email, user_name
    FROM main.gtm_silver.individual_hierarchy_field
    WHERE email IS NOT NULL AND email NOT LIKE 'tbh%'
      AND (
        upper(manager_level_1_name) LIKE '%MAILLARD%'
        OR upper(manager_level_2_name) LIKE '%MAILLARD%'
        OR upper(manager_level_3_name) LIKE '%MAILLARD%'
        OR upper(manager_level_4_name) LIKE '%MAILLARD%'
        OR upper(manager_level_5_name) LIKE '%MAILLARD%'
        OR upper(manager_level_6_name) LIKE '%MAILLARD%'
      )
    ORDER BY user_name
""")

users = [(r.email, r.user_name) for r in maillard_users_df.collect()]
print(f"Found {len(users)} users under Nicolas Maillard")

for email, name in users:
    try:
        spark.sql(f"GRANT USE CATALOG ON CATALOG home_luca_bolognesi TO `{email}`")
        spark.sql(f"GRANT USE SCHEMA ON SCHEMA home_luca_bolognesi.forecasting_tool TO `{email}`")
        for tbl in tables:
            spark.sql(f"GRANT SELECT ON TABLE {tbl} TO `{email}`")
        print(f"OK  {name} ({email})")
    except Exception as e:
        print(f"ERR {name} ({email}): {e}")

print(f"\nDone — {len(users)} users processed.")


# COMMAND ----------

