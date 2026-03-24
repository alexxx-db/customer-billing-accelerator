# Databricks notebook source
# MAGIC %md
# MAGIC # Lakehouse Federation Setup
# MAGIC
# MAGIC Sets up Unity Catalog infrastructure for external data integration.
# MAGIC
# MAGIC **Track A** (real federation): Registers a UC connection to an external ERP database
# MAGIC and creates a foreign catalog for query pushdown.
# MAGIC
# MAGIC **Track B** (simulation): No external credentials needed. Run `08a_erp_data_simulation`
# MAGIC to generate synthetic ERP data instead.
# MAGIC
# MAGIC The track is determined by whether `erp_connection_host` is set in config.

# COMMAND ----------

# MAGIC %run "./000-config"

# COMMAND ----------

# DBTITLE 1,Determine Track
import yaml

USE_REAL_FEDERATION = bool(config.get("erp_connection_host", ""))

print(f"Federation track: {'REAL (external DB)' if USE_REAL_FEDERATION else 'SIMULATION (synthetic data)'}")

# COMMAND ----------

# DBTITLE 1,Track A: Register UC Connection to External ERP
if USE_REAL_FEDERATION:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import ConnectionType

    w = WorkspaceClient()
    catalog = config['catalog']
    schema = config['database']

    CONNECTION_NAME = f"erp_external_{config['database']}"

    existing = None
    try:
        existing = w.connections.get(CONNECTION_NAME)
        print(f"Connection already exists: {CONNECTION_NAME}")
    except Exception:
        pass

    if not existing:
        connection = w.connections.create(
            name=CONNECTION_NAME,
            connection_type=ConnectionType.POSTGRESQL,
            options={
                "host":     config["erp_connection_host"],
                "port":     str(config.get("erp_connection_port", "5432")),
                "user":     config["erp_connection_user"],
                "password": config["erp_connection_password"],
                "database": config.get("erp_connection_database", "erp"),
            },
            comment="External ERP system connection for billing accelerator",
            read_only=True,
        )
        print(f"Created connection: {connection.full_name}")

    FOREIGN_CATALOG_NAME = f"erp_federated_{config['database']}"
    try:
        w.catalogs.get(FOREIGN_CATALOG_NAME)
        print(f"Foreign catalog already exists: {FOREIGN_CATALOG_NAME}")
    except Exception:
        foreign_catalog = w.catalogs.create(
            name=FOREIGN_CATALOG_NAME,
            connection_name=CONNECTION_NAME,
            options={"database": config.get("erp_connection_database", "erp")},
            comment="Lakehouse Federation catalog for ERP external data access",
        )
        print(f"Created foreign catalog: {foreign_catalog.full_name}")

    # Persist to config.yaml using in-place update
    with open("config.yaml", "r") as f:
        yaml_text = f.read()
    import re
    for key, val in [("erp_foreign_catalog", FOREIGN_CATALOG_NAME), ("erp_uc_connection", CONNECTION_NAME)]:
        if f"{key}:" in yaml_text:
            yaml_text = re.sub(rf"{key}:.*", f"{key}: '{val}'", yaml_text)
        else:
            yaml_text += f"\n{key}: '{val}'\n"
    with open("config.yaml", "w") as f:
        f.write(yaml_text)

    print(f"Config updated: erp_foreign_catalog={FOREIGN_CATALOG_NAME}, erp_uc_connection={CONNECTION_NAME}")
else:
    print("Track A skipped — no erp_connection_host configured. Using simulation track.")
    print("To enable real federation, set erp_connection_host/user/password in config.")

# COMMAND ----------

# DBTITLE 1,Summary
print(f"Federation track: {'REAL' if USE_REAL_FEDERATION else 'SIMULATION'}")
print(f"Next step: Run 08a_erp_data_simulation (Track B) or 08b_external_data_ingestion (Track A/B)")
