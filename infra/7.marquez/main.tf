data "azurerm_resource_group" "main" {
  name = local.config.resource_group_name
}

# ---------------------------------------------------------------------------
# Marquez database — added to the existing cricwebsite PostgreSQL server.
# Marquez runs its own schema migrations on first startup.
# ---------------------------------------------------------------------------

data "azurerm_postgresql_flexible_server" "cricwebsite" {
  name                = "psql-cricwebsite"
  resource_group_name = local.config.resource_group_cricwebsite
}

resource "azurerm_postgresql_flexible_server_database" "marquez" {
  name      = "marquez"
  server_id = data.azurerm_postgresql_flexible_server.cricwebsite.id
  charset   = "UTF8"
  collation = "en_US.utf8"
}

# ---------------------------------------------------------------------------
# Azure Container Instance — Marquez API + Web UI
#
# Both containers share the same network namespace (localhost) so marquez-web
# can reach marquez-api via localhost:5000 without extra networking.
#
# Exposed ports:
#   5000 — Marquez OpenLineage API  (POST /api/v1/lineage, GET /api/v1/jobs)
#   3000 — Marquez Web UI
# ---------------------------------------------------------------------------

resource "azurerm_container_group" "marquez" {
  name                = "aci-${local.marquez_name}"
  resource_group_name = data.azurerm_resource_group.main.name
  location            = data.azurerm_resource_group.main.location
  os_type             = "Linux"
  ip_address_type     = "Public"
  dns_name_label      = local.marquez_name

  # Marquez API server
  container {
    name   = "marquez-api"
    image  = "marquezproject/marquez:0.50.0"
    cpu    = "0.5"
    memory = "1.0"

    ports {
      port     = 5000
      protocol = "TCP"
    }

    environment_variables = {
      MARQUEZ_DB_HOST = data.azurerm_postgresql_flexible_server.cricwebsite.fqdn
      MARQUEZ_DB_PORT = "5432"
      # Appending SSL params to the db name produces the valid JDBC URL:
      # jdbc:postgresql://{host}:5432/marquez?ssl=true&sslmode=require
      # Azure PostgreSQL Flexible Server enforces SSL — without this the connection is rejected.
      MARQUEZ_DB_NAME = "marquez?ssl=true&sslmode=require"
      MARQUEZ_DB_USER = local.config.cricwebsite_db_user
    }

    secure_environment_variables = {
      MARQUEZ_DB_PASSWORD = var.marquez_db_password
    }
  }

  # Marquez Web UI — talks to marquez-api via shared localhost
  container {
    name   = "marquez-web"
    image  = "marquezproject/marquez-web:0.50.0"
    cpu    = "0.25"
    memory = "0.5"

    ports {
      port     = 3000
      protocol = "TCP"
    }

    environment_variables = {
      MARQUEZ_HOST = "localhost"
      MARQUEZ_PORT = "5000"
    }
  }

  image_registry_credential {
    server   = "index.docker.io"
    username = var.dockerhub_username
    password = var.dockerhub_token
  }

  tags = local.config.tags
}
