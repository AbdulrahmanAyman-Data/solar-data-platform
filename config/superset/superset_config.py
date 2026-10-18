
import os

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "change_this_secret_key_before_first_boot"
)

SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ.get('DATABASE_USER', 'superset')}:"
    f"{os.environ.get('DATABASE_PASSWORD', 'superset')}@"
    f"{os.environ.get('DATABASE_HOST', 'postgres-superset')}:"
    f"{os.environ.get('DATABASE_PORT', '5432')}/superset"
)

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
}

# Mapbox
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY", "")

# Suggested extra database connection to add manually (or via import) in the
# Superset UI -> Settings -> Database Connections, once the stack is up:
#   trino://trino-engine-eco:8080/delta
