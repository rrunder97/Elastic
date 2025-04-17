# config.py
import logging
from elasticsearch import Elasticsearch
#dfsdf
# === Configuration ===
SOURCE_ES = "http://source-es-url:9200"
TARGET_ES = "http://target-es-url:9200"
AUTH = {"user": "user", "pass": "pass"}
#PREFIX = "migrated-"
SLICE_COUNT = 4
BATCH_SIZE = 1000
REQUEST_TIMEOUT = 600
THROTTLE_DOCS_PER_SEC = -1  # -1 = no throttle

# === Logging Setup ===
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(message)s",
    level=logging.INFO
)
logger = logging.getLogger("es_migration")

# === Elasticsearch Clients ===
es_source = Elasticsearch(
    SOURCE_ES,
    basic_auth=(AUTH["user"], AUTH["pass"])
)
es_target = Elasticsearch(
    TARGET_ES,
    basic_auth=(AUTH["user"], AUTH["pass"])
)
