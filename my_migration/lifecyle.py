# lifecycle.py
from elasticsearch import exceptions
from config import es_source, es_target, logger

def migrate_ilm_policies():
    """
    Migrate Index Lifecycle Management (ILM) policies from source to target.
    """
    try:
        policies = es_source.ilm.get_lifecycle()
        for name, body in policies.items():
            es_target.ilm.put_lifecycle(name=name, policy=body["policy"])
            logger.info("🕒 Migrated ILM policy '%s'", name)
    except Exception as e:
        logger.error("Error migrating ILM policies: %s", e)
