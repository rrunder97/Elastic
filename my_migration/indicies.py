# indices.py
import fnmatch
import time
from elasticsearch import exceptions
from config import es_source, es_target, PREFIX, SLICE_COUNT, BATCH_SIZE, REQUEST_TIMEOUT, THROTTLE_DOCS_PER_SEC, logger

def list_indices():
    """
    Return all non‑system indices (open + closed) from the source cluster.
    """
    try:
        raw = es_source.cat.indices(format="json", expand_wildcards="all")
        return [idx["index"] for idx in raw if not idx["index"].startswith(".")]
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing indices: %s", e)
        return []

def create_index_if_no_template(es_source, es_target, index_name, new_index_name):
    """
    Ensure new_index_name exists on target with the same settings/mappings/aliases as index_name on source—
    either via an index template or by copying directly.
    """
    try:
        # 1) Try to find a matching index-template on source
        templates = es_source.indices.get_index_template().get("index_templates", [])
        for tpl in templates:
            patterns = tpl["index_template"]["index_patterns"]
            if any(fnmatch.fnmatch(index_name, pat) for pat in patterns):
                logger.info("🧩 Using template '%s' for index '%s'", tpl["name"], index_name)
                tmpl = tpl["index_template"]["template"]
                settings = {
                    k: v for k, v in tmpl["settings"].get("index", {}).items()
                    if not k.startswith(("version","uuid","provided_name"))
                }
                body = {
                    "settings": settings,
                    "mappings": tmpl.get("mappings", {}),
                    "aliases": tmpl.get("aliases", {})
                }
                es_target.indices.create(index=new_index_name, body=body)
                return
        # 2) No template found → copy settings & mappings directly
        logger.info("⚙️  No template match for '%s'; copying settings/mappings manually", index_name)
        src_settings = es_source.indices.get_settings(index=index_name)[index_name]["settings"]["index"]
        settings = {
            k: v for k, v in src_settings.items()
            if not k.startswith(("version","uuid","provided_name"))
        }
        mappings = es_source.indices.get_mapping(index=index_name)[index_name]["mappings"]
        body = {"settings": settings, "mappings": mappings}
        es_target.indices.create(index=new_index_name, body=body)
        # Copy any aliases from the source index
        aliases = es_source.indices.get(index=index_name)[index_name].get("aliases", {})
        for alias in aliases:
            es_target.indices.put_alias(index=new_index_name, name=alias)
        logger.info("✅ Created '%s' manually with aliases %s", new_index_name, list(aliases))
    except exceptions.ElasticsearchException as e:
        logger.error("Error creating index '%s': %s", new_index_name, e)

def swap_aliases(es_client, old_index, new_index, aliases):
    """
    Atomically move each alias in `aliases` from old_index to new_index.
    """
    try:
        actions = [{"add": {"index": new_index, "alias": alias}} for alias in aliases]
        es_client.indices.update_aliases(body={"actions": actions})
    except exceptions.ElasticsearchException as e:
        logger.error("Error swapping aliases from '%s' to '%s': %s", old_index, new_index, e)

def migrate_index(es_source, es_target, index_name):
    """
    Migrate data for a specific index from the source to the target.
    """
    new_index = f"{PREFIX}{index_name}"
    # 1) Create target index if needed
    create_index_if_no_template(es_source, es_target, index_name, new_index)

    # 2) Kick off remote, sliced reindex
    body = {
        "source": {
            "remote": {
                "host":     es_source.transport.hosts[0]['host'],
                "username": es_source.transport.hosts[0].get("user", AUTH["user"]),
                "password": es_source.transport.hosts[0].get("pass", AUTH["pass"])
            },
            "index": index_name,
            "size":  BATCH_SIZE
        },
        "dest": {"index": new_index},
        "slices": SLICE_COUNT,
        "requests_per_second": THROTTLE_DOCS_PER_SEC
    }

    try:
        resp = es_target.reindex(
            body=body,
            wait_for_completion=False,
            request_timeout=REQUEST_TIMEOUT
        )
        task_id = resp["task"]
        logger.info("🚀 Started reindex task %s for %s → %s", task_id, index_name, new_index)

        # 3) Poll until task completes
        while True:
            status = es_target.tasks.get(task_id=task_id)
            if status.get("completed"):
                break
            stats = status["task"]["status"]
            logger.info("   Progress: %d/%d docs", stats.get("created", 0), stats.get("total", 0))
            time.sleep(30)

        # 4) Check for failures
        failures = status["task"]["status"].get("failures", [])
        if failures:
            logger.warning("❗ Reindex of '%s' completed with %d failures", index_name, len(failures))
        else:
            logger.info("✅ Reindex of '%s' complete (%d docs)", index_name, stats.get("created", 0))

        # 5) Swap aliases, if any
        src_aliases = list(es_source.indices.get_alias(index=index_name)[index_name]["aliases"].keys())
        if src_aliases:
            swap_aliases(es_target, index_name, new_index, src_aliases)

    except exceptions.TransportError as e:
        logger.error("TransportError during reindex of '%s': %s", index_name, e.info)
    except Exception as e:
        logger.error("Unexpected error during reindex of '%s': %s", index_name, e)
