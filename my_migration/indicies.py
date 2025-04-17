# indices.py

import fnmatch
import time
import re
from elasticsearch import exceptions
from config import (
    es_source,        # Elasticsearch client for the source cluster
    es_target,        # Elasticsearch client for the target cluster
    PREFIX,           # Prefix to apply to migrated index names
    SLICE_COUNT,      # Number of slices for parallel reindexing
    BATCH_SIZE,       # Number of documents per batch in reindex
    REQUEST_TIMEOUT,  # Timeout for long‐running requests
    THROTTLE_DOCS_PER_SEC,  # Throttle speed for reindex
    logger            # Central logger
)
from alias_utils import migrate_alias_between_clusters  # Cross‐cluster alias helper
from validation_utils import compare_doc_counts, compare_mappings

def list_indices(source_client):
    """
    Return all non‑system indices (open + closed) from the given source cluster.

    Uses the Cat Indices API to list indices, then filters out any name
    beginning with a dot ('.') to avoid system indices.
    """
    try:
        raw = source_client.cat.indices(format="json", expand_wildcards="all")
        # Each entry has a field "index"; filter out names starting with '.'
        return [idx["index"] for idx in raw if not idx["index"].startswith(".")]
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing indices: %s", e)
        return []

def list_indices_by_regex(source_client, regex_pattern):
    """
    Retrieve all non‑system indices that match the given regex pattern.

    1. List all non‑system indices.
    2. Compile the provided regex and filter the list.
    """
    try:
        raw = source_client.cat.indices(format="json", expand_wildcards="all")
        indices = [idx["index"] for idx in raw if not idx["index"].startswith(".")]
        compiled = re.compile(regex_pattern)
        return [idx for idx in indices if compiled.search(idx)]
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing indices by regex '%s': %s", regex_pattern, e)
        return []

def list_specific_index(source_client, index_name):
    """
    Retrieve a specific index by exact match.

    Lists non‑system indices and returns a list containing index_name if it exists.
    """
    try:
        raw = source_client.cat.indices(format="json", expand_wildcards="all")
        indices = [idx["index"] for idx in raw if not idx["index"].startswith(".")]
        return [idx for idx in indices if idx == index_name]
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing specific index '%s': %s", index_name, e)
        return []




def create_index_if_no_template(source_client, target_client, index_name, new_index_name):
    """
    Ensure new_index_name exists on target with the same settings/mappings/aliases
    as index_name on source—either via an index template or by copying directly.

    Steps:
    1) Try to find a matching index template on the source cluster.
       - If found, extract its settings/mappings/aliases and create the index on target.
    2) If no template matches, fetch the source index's settings & mappings
       via the Get Settings and Get Mapping APIs and create the target index.
    3) Copy any existing aliases from source → target via the Put Alias API.
    """
    try:
        # Step 1: Look for a matching index template
        templates = source_client.indices.get_index_template().get("index_templates", [])
        for tpl in templates:
            patterns = tpl["index_template"]["index_patterns"]
            if any(fnmatch.fnmatch(index_name, pat) for pat in patterns):
                logger.info("🧩 Using template '%s' for index '%s'", tpl["name"], index_name)
                tmpl = tpl["index_template"]["template"]
                # Filter out internal metadata keys
                settings = {
                    k: v for k, v in tmpl["settings"].get("index", {}).items()
                    if not k.startswith(("version", "uuid", "provided_name"))
                }
                body = {
                    "settings": settings,
                    "mappings": tmpl.get("mappings", {}),
                    "aliases": tmpl.get("aliases", {})
                }
                # Create the index on the target cluster
                target_client.indices.create(index=new_index_name, body=body)
                return

        # Step 2: No template → copy settings & mappings directly
        logger.info("⚙️  No template for '%s'; copying settings/mappings manually", index_name)
        # Get source index settings
        src_settings = source_client.indices.get_settings(index=index_name)[index_name]["settings"]["index"]
        settings = {
            k: v for k, v in src_settings.items()
            if not k.startswith(("version", "uuid", "provided_name"))
        }
        # Get source index mappings
        mappings = source_client.indices.get_mapping(index=index_name)[index_name]["mappings"]
        body = {"settings": settings, "mappings": mappings}
        # Create the index on the target cluster
        target_client.indices.create(index=new_index_name, body=body)

        # Step 3: Copy aliases
        aliases = source_client.indices.get(index=index_name)[index_name].get("aliases", {})
        for alias in aliases:
            target_client.indices.put_alias(index=new_index_name, name=alias)
        logger.info("✅ Created '%s' with aliases %s", new_index_name, list(aliases))

    except exceptions.ElasticsearchException as e:
        logger.error("Error creating index '%s': %s", new_index_name, e)

def migrate_index(source_client, target_client, index_name):
    """
    Migrate data for a specific index from the source to the target cluster.

    1) Ensure the target index exists with proper settings/mappings.
    2) Kick off a remote, sliced reindex to copy documents.
    3) Poll the Tasks API until reindex completes.
    4) Log any failures or successes.
    5) Migrate aliases from the source cluster to the target cluster.
    """
    new_index = f"{PREFIX}{index_name}"

    # 1) Create target index if it doesn't already exist
    create_index_if_no_template(source_client, target_client, index_name, new_index)

    # 2) Prepare the reindex body
    body = {
        "source": {
            "remote": {
                # Remote host info for cross-cluster reindex
                "host":     source_client.transport.hosts[0]["host"],
                "username": source_client.transport.hosts[0].get("user", "user"),
                "password": source_client.transport.hosts[0].get("pass", "pass")
            },
            "index": index_name,
            "size":  BATCH_SIZE
        },
        "dest": {"index": new_index},
        "slices": SLICE_COUNT,                   # Parallelize the reindex into N slices
        "requests_per_second": THROTTLE_DOCS_PER_SEC  # Throttle speed if needed
    }

    try:
        # 2) Kick off the reindex as a background task
        resp = target_client.reindex(
            body=body,
            wait_for_completion=False,
            request_timeout=REQUEST_TIMEOUT
        )
        task_id = resp["task"]
        logger.info("🚀 Started reindex task %s for %s → %s", task_id, index_name, new_index)

        # 3) Poll the Tasks API for completion
        while True:
            status = target_client.tasks.get(task_id=task_id)
            if status.get("completed"):
                break
            stats = status["task"]["status"]
            logger.info("   Progress: %d/%d docs", stats.get("created", 0), stats.get("total", 0))
            time.sleep(30)

        # 4) Check for failures in the reindex response
        failures = status["task"]["status"].get("failures", [])
        if failures:
            logger.warning("❗ Reindex of '%s' completed with %d failures", index_name, len(failures))
        else:
            logger.info("✅ Reindex of '%s' complete (%d docs)", index_name, stats.get("created", 0))


        # 5) Validation checks before alias cutover
        if not compare_doc_counts(source_client, target_client, index_name, new_index):
            logger.error("❌ Document count mismatch for '%s'. Aborting alias cutover.", index_name)
            return

        if not compare_mappings(source_client, target_client, index_name, new_index):
            logger.error("❌ Mapping structure mismatch for '%s'. Aborting alias cutover.", index_name)
            return

        # 5) Migrate aliases cross‑cluster, if any exist
        src_aliases = list(
            source_client.indices
                .get_alias(index=index_name)[index_name]["aliases"]
                .keys()
        )
        if src_aliases:
            migrate_alias_between_clusters(
                source_client,   # remove aliases here
                target_client,   # add aliases here
                index_name,      # old index name
                new_index,       # new index name
                src_aliases      # list of alias names
            )

    except exceptions.TransportError as e:
        logger.error("TransportError during reindex of '%s': %s", index_name, e.info)
    except Exception as e:
        logger.error("Unexpected error during reindex of '%s': %s", index_name, e)




# if not compare_doc_counts(source_client, target_client, source_index, new_index):
#     logger.error("Document count mismatch for '%s'. Aborting alias cutover.", source_index)
#     return

# if not compare_mappings(source_client, target_client, source_index, new_index):
#     logger.error("Mapping mismatch for '%s'. Aborting alias cutover.", source_index)
#     return