# indices.py

import fnmatch
import time
import re
from elasticsearch import exceptions
from config import (
    es_source,        # Elasticsearch client for the source cluster
    es_target,        # Elasticsearch client for the target cluster
    PREFIX,           # Optional prefix for renaming indices during restore if needed
    SLICE_COUNT,      # No longer used with snapshot restore
    BATCH_SIZE,       # No longer used with snapshot restore
    REQUEST_TIMEOUT,  # Timeout for long-running requests
    THROTTLE_DOCS_PER_SEC,  # No longer used with snapshot restore
    logger            # Central logger
)
from alias_utils import migrate_alias_between_clusters  # Alias migration helper (if renaming is required)
from validation_utils import compare_doc_counts, compare_mappings

# ----------------------------
# Index Listing and Discovery
# ----------------------------

def list_indices(source_client):
    """
    Return all non‑system indices (open + closed) from the given source cluster.
    Uses the Cat Indices API and filters out system indices starting with a dot.
    """
    try:
        raw = source_client.cat.indices(format="json", expand_wildcards="all")
        return [idx["index"] for idx in raw if not idx["index"].startswith(".")]
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing indices: %s", e)
        return []

def list_indices_by_regex(source_client, regex_pattern):
    """
    Retrieve all non‑system indices matching the provided regex pattern.
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
    """
    try:
        raw = source_client.cat.indices(format="json", expand_wildcards="all")
        indices = [idx["index"] for idx in raw if not idx["index"].startswith(".")]
        return [idx for idx in indices if idx == index_name]
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing specific index '%s': %s", index_name, e)
        return []

# --------------------------------------
# Snapshot and Restore Related Functions
# --------------------------------------

def trigger_snapshot(source_client, repo_name, snapshot_name, indices=None):
    """
    Trigger a snapshot creation on the source cluster.
    
    :param repo_name: The snapshot repository name.
    :param snapshot_name: The identifier for the snapshot.
    :param indices: Optional list of indices to snapshot (as a comma-separated string). If None, all eligible indices are included.
    :return: Response from the snapshot creation API.
    """
    body = {}
    if indices:
        # Convert list of indices to comma-separated string
        body["indices"] = ",".join(indices)
    try:
        # Optionally, you can choose wait_for_completion=False and implement polling in your automation.
        resp = source_client.snapshot.create(
            repository=repo_name,
            snapshot=snapshot_name,
            body=body,
            wait_for_completion=True,
            request_timeout=REQUEST_TIMEOUT
        )
        logger.info("Snapshot '%s' created in repository '%s'", snapshot_name, repo_name)
        return resp
    except exceptions.ElasticsearchException as e:
        logger.error("Error triggering snapshot '%s': %s", snapshot_name, e)
        return None

def check_snapshot_status(source_client, repo_name, snapshot_name):
    """
    Check the status of a snapshot from the source cluster.
    
    :param repo_name: The repository name.
    :param snapshot_name: The name of the snapshot.
    :return: The snapshot status details.
    """
    try:
        status = source_client.snapshot.status(
            repository=repo_name,
            snapshot=snapshot_name
        )
        logger.info("Snapshot status for '%s': %s", snapshot_name, status)
        return status
    except exceptions.ElasticsearchException as e:
        logger.error("Error checking snapshot status for '%s': %s", snapshot_name, e)
        return None

def restore_snapshot(target_client, repo_name, snapshot_name, indices_pattern="_all", rename_pattern=None, rename_replacement=None):
    """
    Restore a snapshot on the target cluster.
    
    Optionally, you can rename indices during restore if the index naming convention needs to change.
    
    :param repo_name: The snapshot repository name.
    :param snapshot_name: The snapshot identifier.
    :param indices_pattern: Which indices from the snapshot to restore (default: "_all").
    :param rename_pattern: Regex pattern to match index names for renaming during restore.
    :param rename_replacement: Replacement string if renaming is needed.
    :return: Response from the snapshot restore API.
    """
    body = {
        "indices": indices_pattern,
    }
    if rename_pattern and rename_replacement:
        body["rename_pattern"] = rename_pattern
        body["rename_replacement"] = rename_replacement
    try:
        # Like snapshot creation, you can choose wait_for_completion=True for synchronous restore
        resp = target_client.snapshot.restore(
            repository=repo_name,
            snapshot=snapshot_name,
            body=body,
            wait_for_completion=True,
            request_timeout=REQUEST_TIMEOUT
        )
        logger.info("Snapshot '%s' restored on target cluster", snapshot_name)
        return resp
    except exceptions.ElasticsearchException as e:
        logger.error("Error restoring snapshot '%s': %s", snapshot_name, e)
        return None

# ------------------------------------------------
# Post-Restore Validations and Alias Migration
# ------------------------------------------------

def post_restore_validations(source_client, target_client, original_index_name, restored_index_name):
    """
    Validate that the restored index on the target cluster matches the source.
    Verifies document counts and mappings.
    
    :return: True if validations pass, False otherwise.
    """
    if not compare_doc_counts(source_client, target_client, original_index_name, restored_index_name):
        logger.error("❌ Document count mismatch for index '%s'", original_index_name)
        return False
    if not compare_mappings(source_client, target_client, original_index_name, restored_index_name):
        logger.error("❌ Mapping structure mismatch for index '%s'", original_index_name)
        return False
    logger.info("✅ Validations passed for index '%s'", original_index_name)
    return True

def handle_alias_migration(source_client, target_client, original_index_name, restored_index_name):
    """
    Handle alias migration if the index name is changed during restore.
    Even though snapshots transfer alias details, a rename may require an alias cutover.
    """
    try:
        # Retrieve aliases from the source index
        src_aliases = list(
            source_client.indices.get_alias(index=original_index_name)[original_index_name]["aliases"].keys()
        )
        if src_aliases:
            migrate_alias_between_clusters(
                source_client,   # remove aliases from source if needed
                target_client,   # apply aliases to the restored index on the target cluster
                original_index_name,
                restored_index_name,
                src_aliases
            )
            logger.info("✅ Alias migration complete for index '%s'", original_index_name)
    except exceptions.ElasticsearchException as e:
        logger.error("Error migrating aliases for index '%s': %s", original_index_name, e)
