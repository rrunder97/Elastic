# validation_utils.py

import logging
from elasticsearch import exceptions

logger = logging.getLogger("validation_utils")

def check_version_compatibility(source_client, target_client):
    """
    Ensure source and target Elasticsearch clusters are running the same major.minor version.
    Returns True if compatible, False otherwise.
    """
    try:
        src_version = source_client.info()['version']['number']
        tgt_version = target_client.info()['version']['number']
        src_mm = '.'.join(src_version.split('.')[:2])
        tgt_mm = '.'.join(tgt_version.split('.')[:2])
        if src_mm != tgt_mm:
            logger.error(
                "Version mismatch: source %s vs target %s", src_version, tgt_version
            )
            return False
        logger.info(
            "Version compatibility OK: source %s == target %s", src_version, tgt_version
        )
        return True
    except exceptions.ElasticsearchException as e:
        logger.error("Error checking versions: %s", e)
        return False

def compare_doc_counts(source_client, target_client, source_index, target_index):
    """
    Compare document counts between source_index on source_client and
    target_index on target_client. Returns True if they match.
    """
    try:
        src_count = source_client.count(index=source_index)['count']
        tgt_count = target_client.count(index=target_index)['count']
        if src_count != tgt_count:
            logger.error(
                "Doc count mismatch for '%s': source=%d, target=%d",
                source_index, src_count, tgt_count
            )
            return False
        logger.info(
            "Doc counts match for '%s': %d docs",
            source_index, src_count
        )
        return True
    except exceptions.ElasticsearchException as e:
        logger.error(
            "Error comparing doc counts '%s' vs '%s': %s",
            source_index, target_index, e
        )
        return False

def compare_mappings(source_client, target_client, source_index, target_index):
    """
    Compare mappings between source_index and target_index.
    Returns True if they are identical.
    """
    try:
        src_map = source_client.indices.get_mapping(index=source_index)[source_index]['mappings']
        tgt_map = target_client.indices.get_mapping(index=target_index)[target_index]['mappings']
        if src_map != tgt_map:
            logger.error("Mapping mismatch for '%s' vs '%s'", source_index, target_index)
            # For debugging, you can uncomment these:
            # import json
            # logger.debug("Source mapping: %s", json.dumps(src_map, indent=2, sort_keys=True))
            # logger.debug("Target mapping: %s", json.dumps(tgt_map, indent=2, sort_keys=True))
            return False
        logger.info("Mappings match for '%s' and '%s'", source_index, target_index)
        return True
    except exceptions.ElasticsearchException as e:
        logger.error(
            "Error comparing mappings '%s' vs '%s': %s",
            source_index, target_index, e
        )
        return False

def create_snapshot(client, repository, snapshot_name, indices=None, wait_for_completion=True):
    """
    Create a snapshot of the given indices (or all if None) in the specified repository.
    """
    try:
        body = {'indices': indices} if indices else {}
        client.snapshot.create(
            repository=repository,
            snapshot=snapshot_name,
            body=body,
            wait_for_completion=wait_for_completion
        )
        logger.info("Snapshot '%s' created in repository '%s'", snapshot_name, repository)
        return True
    except exceptions.ElasticsearchException as e:
        logger.error("Error creating snapshot '%s': %s", snapshot_name, e)
        return False

def list_snapshots(client, repository):
    """
    List all snapshots in the given repository.
    """
    try:
        resp = client.snapshot.get(repository=repository, snapshot='_all')
        snapshots = [s['snapshot'] for s in resp.get('snapshots', [])]
        logger.info("Snapshots in '%s': %s", repository, snapshots)
        return snapshots
    except exceptions.ElasticsearchException as e:
        logger.error("Error listing snapshots in '%s': %s", repository, e)
        return []
