# alias_utils.py
from elasticsearch import exceptions
from config import logger

def migrate_alias_between_clusters(
    source_client, target_client,
    old_index, new_index, aliases
):
    """
    1) Remove each alias from old_index on the source cluster.
    2) Add each alias to new_index on the target cluster.

    :param source_client: Elasticsearch client for the source environment.
    :param target_client: Elasticsearch client for the target environment.
    :param old_index: Name of the source index.
    :param new_index: Name of the target index.
    :param aliases: List of alias names to migrate.
    """
    # 1) Remove on source cluster
    for alias in aliases:
        try:
            source_client.indices.delete_alias(index=old_index, name=alias)
            logger.info("🔗 Removed alias '%s' from '%s' on SOURCE", alias, old_index)
        except exceptions.NotFoundError:
            # Alias wasn’t there—ignore
            pass
        except Exception as e:
            logger.error("❗ Error removing alias '%s' from '%s' on SOURCE: %s",
                         alias, old_index, e)

    # 2) Add on target cluster
    actions = [{"add": {"index": new_index, "alias": alias}} for alias in aliases]
    try:
        target_client.indices.update_aliases(body={"actions": actions})
        logger.info("🔗 Added aliases %s to '%s' on TARGET", aliases, new_index)
    except Exception as e:
        logger.error("❗ Error adding aliases %s to '%s' on TARGET: %s",
                     aliases, new_index, e)
