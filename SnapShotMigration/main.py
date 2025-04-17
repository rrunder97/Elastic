# main.py
import argparse
import re
from config import es_source, es_target, REPO_NAME, SNAPSHOT_NAME, logger
from cluster_migrations import (
    migrate_component_templates,
    migrate_index_templates,
    migrate_ingest_pipelines,
    migrate_stored_scripts,
    migrate_enrich_policies,
    migrate_transforms,
    migrate_rollup_jobs,
    migrate_watchers,
    migrate_roles,
    migrate_users,
    migrate_role_mappings
)
from lifecycle import migrate_ilm_policies
from indices import (
    list_indices,
    list_indices_by_regex,
    list_specific_index,
    trigger_snapshot,
    check_snapshot_status,
    restore_snapshot,
    post_restore_validations,
    handle_alias_migration
)

# Optional renaming values.
# For example, if you want to add a prefix "new_" to each index upon restore, define these:
RENAME_PATTERN = "^(.*)$"     # Capture the whole index name
RENAME_REPLACEMENT = "new_\\1"  # Prepend "new_" to each index name

def main():
    parser = argparse.ArgumentParser(
        description="Perform Elasticsearch migration on selected indices."
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--index", type=str,
        help="Migrate only the specified index (exact name match)."
    )
    group.add_argument(
        "--regex", type=str,
        help="Migrate indices matching the provided regex pattern."
    )
    args = parser.parse_args()

    # 1) Determine which indices to migrate:
    if args.index:
        indices = list_specific_index(es_source, args.index)
        logger.info("Migrating specific index: %s", indices)
    elif args.regex:
        indices = list_indices_by_regex(es_source, args.regex)
        logger.info("Migrating indices matching regex '%s': %s", args.regex, indices)
    else:
        indices = list_indices(es_source)
        logger.info("Migrating all non‑system indices: %s", indices)

    if not indices:
        logger.info("No matching indices found. Exiting.")
        return

    # 2) Run all cluster‑level migrations.
    # These functions handle migration of global configuration items that snapshots do not cover:
    migrate_component_templates()
    migrate_index_templates()
    migrate_ingest_pipelines()
    migrate_stored_scripts()
    migrate_ilm_policies()
    migrate_roles()
    migrate_users()
    migrate_role_mappings()
    migrate_transforms()
    migrate_rollup_jobs()
    migrate_watchers()
    migrate_enrich_policies()

    # 3) Perform the snapshot/restore for the selected indices.
    logger.info("Triggering snapshot of indices: %s", indices)
    snapshot_resp = trigger_snapshot(es_source, REPO_NAME, SNAPSHOT_NAME, indices)
    if not snapshot_resp:
        logger.error("Snapshot creation failed. Aborting migration.")
        return

    # Optionally, check snapshot status.
    status = check_snapshot_status(es_source, REPO_NAME, SNAPSHOT_NAME)
    logger.info("Snapshot status: %s", status)

    # Restore the snapshot on the target cluster.
    # Here, we restore all indices from the snapshot.
    logger.info("Restoring snapshot '%s' on target cluster", SNAPSHOT_NAME)
    restore_resp = restore_snapshot(
        es_target,
        REPO_NAME,
        SNAPSHOT_NAME,
        indices_pattern="_all",
        rename_pattern=RENAME_PATTERN,
        rename_replacement=RENAME_REPLACEMENT
    )
    if not restore_resp:
        logger.error("Snapshot restore failed. Aborting migration.")
        return

    # 4) Post-restore: Validate each index and adjust aliases if necessary.
    for original_index in indices:
        # Determine the new index name based on the rename rule.
        new_index = re.sub(RENAME_PATTERN, RENAME_REPLACEMENT, original_index)
        logger.info("Validating migration for '%s' (restored as '%s')", original_index, new_index)
        if not post_restore_validations(es_source, es_target, original_index, new_index):
            logger.error("Validation failed for index '%s'.", original_index)
        else:
            logger.info("Index '%s' passed validations.", original_index)

        # Handle alias migration (this will adjust aliases if the index name has changed).
        handle_alias_migration(es_source, es_target, original_index, new_index)

    logger.info("🎉 Migration completed successfully")

if __name__ == "__main__":
    main()
