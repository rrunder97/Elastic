# main.py
import argparse
from config import es_source, es_target, logger
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
    migrate_index
)

#To run for a specific index:
#python main.py --index my_index_name

# To run using a regex:
#python main.py --regex '.*logs.*'

#cd ~/Desktop/Elastic/my_migration
#python main.py --regex '.*mdr.*'


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
        # list_specific_index now takes (source_client, index_name)
        indices = list_specific_index(es_source, args.index)
        logger.info("Migrating specific index: %s", indices)
    elif args.regex:
        # list_indices_by_regex now takes (source_client, regex_pattern)
        indices = list_indices_by_regex(es_source, args.regex)
        logger.info("Migrating indices matching regex '%s': %s", args.regex, indices)
    else:
        # list_indices now takes (source_client)
        indices = list_indices(es_source)
        logger.info("Migrating all non‑system indices: %s", indices)

    if not indices:
        logger.info("No matching indices found. Exiting.")
        return

    # 2) Run all cluster‑level migrations (no client ambiguity here):
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

    # 3) Migrate each index (explicitly passing both clients)
    for idx in indices:
        migrate_index(es_source, es_target, idx)

    logger.info("🎉 Migration completed successfully")

if __name__ == "__main__":
    main()