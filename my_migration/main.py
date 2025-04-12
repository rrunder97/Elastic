# main.py
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
from indices import list_indices, migrate_index

def main():
    logger.info("🔄 Starting full Elasticsearch migration")

    # Cluster‑level migrations:
    migrate_component_templates()
    migrate_index_templates()
    migrate_ingest_pipelines()
    migrate_stored_scripts()
    migrate_ilm_policies()    # ILM policies are in lifecycle.py
    migrate_roles()
    migrate_users()
    migrate_role_mappings()
    migrate_transforms()
    migrate_rollup_jobs()
    migrate_watchers()
    migrate_enrich_policies()

    # Data migration for all non‑system indices:
    indices = list_indices()
    for name in indices:
        migrate_index(es_source, es_target, name)

    logger.info("🎉 Migration completed successfully")

if __name__ == "__main__":
    main()
