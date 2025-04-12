# cluster_migrations.py
from config import es_source, es_target, logger
from elasticsearch import exceptions

def migrate_component_templates():
    """
    Migrate component templates from source to target.
    """
    try:
        resp = es_source.cluster.get_component_template()
        for tmpl in resp.get("component_templates", []):
            name = tmpl["name"]
            body = tmpl["component_template"]
            es_target.cluster.put_component_template(name=name, body=body)
            logger.info("📦 Migrated component template '%s'", name)
    except Exception as e:
        logger.error("Error migrating component templates: %s", e)

def migrate_index_templates():
    """
    Migrate index templates from source to target.
    """
    try:
        resp = es_source.indices.get_index_template()
        for tpl in resp.get("index_templates", []):
            name = tpl["name"]
            body = tpl["index_template"]
            es_target.indices.put_index_template(name=name, body=body)
            logger.info("📦 Migrated index template '%s'", name)
    except Exception as e:
        logger.error("Error migrating index templates: %s", e)

def migrate_ingest_pipelines():
    """
    Migrate ingest pipelines from source to target.
    """
    try:
        pipelines = es_source.ingest.get_pipeline()
        for pid, body in pipelines.items():
            es_target.ingest.put_pipeline(id=pid, body=body)
            logger.info("🚰 Migrated ingest pipeline '%s'", pid)
    except Exception as e:
        logger.error("Error migrating ingest pipelines: %s", e)

def migrate_stored_scripts():
    """
    Migrate stored scripts from source to target.
    """
    try:
        scripts = es_source.cluster.get_stored_script()
        for lang, slist in scripts.items():
            for sid, body in slist.items():
                es_target.cluster.put_stored_script(id=sid, body=body)
                logger.info("✒️  Migrated stored script '%s'", sid)
    except Exception as e:
        logger.error("Error migrating stored scripts: %s", e)

def migrate_enrich_policies():
    """
    Migrate enrich policies from source to target.
    """
    try:
        policies = es_source.enrich.get_policy().get("policies", [])
        for pol in policies:
            name = pol["name"]
            es_target.enrich.put_policy(name=name, body=pol)
            logger.info("🌾 Migrated enrich policy '%s'", name)
    except Exception as e:
        logger.error("Error migrating enrich policies: %s", e)

def migrate_transforms():
    """
    Migrate transforms from source to target.
    """
    try:
        transforms = es_source.transform.get_transform()
        for t in transforms.get("transforms", []):
            tid = t["id"]
            cfg = t["config"]
            es_target.transform.put_transform(transform_id=tid, body=cfg)
            logger.info("🔄 Migrated transform '%s'", tid)
    except Exception as e:
        logger.error("Error migrating transforms: %s", e)

def migrate_rollup_jobs():
    """
    Migrate rollup jobs from source to target.
    """
    try:
        jobs = es_source.rollup.get_jobs().get("jobs", [])
        for job in jobs:
            cfg = job["config"]
            jid = cfg["id"]
            es_target.rollup.put_job(id=jid, body=cfg)
            logger.info("📊 Migrated rollup job '%s'", jid)
    except Exception as e:
        logger.error("Error migrating rollup jobs: %s", e)

def migrate_watchers():
    """
    Migrate watchers from source to target.
    """
    try:
        watches = es_source.watcher.get_watch()
        for wid, body in watches.items():
            es_target.watcher.put_watch(id=wid, body=body["watch"])
            logger.info("🔔 Migrated watcher '%s'", wid)
    except Exception as e:
        logger.error("Error migrating watcher watches: %s", e)

def migrate_roles():
    """
    Migrate security roles from source to target.
    """
    try:
        roles = es_source.security.get_role()
        for role, body in roles.items():
            es_target.security.put_role(name=role, body=body)
            logger.info("🔐 Migrated role '%s'", role)
    except Exception as e:
        logger.error("Error migrating roles: %s", e)

def migrate_users():
    """
    Migrate users from source to target.
    """
    try:
        users = es_source.security.get_user()
        for user, body in users.items():
            es_target.security.put_user(username=user, body=body)
            logger.info("👤 Migrated user '%s'", user)
    except Exception as e:
        logger.error("Error migrating users: %s", e)

def migrate_role_mappings():
    """
    Migrate role mappings from source to target.
    """
    try:
        mappings = es_source.security.get_role_mapping()
        for name, body in mappings.items():
            es_target.security.put_role_mapping(name=name, body=body)
            logger.info("🔗 Migrated role mapping '%s'", name)
    except Exception as e:
        logger.error("Error migrating role mappings: %s", e)
