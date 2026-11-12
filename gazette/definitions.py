import dagster as dg

from gazette import alerte
from gazette.bronze import assets_bronze
from gazette.partitions import FUSEAU
from gazette.silver import assets_silver

job_collecte = dg.define_asset_job(
    "gazette_collecte",
    selection=dg.AssetSelection.groups("bronze"),  # partition journalière reprise des assets
)

job_qualite = dg.define_asset_job(
    "gazette_qualite",
    selection=dg.AssetSelection.groups("silver"),
)


# Toutes les 30 min, heure de Paris, sans que personne n'appuie : assez pour la presse
# quotidienne, raisonnable pour les éditeurs (un flux inchangé répond 304). Chaque passage
# remplit la partition du jour ; les articles déjà collectés plus tôt sont ignorés.
@dg.schedule(
    cron_schedule="*/30 * * * *",
    job=job_collecte,
    execution_timezone=FUSEAU,
    default_status=dg.DefaultScheduleStatus.RUNNING,
)
def collecte_toutes_les_30_min(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    heure = context.scheduled_execution_time
    return dg.RunRequest(
        run_key=heure.isoformat(),  # un seul run par créneau, même si le démon réévalue
        partition_key=heure.date().isoformat(),
    )


def _qualite_apres(context: dg.RunStatusSensorContext) -> dg.RunRequest | dg.SkipReason:
    run = context.dagster_run
    partition = run.tags.get("dagster/partition")
    if not partition:
        return dg.SkipReason(f"Run {run.run_id} sans partition : rien à trier.")
    return dg.RunRequest(run_key=run.run_id, partition_key=partition)


# Après chaque passage de la collecte, réussi ou non (une source en panne n'empêche pas de trier
# les autres) : tri silver, contrôles, rapport. Un capteur par issue du run.
@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.SUCCESS,
    monitored_jobs=[job_collecte],
    request_job=job_qualite,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def qualite_apres_collecte(context: dg.RunStatusSensorContext):
    return _qualite_apres(context)


@dg.run_status_sensor(
    run_status=dg.DagsterRunStatus.FAILURE,
    monitored_jobs=[job_collecte],
    request_job=job_qualite,
    default_status=dg.DefaultSensorStatus.RUNNING,
)
def qualite_apres_collecte_en_echec(context: dg.RunStatusSensorContext):
    return _qualite_apres(context)


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING)
def alerte_run_en_echec(context: dg.RunFailureSensorContext) -> None:
    """Tout run en échec (collecte ou qualité) part sur le canal de l'équipe."""
    run = context.dagster_run
    message = (
        f"🔴 Gazette · run {run.job_name} en échec "
        f"(partition {run.tags.get('dagster/partition', '–')}, id {run.run_id[:8]}) : "
        f"{context.failure_event.message or 'voir les journaux dans Dagster'}"
    )
    if not alerte.envoyer(message):
        context.log.warning(f"{alerte.WEBHOOK_ENV} non défini, alerte non envoyée : {message}")


defs = dg.Definitions(
    assets=[*assets_bronze, *assets_silver],
    jobs=[job_collecte, job_qualite],
    schedules=[collecte_toutes_les_30_min],
    sensors=[qualite_apres_collecte, qualite_apres_collecte_en_echec, alerte_run_en_echec],
)
