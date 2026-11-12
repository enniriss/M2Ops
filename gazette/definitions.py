import dagster as dg

from gazette.bronze import assets_bronze
from gazette.partitions import FUSEAU

job_collecte = dg.define_asset_job(
    "gazette_collecte",
    selection=dg.AssetSelection.groups("bronze"),  # partition journalière reprise des assets
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


defs = dg.Definitions(
    assets=assets_bronze,
    jobs=[job_collecte],
    schedules=[collecte_toutes_les_30_min],
)
