import dagster as dg

FUSEAU = "Europe/Paris"

# Une partition par jour, heure de Paris. end_offset=1 rend la partition du jour disponible
# dès minuit : chaque collecte de la journée (toutes les 30 min) écrit dans la date du jour.
partitions_quotidiennes = dg.DailyPartitionsDefinition(
    start_date="2026-10-19",
    timezone=FUSEAU,
    end_offset=1,
)
