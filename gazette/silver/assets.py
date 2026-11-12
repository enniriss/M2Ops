"""Couche silver dans Dagster : deux assets, lancés après chaque passage de la collecte.

    bronze/<slug> (×8) ──► silver/articles_propres ──► silver/rapport_qualite
                           normalise, applique les       contrôles (asset checks), métriques,
                           règles, range propres et      alerte si un fait devient faux,
                           rejets                        rapport du jour archivé

Le job gazette_qualite est lancé par un capteur à la fin de chaque run de collecte, qu'il ait
réussi ou non : une source en panne ne doit pas empêcher de trier celles qui ont répondu.
"""

import os
from contextlib import closing
from pathlib import Path

import dagster as dg

from gazette import alerte
from gazette.bronze import assets_bronze
from gazette.db import connecter
from gazette.partitions import partitions_quotidiennes
from gazette.silver import rapport, traitement
from gazette.silver.controles import NOMS_CONTROLES, controler
from gazette.silver.regles import charger_regles

CLE_PROPRES = dg.AssetKey(["silver", "articles_propres"])
CLE_RAPPORT = dg.AssetKey(["silver", "rapport_qualite"])


def racine_donnees() -> Path:
    return Path(os.environ.get("GAZETTE_DATA_DIR", "data"))


@dg.asset(
    key=CLE_PROPRES,
    group_name="silver",
    partitions_def=partitions_quotidiennes,
    deps=[a.key for a in assets_bronze],
    description="Articles normalisés et triés : gardés dans articles_propres, écartés dans "
    "rejets avec leur règle (gazette/silver/regles.toml).",
)
def articles_propres(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    config = charger_regles()
    passage = traitement.passer(
        racine_donnees(), context.partition_key, config, run_id=context.run_id
    )
    context.log.info(
        f"Passage {passage.id} : {passage.fichiers} fichiers bronze, {passage.entres} entrés, "
        f"{passage.gardes} gardés, {passage.rejetes} rejetés "
        f"({', '.join(f'{r} {n}' for r, n in passage.par_regle.most_common()) or 'aucun rejet'})"
    )
    return dg.MaterializeResult(
        metadata={
            "passage": passage.id,
            "fichiers_bronze": passage.fichiers,
            "entres": passage.entres,
            "gardes": passage.gardes,
            "rejetes": passage.rejetes,
            "taux_rejet": round(passage.rejetes / passage.entres, 3) if passage.entres else 0.0,
            # Une métrique par règle, à zéro si elle n'a rien écarté : Dagster en trace l'historique.
            **{f"rejets_{r.id}": passage.par_regle.get(r.id, 0) for r in config.regles},
            "par_source": dg.MetadataValue.json(
                {s: dict(c) for s, c in passage.par_source.items()}
            ),
        }
    )


@dg.asset(
    key=CLE_RAPPORT,
    group_name="silver",
    partitions_def=partitions_quotidiennes,
    deps=[CLE_PROPRES],
    description="Contrôles de qualité sur les vraies données, alerte, rapport du jour "
    "(rapports/qualite-AAAA-MM-JJ.md).",
    check_specs=[dg.AssetCheckSpec(nom, asset=CLE_RAPPORT) for nom in NOMS_CONTROLES],
)
def rapport_qualite(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    racine, partition, config = racine_donnees(), context.partition_key, charger_regles()
    with closing(connecter(racine)) as connexion:
        dernier = connexion.execute(
            "SELECT id FROM passages WHERE partition = ? ORDER BY id DESC LIMIT 1", (partition,)
        ).fetchone()
        passage = _relire_passage(connexion, dernier["id"]) if dernier else None
        resultats = controler(connexion, partition, config)
        jour = rapport.donnees_du_jour(connexion, partition)
        messages = alerte.changements(connexion, resultats)

    for message in messages:
        context.log.warning(message)
        try:
            if not alerte.envoyer(message):
                context.log.warning(f"Alerte non envoyée : {alerte.WEBHOOK_ENV} n'est pas défini.")
        except Exception as erreur:  # noqa: BLE001 — l'alerte ne doit pas faire tomber le rapport
            context.log.error(f"Alerte non envoyée : {erreur}")

    passage = passage or traitement.Passage(0, partition)
    chemin = rapport.ecrire_rapport(racine, partition, passage, jour, resultats, config)
    context.log.info(f"Rapport écrit : {chemin}")
    return dg.MaterializeResult(
        metadata={
            "rapport": str(chemin),
            "entres_jour": jour["entres"],
            "gardes_jour": jour["gardes"],
            "rejetes_jour": jour["rejetes"],
            "taux_rejet_jour": round(jour["rejetes"] / jour["entres"], 3) if jour["entres"] else 0.0,
            "controles_en_echec": sum(not r.ok for r in resultats),
            "alertes_envoyees": len(messages),
            "top5": dg.MetadataValue.json(jour["top5"]),
            "apercu": dg.MetadataValue.md(chemin.read_text(encoding="utf-8")),
        },
        check_results=[
            dg.AssetCheckResult(
                check_name=r.nom,
                passed=r.ok,
                severity=dg.AssetCheckSeverity.ERROR,
                description=r.message,
                metadata={k: v for k, v in r.metriques.items() if not isinstance(v, list)}
                | {"exemples": dg.MetadataValue.json(r.metriques.get("exemples", []))},
            )
            for r in resultats
        ],
    )


def _relire_passage(connexion, passage_id: int) -> traitement.Passage:
    """Le dernier passage, recompté depuis la base (le rapport tourne dans une autre étape)."""
    ligne = connexion.execute("SELECT * FROM passages WHERE id = ?", (passage_id,)).fetchone()
    passage = traitement.Passage(
        passage_id, ligne["partition"], ligne["entres"], ligne["gardes"], ligne["rejetes"]
    )
    passage.fichiers = connexion.execute(
        "SELECT count(*) FROM fichiers_traites WHERE passage_id = ?", (passage_id,)
    ).fetchone()[0]
    for table in ("articles_propres", "rejets"):
        colonne = "regle" if table == "rejets" else "NULL"
        for source, regle, nombre in connexion.execute(
            f"SELECT source, {colonne}, count(*) FROM {table} WHERE passage_id = ? "
            "GROUP BY source, 2",
            (passage_id,),
        ):
            compteur = passage.par_source[source]
            compteur["entres"] += nombre
            if regle:
                compteur["rejetes"] += nombre
                compteur[regle] += nombre
                passage.par_regle[regle] += nombre
            else:
                compteur["gardes"] += nombre
    return passage


assets_silver = [articles_propres, rapport_qualite]
