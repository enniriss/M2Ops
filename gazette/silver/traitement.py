"""Un passage silver : les JSON bronze pas encore traités → articles_propres ou rejets.

    bronze (articles_bruts/date=J/<slug>/*.json)
      → normalisation (normalisation.py)
      → règles d'exclusion (regles.py, regles.toml)
      → articles_propres  |  rejets (avec la règle)
      → passages (les compteurs du passage), fichiers_traites

Tout le passage se fait sous verrou d'écriture (BEGIN IMMEDIATE) : deux passages lancés en même
temps ne traitent pas deux fois le même fichier, et un passage interrompu ne laisse rien à moitié.
Rejouer un passage ne change rien : un fichier traité ne l'est plus, un id déjà rangé est ignoré.
"""

import json
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

from gazette.bronze.assets import DOSSIER_ARTICLES
from gazette.db import connecter
from gazette.silver.normalisation import normaliser_article
from gazette.silver.regles import Config, Memoire, Verdict, trier


@dataclass
class Passage:
    id: int
    partition: str
    entres: int = 0
    gardes: int = 0
    rejetes: int = 0
    fichiers: int = 0
    par_regle: Counter = field(default_factory=Counter)
    par_source: dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    exemples: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))


def fichiers_bronze(racine: Path, partition: str) -> list[Path]:
    return sorted((racine / DOSSIER_ARTICLES / f"date={partition}").glob("*/*.json"))


def memoire_recente(connexion, depuis: str) -> Memoire:
    """Les articles gardés depuis `depuis` : la référence des doublons (R10 à R12)."""
    memoire = Memoire()
    for ligne in connexion.execute(
        "SELECT id, source, lien, titre, titre_cle FROM articles_propres "
        "WHERE recupere_le >= ? ORDER BY publie_le, id",
        (depuis,),
    ):
        memoire.ajouter(dict(ligne))
    return memoire


def _ranger(connexion, article: dict, verdict: Verdict | None, partition: str, passage_id: int):
    commun = (article["id"], article["source"], article["flux"], article["titre"])
    if verdict is None:
        connexion.execute(
            "INSERT OR IGNORE INTO articles_propres (id, source, flux, titre, extrait, lien, "
            "publie_le, recupere_le, rubrique, titre_cle, partition, fichier_brut, passage_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*commun, article["extrait"], article["lien"], article["publie_le"],
             article["recupere_le"], article["rubrique"], article["titre_cle"], partition,
             article["fichier_brut"], passage_id),
        )  # fmt: skip
    else:
        connexion.execute(
            "INSERT OR IGNORE INTO rejets (id, source, flux, titre, lien, publie_le, recupere_le, "
            "regle, detail, doublon_de, partition, fichier_brut, passage_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*commun, article["lien"], article["publie_le"], article["recupere_le"],
             verdict.regle, verdict.detail, verdict.doublon_de, partition,
             article["fichier_brut"], passage_id),
        )  # fmt: skip


def passer(
    racine: Path,
    partition: str,
    config: Config,
    run_id: str | None = None,
    maintenant: datetime | None = None,
) -> Passage:
    """Traite les fichiers bronze de la partition qui ne l'ont pas encore été."""
    maintenant = maintenant or datetime.now(timezone.utc)
    with closing(connecter(racine)) as connexion:
        connexion.execute("BEGIN IMMEDIATE")
        try:
            passage_id = connexion.execute(
                "INSERT INTO passages (partition, debut, run_id) VALUES (?, ?, ?)",
                (partition, maintenant.isoformat(), run_id),
            ).lastrowid
            passage = Passage(passage_id, partition)

            deja = {f for (f,) in connexion.execute("SELECT fichier FROM fichiers_traites")}
            a_traiter = [
                f for f in fichiers_bronze(racine, partition)
                if f.relative_to(racine).as_posix() not in deja
            ]  # fmt: skip
            articles = []
            for fichier in a_traiter:
                relatif = fichier.relative_to(racine).as_posix()
                for brut in json.loads(fichier.read_text(encoding="utf-8")):
                    articles.append(normaliser_article(brut, relatif))
            depuis = maintenant - timedelta(hours=config.seuils["fenetre_doublons_h"])
            memoire = memoire_recente(connexion, depuis.isoformat())

            for article, verdict in trier(articles, memoire, config):
                _ranger(connexion, article, verdict, partition, passage_id)
                compteur = passage.par_source[article["source"]]
                compteur["entres"] += 1
                if verdict is None:
                    passage.gardes += 1
                    compteur["gardes"] += 1
                else:
                    passage.rejetes += 1
                    compteur["rejetes"] += 1
                    compteur[verdict.regle] += 1
                    passage.par_regle[verdict.regle] += 1
                    if len(passage.exemples[verdict.regle]) < 3:
                        passage.exemples[verdict.regle].append(article["titre"])
            passage.entres = len(articles)
            passage.fichiers = len(a_traiter)

            connexion.executemany(
                "INSERT INTO fichiers_traites (fichier, passage_id) VALUES (?, ?)",
                [(f.relative_to(racine).as_posix(), passage_id) for f in a_traiter],
            )
            connexion.execute(
                "UPDATE passages SET fin = ?, entres = ?, gardes = ?, rejetes = ? WHERE id = ?",
                (datetime.now(timezone.utc).isoformat(), passage.entres, passage.gardes,
                 passage.rejetes, passage_id),
            )  # fmt: skip
            connexion.execute("COMMIT")
        except BaseException:
            connexion.execute("ROLLBACK")
            raise
    return passage
