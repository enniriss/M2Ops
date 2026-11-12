"""Les contrôles de qualité : des faits vérifiés sur les vraies données, après chaque passage.

Ils ne testent pas le code (c'est le rôle de tests/ et de la CI) : ils relisent articles_propres
et rejets tels qu'ils sont en base, et disent si un fait est faux. Lancés par Dagster après
chaque passage (asset silver/rapport_qualite), leurs chiffres partent en métadonnées (métriques),
et un contrôle qui devient faux déclenche une alerte (alerte.py).

Assertions maison plutôt qu'un outil dédié (Great Expectations, Soda) : cinq requêtes SQL sur
une base SQLite, sans service ni dépendance de plus, et Dagster sait déjà afficher, historiser
et tracer des asset checks. Voir docs/qualite.md.
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta

from gazette.silver.normalisation import encodage_casse
from gazette.silver.regles import Config


@dataclass
class Resultat:
    nom: str
    ok: bool
    message: str
    metriques: dict = field(default_factory=dict)


def aucune_date_future(connexion, partition: str, config: Config) -> Resultat:
    tolerance = timedelta(minutes=config.seuils["avance_toleree_min"])
    fautifs = [
        ligne["id"]
        for ligne in connexion.execute(
            "SELECT id, publie_le, recupere_le FROM articles_propres "
            "WHERE partition = ? AND publie_le IS NOT NULL",
            (partition,),
        )
        if datetime.fromisoformat(ligne["publie_le"])
        > datetime.fromisoformat(ligne["recupere_le"]) + tolerance
    ]
    return Resultat(
        "aucune_date_future",
        not fautifs,
        f"{len(fautifs)} article(s) propre(s) daté(s) dans le futur" if fautifs
        else "aucune date future",
        {"nb_dates_futures": len(fautifs), "exemples": fautifs[:5]},
    )


def aucun_titre_vide(connexion, partition: str, config: Config) -> Resultat:
    minimum = config.seuils["longueur_titre_min"]
    fautifs = [
        i for (i,) in connexion.execute(
            "SELECT id FROM articles_propres WHERE partition = ? AND length(trim(titre)) < ?",
            (partition, minimum),
        )
    ]  # fmt: skip
    return Resultat(
        "aucun_titre_vide",
        not fautifs,
        f"{len(fautifs)} titre(s) vide(s) ou trop court(s)" if fautifs else "aucun titre vide",
        {"nb_titres_vides": len(fautifs), "exemples": fautifs[:5]},
    )


def taux_par_source(connexion, partition: str) -> dict[str, tuple[int, int]]:
    """source → (entrés, rejetés) pour la journée."""
    comptes: dict[str, list[int]] = {}
    for table, rang in (("articles_propres", 0), ("rejets", 1)):
        for source, nombre in connexion.execute(
            f"SELECT source, count(*) FROM {table} WHERE partition = ? GROUP BY source",
            (partition,),
        ):
            comptes.setdefault(source, [0, 0])[rang] = nombre
    return {s: (gardes + rejetes, rejetes) for s, (gardes, rejetes) in sorted(comptes.items())}


def taux_rejet_par_source(connexion, partition: str, config: Config) -> Resultat:
    maximum = config.controles["taux_rejet_max"]
    minimum = config.controles["articles_min_taux"]
    taux = {
        source: round(rejetes / entres, 3)
        for source, (entres, rejetes) in taux_par_source(connexion, partition).items()
        if entres >= minimum
    }
    trop = {s: t for s, t in taux.items() if t > maximum}
    return Resultat(
        "taux_rejet_par_source",
        not trop,
        ", ".join(f"{s} rejette {t:.0%} de ses articles" for s, t in trop.items())
        + f" (seuil {maximum:.0%})" if trop else f"toutes les sources sous {maximum:.0%} de rejet",
        {"seuil": maximum, **{f"taux_rejet_{s}": t for s, t in taux.items()}},
    )


def aucun_encodage_casse(connexion, partition: str, config: Config) -> Resultat:
    fautifs = [
        ligne["id"]
        for ligne in connexion.execute(
            "SELECT id, titre, extrait FROM articles_propres WHERE partition = ?", (partition,)
        )
        if encodage_casse(ligne["titre"]) or encodage_casse(ligne["extrait"])
    ]
    return Resultat(
        "aucun_encodage_casse",
        not fautifs,
        f"{len(fautifs)} article(s) à l'encodage cassé" if fautifs else "aucun encodage cassé",
        {"nb_encodages_casses": len(fautifs), "exemples": fautifs[:5]},
    )


def aucun_lien_en_double(connexion, partition: str, config: Config) -> Resultat:
    doubles = [
        lien for (lien,) in connexion.execute(
            "SELECT lien FROM articles_propres WHERE lien IS NOT NULL "
            "GROUP BY lien HAVING count(*) > 1"
        )
    ]  # fmt: skip
    return Resultat(
        "aucun_lien_en_double",
        not doubles,
        f"{len(doubles)} lien(s) présent(s) deux fois dans articles_propres" if doubles
        else "aucun lien en double",
        {"nb_liens_en_double": len(doubles), "exemples": doubles[:5]},
    )


CONTROLES = (
    aucune_date_future,
    aucun_titre_vide,
    taux_rejet_par_source,
    aucun_encodage_casse,
    aucun_lien_en_double,
)
NOMS_CONTROLES = tuple(c.__name__ for c in CONTROLES)


def controler(connexion, partition: str, config: Config) -> list[Resultat]:
    return [controle(connexion, partition, config) for controle in CONTROLES]
