"""État des flux entre deux runs : la table `sources` de gazette.db (migrations/001_sources.sql).

Une ligne par flux de sources.csv, recopiée au début de chaque run (synchroniser_sources) :
le CSV reste le catalogue, la table porte l'état de collecte.

Seuls les échecs permanents (404, 410...) comptent : après SEUIL_SOMMEIL échecs de suite, le flux
passe à actif = 0 et n'est plus contacté tant que quelqu'un ne l'a pas réactivé
(python -m gazette.reactiver). Une erreur transitoire (503, timeout...) ne change rien :
elle se règle par les relances.

etag et last_modified gardent les en-têtes de la dernière réponse 200, renvoyés au run suivant
en If-None-Match / If-Modified-Since : un flux inchangé répond 304 et n'est pas retéléchargé.
"""

from contextlib import closing
from dataclasses import dataclass, fields
from pathlib import Path

from gazette.bronze.sources import Source
from gazette.db import connecter

SEUIL_SOMMEIL = 3


@dataclass
class EtatFlux:
    actif: bool = True
    echecs_consecutifs: int = 0
    derniere_reussite: str | None = None
    derniere_erreur: str | None = None
    etag: str | None = None
    last_modified: str | None = None


COLONNES_ETAT = [f.name for f in fields(EtatFlux)]


def synchroniser_sources(racine: Path, sources: list[Source]) -> None:
    """Ajoute les flux nouveaux du catalogue et met à jour leur description, sans toucher à
    l'état. Un flux dont l'URL change repart sans ETag ni Last-Modified."""
    with closing(connecter(racine)) as connexion:
        connexion.execute("BEGIN")
        for source in sources:
            for flux in source.flux:
                connexion.execute(
                    """
                    INSERT INTO sources (slug, nom, type, region, flux, url_flux)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (slug, flux) DO UPDATE SET
                        nom = excluded.nom,
                        type = excluded.type,
                        region = excluded.region,
                        etag = CASE WHEN url_flux = excluded.url_flux THEN etag END,
                        last_modified = CASE WHEN url_flux = excluded.url_flux
                                             THEN last_modified END,
                        url_flux = excluded.url_flux
                    """,
                    (source.slug, source.nom, source.type, source.region, flux.nom, flux.url),
                )
        connexion.execute("COMMIT")


def lire_etat(racine: Path, slug: str, flux: str) -> EtatFlux:
    """L'état du flux ; un flux jamais vu est actif, sans échec."""
    with closing(connecter(racine)) as connexion:
        ligne = connexion.execute(
            f"SELECT {', '.join(COLONNES_ETAT)} FROM sources WHERE slug = ? AND flux = ?",
            (slug, flux),
        ).fetchone()
    if ligne is None:
        return EtatFlux()
    etat = EtatFlux(**dict(ligne))
    etat.actif = bool(etat.actif)
    return etat


def ecrire_etat(racine: Path, slug: str, flux: str, etat: EtatFlux) -> None:
    """Enregistre l'état d'un flux déjà présent dans la table (voir synchroniser_sources)."""
    with closing(connecter(racine)) as connexion:
        curseur = connexion.execute(
            f"UPDATE sources SET {', '.join(f'{c} = ?' for c in COLONNES_ETAT)} "
            "WHERE slug = ? AND flux = ?",
            (*(getattr(etat, c) for c in COLONNES_ETAT), slug, flux),
        )
    if curseur.rowcount == 0:
        raise KeyError(f"Flux inconnu de la table sources : {slug}/{flux}")


def noter_reussite(etat: EtatFlux, quand: str) -> None:
    etat.echecs_consecutifs = 0
    etat.derniere_reussite = quand
    etat.derniere_erreur = None


def noter_echec_permanent(etat: EtatFlux, erreur: str) -> bool:
    """Compte l'échec ; renvoie True si le flux vient de passer en sommeil."""
    etat.echecs_consecutifs += 1
    etat.derniere_erreur = erreur
    if etat.actif and etat.echecs_consecutifs >= SEUIL_SOMMEIL:
        etat.actif = False
        return True
    return False


def reactiver(racine: Path, slug: str, flux: str | None = None) -> list[str]:
    """Remet en service les flux connus de la source (ou le seul `flux`) ; renvoie leurs noms."""
    filtre, parametres = "slug = ?", [slug]
    if flux:
        filtre, parametres = "slug = ? AND flux = ?", [slug, flux]
    with closing(connecter(racine)) as connexion:
        noms = [
            nom
            for (nom,) in connexion.execute(
                f"SELECT flux FROM sources WHERE {filtre} ORDER BY flux", parametres
            )
        ]
        connexion.execute(
            f"UPDATE sources SET actif = 1, echecs_consecutifs = 0 WHERE {filtre}", parametres
        )
    return noms
