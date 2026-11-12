"""Couche bronze : les articles bruts, en fichiers JSON jamais réécrits.

Un asset par journal, pour que l'échec d'une source n'empêche pas les autres
de tourner et que chaque source ait ses propres relances et son propre check.

Arborescence produite (voir docs/donnees.md pour le détail des champs) :
    $GAZETTE_DATA_DIR/articles_bruts/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.json
    $GAZETTE_DATA_DIR/articles_bruts/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.xml

Le JSON contient une entrée par article nouveau : id, source, lien, titre, resume, publie_le,
recupere_le, rubrique, et payload (l'item tel que feedparser l'a lu, sans nettoyage : c'est le
rôle de silver). Le XML est la réponse HTTP complète, pour pouvoir tout relire plus tard.

Pas de doublon : un article déjà écrit pour (source, lien) n'est pas réécrit, quel que soit le
jour ou le flux où il avait été collecté (le guid RSS sert de clé quand il n'a pas de lien). Un
flux sans aucun article nouveau n'écrit aucun fichier.

Requêtes conditionnelles : l'ETag et le Last-Modified de la dernière réponse sont gardés dans la
table sources et renvoyés en If-None-Match / If-Modified-Since. Un flux inchangé répond 304 :
rien n'est téléchargé ni écrit.

Erreurs de collecte : une erreur transitoire (5xx, 408, 429, timeout, connexion) se règle par
les relances (backoff de la session HTTP, puis RetryPolicy de Dagster). Une erreur permanente
(404, 410...) n'est pas relancée : elle est comptée dans la table sources, et trois échecs de
suite mettent le flux en sommeil (voir etat.py). Le check sources_actives reste en échec tant
qu'un flux dort.
"""

import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg
import feedparser
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from gazette.bronze.etat import (
    SEUIL_SOMMEIL,
    EtatFlux,
    ecrire_etat,
    lire_etat,
    noter_echec_permanent,
    noter_reussite,
    synchroniser_sources,
)
from gazette.bronze.sources import Flux, Source, charger_sources
from gazette.partitions import partitions_quotidiennes

DATA_DIR = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
DOSSIER_ARTICLES = "articles_bruts"
USER_AGENT = "GazetteBot/0.1 (projet etudiant M2Ops Efrei; lecture des flux RSS publics)"
TIMEOUT_S = 30
HTTP_RETRIES = 3
HTTP_BACKOFF_S = 1
CHECK_AU_MOINS_UN_ARTICLE = "au_moins_un_article"
CHECK_SOURCES_ACTIVES = "sources_actives"
STATUTS_TRANSITOIRES = {408, 429}

relances = dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL)


def est_transitoire(erreur: requests.RequestException) -> bool:
    """Une erreur qui peut passer d'elle-même (503, timeout...) : on réessaie.

    Les autres (404, 410, URL invalide...) ne passeront pas en réessayant.
    """
    if isinstance(erreur, requests.HTTPError):
        statut = erreur.response.status_code
        return statut >= 500 or statut in STATUTS_TRANSITOIRES
    return isinstance(erreur, requests.ConnectionError | requests.Timeout)


def ecrire_immuable(chemin: Path, contenu: bytes) -> None:
    """Écrit un fichier qui ne doit jamais exister déjà : le brut ne se réécrit pas."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with open(chemin, "xb") as f:
        f.write(contenu)


def _vers_json(valeur):
    """Ce que json ne sait pas écrire dans un item feedparser : surtout les dates struct_time."""
    if isinstance(valeur, time.struct_time):
        return datetime(*valeur[:6], tzinfo=timezone.utc).isoformat()
    return str(valeur)


def en_json(articles: list[dict]) -> bytes:
    return json.dumps(articles, ensure_ascii=False, indent=2, default=_vers_json).encode("utf-8")


def cle_article(entree: dict) -> str | None:
    """Clé naturelle d'un article au sein d'une source : son lien, à défaut son guid RSS."""
    return entree.get("link") or entree.get("id")


def id_article(source: str, cle: str | None) -> str:
    """Identifiant stable, dérivé de la clé (source, lien) ; aléatoire si l'article n'en a pas."""
    if cle is None:
        return uuid.uuid4().hex
    return hashlib.sha256(f"{source}\n{cle}".encode("utf-8")).hexdigest()[:32]


def date_iso(entree: dict) -> str | None:
    """Date de publication en ISO 8601 UTC, quel que soit le format d'origine (RSS pubDate, Atom
    published ou updated). feedparser fait l'analyse ; à défaut, la chaîne telle quelle."""
    for champ in ("published", "updated"):
        analysee = entree.get(f"{champ}_parsed")
        if analysee:
            return datetime(*analysee[:6], tzinfo=timezone.utc).isoformat()
    return entree.get("published") or entree.get("updated")


def article_brut(source: str, flux: Flux, entree: dict, recupere_le: datetime) -> dict:
    tags = [t.get("term") for t in entree.get("tags", []) if t.get("term")]
    return {
        "id": id_article(source, cle_article(entree)),
        "source": source,
        "flux": flux.nom,
        "lien": entree.get("link"),
        "titre": entree.get("title"),
        "resume": entree.get("summary"),
        "publie_le": date_iso(entree),
        "recupere_le": recupere_le.isoformat(),
        "rubrique": tags[0] if tags else None,
        "payload": dict(entree),
    }


def cles_deja_ecrites(slug: str) -> set[str]:
    """Clés des articles déjà écrits pour cette source, tous jours confondus."""
    cles = set()
    for fichier in (DATA_DIR / DOSSIER_ARTICLES).glob(f"date=*/{slug}/*.json"):
        for article in json.loads(fichier.read_text(encoding="utf-8")):
            cle = article.get("lien") or (article.get("payload") or {}).get("id")
            if cle:
                cles.add(cle)
    return cles


def articles_nouveaux(entrees: list, cles_connues: set[str]) -> list[tuple[int, dict]]:
    """Les (rang, article) jamais écrits ; leurs clés rejoignent `cles_connues`.

    Le rang reste celui de l'article dans le flux. Un article sans lien ni id ne peut pas
    être reconnu : il est toujours gardé.
    """
    nouveaux = []
    for rang, entree in enumerate(entrees):
        cle = cle_article(entree)
        if cle is not None:
            if cle in cles_connues:
                continue
            cles_connues.add(cle)
        nouveaux.append((rang, entree))
    return nouveaux


def creer_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        status=HTTP_RETRIES,
        backoff_factor=HTTP_BACKOFF_S,
        status_forcelist=(503,),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def entetes_conditionnels(etat: EtatFlux) -> dict[str, str]:
    entetes = {}
    if etat.etag:
        entetes["If-None-Match"] = etat.etag
    if etat.last_modified:
        entetes["If-Modified-Since"] = etat.last_modified
    return entetes


def collecter_flux(
    session: requests.Session,
    source: str,
    flux: Flux,
    etat: EtatFlux,
    dossier: Path,
    cles_connues: set[str],
) -> dict:
    """Interroge un flux et écrit ses articles nouveaux ; met à jour etag et last_modified."""
    reponse = session.get(flux.url, timeout=TIMEOUT_S, headers=entetes_conditionnels(etat))
    reponse.raise_for_status()
    recupere_le = datetime.now(timezone.utc)
    vide = {"nb_recus": 0, "nb_ecrits": 0, "nb_ignores": 0, "fichier": None}

    if reponse.status_code == 304:  # rien de neuf depuis la dernière réponse
        return {**vide, "non_modifie": True, "octets": 0}

    etat.etag = reponse.headers.get("ETag")
    etat.last_modified = reponse.headers.get("Last-Modified")
    entrees = feedparser.parse(reponse.content).entries
    nouveaux = articles_nouveaux(entrees, cles_connues)
    resultat = {
        **vide,
        "non_modifie": False,
        "octets": len(reponse.content),
        "nb_recus": len(entrees),
        "nb_ecrits": len(nouveaux),
        "nb_ignores": len(entrees) - len(nouveaux),
    }
    if not nouveaux:
        return resultat

    articles = [article_brut(source, flux, entree, recupere_le) for _, entree in nouveaux]
    base = dossier / f"{flux.nom}_{recupere_le:%Y%m%dT%H%M%S%fZ}"
    fichier = Path(f"{base}.json")
    ecrire_immuable(Path(f"{base}.xml"), reponse.content)
    ecrire_immuable(fichier, en_json(articles))
    return {**resultat, "fichier": str(fichier)}


def construire_asset_bronze(source: Source) -> dg.AssetsDefinition:
    cle = dg.AssetKey(["bronze", source.slug])

    @dg.asset(
        key=cle,
        group_name="bronze",
        partitions_def=partitions_quotidiennes,
        retry_policy=relances,
        description=f"Articles bruts de {source.nom} ({source.type}, {source.region}), JSON immuable.",
        metadata={"journal": source.nom, "type": source.type, "region": source.region},
        check_specs=[
            dg.AssetCheckSpec(
                CHECK_AU_MOINS_UN_ARTICLE,
                asset=cle,
                description="La source a renvoyé au moins un article, ou répondu 304 (inchangée).",
            ),
            dg.AssetCheckSpec(
                CHECK_SOURCES_ACTIVES,
                asset=cle,
                description="Aucun flux de la source n'est en sommeil.",
            ),
        ],
    )
    def _bronze(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        date = context.partition_key
        if date != datetime.now(ZoneInfo("Europe/Paris")).date().isoformat():
            context.log.warning(
                f"Partition {date} rejouée un autre jour : un flux RSS ne donne que ses "
                "articles du moment, les fichiers collectés reflètent donc le flux actuel."
            )

        dossier = DATA_DIR / DOSSIER_ARTICLES / f"date={date}" / source.slug
        debut = time.monotonic()
        synchroniser_sources(DATA_DIR, [source])
        cles_connues = cles_deja_ecrites(source.slug)
        etats = {flux.nom: lire_etat(DATA_DIR, source.slug, flux.nom) for flux in source.flux}
        flux_actifs = [flux for flux in source.flux if etats[flux.nom].actif]
        flux_en_sommeil = [flux.nom for flux in source.flux if not etats[flux.nom].actif]
        if flux_en_sommeil:
            context.log.warning(
                f"Flux en sommeil, non contactés : {', '.join(flux_en_sommeil)}. "
                f"Réactivation : python -m gazette.reactiver {source.slug}"
            )

        collectes, echecs, erreur_transitoire = [], [], False
        with creer_session() as session:
            session.headers["User-Agent"] = USER_AGENT
            for flux in flux_actifs:
                etat = etats[flux.nom]
                try:
                    collectes.append(
                        collecter_flux(session, source.slug, flux, etat, dossier, cles_connues)
                    )
                except requests.RequestException as erreur:
                    context.log.warning(f"Flux {flux.nom} ({flux.url}) en échec : {erreur}")
                    echecs.append(f"{flux.nom} : {erreur}")
                    if est_transitoire(erreur):
                        erreur_transitoire = True
                    elif context.retry_number == 0:  # une relance ne recompte pas le même échec
                        if noter_echec_permanent(etat, str(erreur)):
                            flux_en_sommeil.append(flux.nom)
                            context.log.warning(
                                f"Flux {flux.nom} en sommeil : {SEUIL_SOMMEIL} échecs "
                                "permanents de suite."
                            )
                        ecrire_etat(DATA_DIR, source.slug, flux.nom, etat)
                else:
                    noter_reussite(etat, datetime.now(timezone.utc).isoformat())
                    ecrire_etat(DATA_DIR, source.slug, flux.nom, etat)
        duree_s = round(time.monotonic() - debut, 2)

        if flux_actifs and not collectes:
            raise dg.Failure(
                description=f"Aucun flux de {source.nom} n'a répondu.",
                metadata={
                    "flux_en_echec": echecs,
                    "flux_en_sommeil": flux_en_sommeil,
                    "duree_s": duree_s,
                },
                # Transitoire : les relances de Dagster. Permanent : inutile de réessayer.
                allow_retries=erreur_transitoire,
            )

        nb_recus = sum(c["nb_recus"] for c in collectes)
        nb_ecrits = sum(c["nb_ecrits"] for c in collectes)
        nb_ignores = sum(c["nb_ignores"] for c in collectes)
        nb_non_modifies = sum(c["non_modifie"] for c in collectes)
        # La ligne de journal de la source : ce qu'elle a reçu, écrit, ignoré, et en combien de temps.
        context.log.info(
            f"{source.nom} : {nb_recus} reçus, {nb_ecrits} écrits, {nb_ignores} ignorés, "
            f"{nb_non_modifies} flux non modifiés (304), {len(echecs)} en échec, en {duree_s} s"
        )
        return dg.MaterializeResult(
            metadata={
                "nb_recus": nb_recus,
                "nb_ecrits": nb_ecrits,
                "nb_ignores": nb_ignores,
                "nb_non_modifies": nb_non_modifies,
                "nb_flux_ok": len(collectes),
                "nb_flux_en_echec": len(echecs),
                "flux_en_echec": echecs,
                "flux_en_sommeil": flux_en_sommeil,
                "duree_s": duree_s,
                "octets": sum(c["octets"] for c in collectes),
                "fichiers": [c["fichier"] for c in collectes if c["fichier"]],
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name=CHECK_AU_MOINS_UN_ARTICLE,
                    passed=nb_recus > 0 or nb_non_modifies > 0,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={"nb_recus": nb_recus, "nb_non_modifies": nb_non_modifies},
                ),
                dg.AssetCheckResult(
                    check_name=CHECK_SOURCES_ACTIVES,
                    passed=not flux_en_sommeil,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={
                        "flux_en_sommeil": flux_en_sommeil,
                        "reactiver": f"python -m gazette.reactiver {source.slug}",
                    },
                ),
            ],
        )

    return _bronze


assets_bronze = [construire_asset_bronze(source) for source in charger_sources()]
