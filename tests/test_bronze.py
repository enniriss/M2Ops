import json
import sqlite3
import threading
from contextlib import closing
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from zoneinfo import ZoneInfo

import dagster as dg
import pytest
import requests

from gazette import db, reactiver
from gazette.bronze import assets as bronze
from gazette.bronze import assets_bronze
from gazette.bronze.etat import EtatFlux, ecrire_etat, lire_etat, synchroniser_sources
from gazette.bronze.sources import Flux, Source, charger_sources
from gazette.definitions import collecte_toutes_les_30_min, defs

RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Test</title>
<item><title>Premier</title><link>https://exemple.fr/1</link>
<description>Le premier article</description><category>Politique</category>
<pubDate>Thu, 12 Nov 2026 07:30:00 +0100</pubDate><guid>g-1</guid></item>
<item><title>Second</title><link>https://exemple.fr/2</link></item>
</channel></rss>"""

ATOM = b"""<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom"><title>Test</title>
<entry><title>Atome</title><link href="https://exemple.fr/a"/><id>urn:a</id>
<updated>2026-11-12T06:30:00Z</updated><summary>Un resume Atom</summary>
<category term="Sport"/></entry>
</feed>"""

CHAMPS_ARTICLE = {
    "id", "source", "flux", "lien", "titre", "resume",
    "publie_le", "recupere_le", "rubrique", "payload",
}  # fmt: skip


def rss_de(*liens: str) -> bytes:
    items = "".join(f"<item><title>Article {u}</title><link>{u}</link></item>" for u in liens)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<rss version="2.0"><channel><title>Test</title>{items}</channel></rss>'
    ).encode()


def reponse_rss(url: str, contenu: bytes = RSS, statut: int = 200) -> requests.Response:
    reponse = requests.Response()
    reponse.status_code = statut
    reponse._content = contenu
    reponse.url = url
    reponse.headers["Content-Type"] = "application/rss+xml"
    return reponse


def erreur_http(statut: int) -> requests.HTTPError:
    reponse = requests.Response()
    reponse.status_code = statut
    return requests.HTTPError(response=reponse)


def source_test(*flux: Flux) -> Source:
    return Source("test", "Journal test", "PQN", "National", flux or (Flux("une", "https://exemple.fr/u"),))


def collecter(
    source: Source, partition: str, raise_on_error: bool = True
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [bronze.construire_asset_bronze(source)],
        partition_key=partition,
        raise_on_error=raise_on_error,
    )


def compteurs(resultat: dg.ExecuteInProcessResult, slug: str) -> dict[str, int]:
    metadata = resultat.asset_materializations_for_node(f"bronze__{slug}")[0].metadata
    return {nom: metadata[nom].value for nom in ("nb_recus", "nb_ecrits", "nb_ignores")}


def articles_ecrits(racine) -> list[dict]:
    """Tous les articles bruts, tous fichiers et tous jours confondus."""
    return [
        article
        for fichier in sorted(racine.glob("articles_bruts/date=*/*/*.json"))
        for article in json.loads(fichier.read_text(encoding="utf-8"))
    ]


def liens_ecrits(racine) -> list[str]:
    return [a["lien"] for a in articles_ecrits(racine)]


def verdicts(resultat: dg.ExecuteInProcessResult) -> dict[str, bool]:
    return {c.check_name: c.passed for c in resultat.get_asset_check_evaluations()}


# ── stockage : la migration sources, les fichiers bruts ─────────────────────────────────────


def test_migration_sources_creee_une_seule_fois(tmp_path):
    connexion = db.connecter(tmp_path)
    colonnes = {ligne["name"] for ligne in connexion.execute("PRAGMA table_info(sources)")}
    assert {
        "nom", "type", "url_flux", "actif", "derniere_reussite",
        "echecs_consecutifs", "etag", "last_modified",
    } <= colonnes  # fmt: skip
    assert db.migrer(connexion) == []  # déjà appliquée : rien à refaire
    versions = [v for (v,) in connexion.execute("SELECT version FROM schema_migrations")]
    assert versions == [1, 2]
    connexion.close()


def test_premier_demarrage_huit_sources_en_parallele_sur_une_base_neuve(tmp_path):
    """Au premier run, les 8 assets ouvrent la base en même temps : une seule migration passe,
    les autres attendent puis voient qu'elle est faite (pas de « table already exists »)."""
    depart = threading.Barrier(8)
    erreurs = []

    def ouvrir():
        depart.wait()
        try:
            db.connecter(tmp_path).close()
        except Exception as erreur:  # noqa: BLE001 — on veut voir toute erreur
            erreurs.append(erreur)

    fils = [threading.Thread(target=ouvrir) for _ in range(8)]
    for fil in fils:
        fil.start()
    for fil in fils:
        fil.join()

    assert erreurs == []
    with closing(sqlite3.connect(tmp_path / db.NOM_BASE)) as connexion:
        assert [v for (v,) in connexion.execute("SELECT version FROM schema_migrations")] == [1, 2]


def test_decoupage_du_script_de_migration():
    script = (
        "-- en-tête ; avec un point-virgule\n"
        "CREATE TABLE a (x TEXT DEFAULT 'un;deux'); -- fin de ligne ;\n"
        "INSERT INTO a VALUES ('trois')\n"  # dernière instruction sans point-virgule
    )
    instructions = db.instructions_sql(script)
    assert len(instructions) == 2
    assert "'un;deux'" in instructions[0]
    assert "INSERT INTO a" in instructions[1]


def test_une_migration_en_erreur_ne_laisse_rien(tmp_path):
    (tmp_path / "001_ok.sql").write_text("CREATE TABLE a (x);", encoding="utf-8")
    (tmp_path / "002_ko.sql").write_text("CREATE TABLE b (x); SELECT * FROM inconnue;", encoding="utf-8")
    with closing(sqlite3.connect(tmp_path / "t.db", isolation_level=None)) as connexion:
        with pytest.raises(sqlite3.OperationalError):
            db.migrer(connexion, tmp_path)
        tables = {n for (n,) in connexion.execute("SELECT name FROM sqlite_master")}
        assert tables == {"schema_migrations"}
        assert not connexion.in_transaction


def test_synchroniser_sources_garde_l_etat_et_oublie_l_etag_si_l_url_change(tmp_path):
    source = source_test(Flux("une", "https://exemple.fr/v1"))
    synchroniser_sources(tmp_path, [source])
    ecrire_etat(tmp_path, "test", "une", EtatFlux(echecs_consecutifs=2, etag='"abc"'))

    synchroniser_sources(tmp_path, [source])
    assert lire_etat(tmp_path, "test", "une") == EtatFlux(echecs_consecutifs=2, etag='"abc"')

    synchroniser_sources(tmp_path, [source_test(Flux("une", "https://exemple.fr/v2"))])
    assert lire_etat(tmp_path, "test", "une") == EtatFlux(echecs_consecutifs=2)
    with sqlite3.connect(db.chemin_base(tmp_path)) as connexion:
        assert connexion.execute("SELECT url_flux FROM sources").fetchall() == [
            ("https://exemple.fr/v2",)
        ]


def test_sources_csv_valide():
    sources = charger_sources()
    assert len(sources) == 8
    assert {s.type for s in sources} == {"PQN", "PQR"}
    assert all(f.url.startswith("https://") for s in sources for f in s.flux)


def test_articles_bruts_ecrits_en_json_avec_les_champs_attendus(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kw: reponse_rss(url))

    assert collecter(source_test(), "2026-10-19").success

    dossier = tmp_path / "articles_bruts" / "date=2026-10-19" / "test"
    [fichier] = dossier.glob("*.json")
    assert fichier.name.startswith("une_")
    assert fichier.with_suffix(".xml").read_bytes() == RSS  # la réponse complète, telle quelle
    premier, second = json.loads(fichier.read_text(encoding="utf-8"))
    assert set(premier) == CHAMPS_ARTICLE
    assert premier["source"] == "test"
    assert premier["lien"] == "https://exemple.fr/1"
    assert premier["titre"] == "Premier"
    assert premier["resume"] == "Le premier article"
    assert premier["rubrique"] == "Politique"
    assert premier["publie_le"] == "2026-11-12T06:30:00+00:00"  # pubDate RFC 822 -> ISO UTC
    assert premier["payload"]["id"] == "g-1"
    assert premier["id"] == bronze.id_article("test", "https://exemple.fr/1")
    assert second["rubrique"] is None and second["publie_le"] is None


def test_atom_donne_les_memes_champs_que_rss(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kw: reponse_rss(url, ATOM))

    assert collecter(source_test(), "2026-10-19").success

    [article] = articles_ecrits(tmp_path)
    assert article["lien"] == "https://exemple.fr/a"
    assert article["resume"] == "Un resume Atom"
    assert article["publie_le"] == "2026-11-12T06:30:00+00:00"  # Atom updated
    assert article["rubrique"] == "Sport"


def test_bronze_ne_reecrit_jamais_un_fichier(tmp_path):
    fichier = tmp_path / "a.json"
    bronze.ecrire_immuable(fichier, b"[1]")
    with pytest.raises(FileExistsError):
        bronze.ecrire_immuable(fichier, b"[2]")
    assert fichier.read_bytes() == b"[1]"


# ── le collecteur ────────────────────────────────────────────────────────────────────────────


def test_bronze_ecrit_les_flux_et_isole_les_echecs(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)

    def faux_get(self, url, **kw):
        if "panne" in url:
            raise requests.ConnectionError("injoignable")
        return reponse_rss(url)

    monkeypatch.setattr(requests.Session, "get", faux_get)
    source = source_test(
        Flux("une", "https://exemple.fr/une.xml"), Flux("panne", "https://exemple.fr/panne.xml")
    )

    resultat = dg.materialize([bronze.construire_asset_bronze(source)], partition_key="2026-10-19")

    assert resultat.success
    materialisation = resultat.asset_materializations_for_node("bronze__test")[0]
    assert compteurs(resultat, "test") == {"nb_recus": 2, "nb_ecrits": 2, "nb_ignores": 0}
    assert materialisation.metadata["nb_flux_en_echec"].value == 1
    assert "duree_s" in materialisation.metadata
    assert all(check.passed for check in resultat.get_asset_check_evaluations())
    assert liens_ecrits(tmp_path) == ["https://exemple.fr/1", "https://exemple.fr/2"]


def test_une_ligne_de_journal_par_source(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kw: reponse_rss(url))
    instance = dg.DagsterInstance.ephemeral()

    resultat = dg.materialize(
        [bronze.construire_asset_bronze(source_test())], partition_key="2026-10-19", instance=instance
    )

    lignes = [
        e.user_message for e in instance.all_logs(resultat.run_id) if "Journal test :" in e.user_message
    ]
    assert len(lignes) == 1
    assert "2 reçus, 2 écrits, 0 ignorés" in lignes[0] and lignes[0].endswith(" s")


def test_user_agent_et_timeout_envoyes(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    appels = []

    def faux_get(self, url, **kw):
        appels.append({"ua": self.headers["User-Agent"], **kw})
        return reponse_rss(url)

    monkeypatch.setattr(requests.Session, "get", faux_get)
    collecter(source_test(), "2026-10-19")

    assert appels[0]["ua"] == bronze.USER_AGENT
    assert appels[0]["timeout"] == bronze.TIMEOUT_S
    assert appels[0]["headers"] == {}  # premier passage : rien à comparer


# ── zéro doublon ─────────────────────────────────────────────────────────────────────────────


def test_deux_passages_zero_doublon(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    flux_distant = {"contenu": rss_de("https://exemple.fr/1", "https://exemple.fr/2")}
    monkeypatch.setattr(
        requests.Session,
        "get",
        lambda self, url, **kw: reponse_rss(url, flux_distant["contenu"]),
    )
    source = source_test()

    premier = collecter(source, "2026-10-19")
    assert compteurs(premier, "test") == {"nb_recus": 2, "nb_ecrits": 2, "nb_ignores": 0}
    assert len(articles_ecrits(tmp_path)) == 2

    # Même partition rejouée : rien de nouveau, donc ni fichier ni article de plus.
    fichiers = sorted(tmp_path.glob("articles_bruts/**/*"))
    second = collecter(source, "2026-10-19")
    assert compteurs(second, "test") == {"nb_recus": 2, "nb_ecrits": 0, "nb_ignores": 2}
    assert sorted(tmp_path.glob("articles_bruts/**/*")) == fichiers
    assert len(articles_ecrits(tmp_path)) == 2
    # la source répond : 0 écrit n'est pas une panne
    assert all(check.passed for check in second.get_asset_check_evaluations())

    # Le lendemain, le flux garde les anciens articles et en ajoute un : seul celui-là est écrit.
    flux_distant["contenu"] = rss_de(
        "https://exemple.fr/3", "https://exemple.fr/1", "https://exemple.fr/2"
    )
    troisieme = collecter(source, "2026-10-20")
    assert compteurs(troisieme, "test") == {"nb_recus": 3, "nb_ecrits": 1, "nb_ignores": 2}
    assert sorted(liens_ecrits(tmp_path)) == [f"https://exemple.fr/{n}" for n in (1, 2, 3)]
    ids = [a["id"] for a in articles_ecrits(tmp_path)]
    assert len(ids) == len(set(ids))


def test_un_article_present_dans_deux_flux_n_est_ecrit_qu_une_fois(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    monkeypatch.setattr(requests.Session, "get", lambda self, url, **kw: reponse_rss(url))
    source = source_test(
        Flux("une", "https://exemple.fr/une.xml"), Flux("tout", "https://exemple.fr/tout.xml")
    )

    resultat = collecter(source, "2026-10-19")

    assert compteurs(resultat, "test") == {"nb_recus": 4, "nb_ecrits": 2, "nb_ignores": 2}
    assert len(articles_ecrits(tmp_path)) == 2


def test_cle_de_dedoublonnage_repli_sur_l_id_puis_jamais_ignore():
    connues: set[str] = set()
    entrees = [
        {"link": "https://exemple.fr/1", "id": "a"},
        {"link": "https://exemple.fr/1", "id": "autre"},  # même lien
        {"id": "b"},  # sans lien : l'id sert de clé
        {"id": "b"},
        {},  # ni lien ni id : impossible à reconnaître, toujours gardé
        {},
    ]

    gardes = bronze.articles_nouveaux(entrees, connues)

    assert [rang for rang, _ in gardes] == [0, 2, 4, 5]
    assert connues == {"https://exemple.fr/1", "b"}


# ── reprise et sommeil, sur un vrai serveur HTTP local ──────────────────────────────────────


class ServeurLocal:
    """Vrai serveur HTTP local. `statuts[chemin]` : les statuts à renvoyer, dans l'ordre ; une fois
    la liste épuisée, il répond 200 avec le flux RSS. `appels` garde chaque chemin demandé.
    Si `etag` est défini, un 200 l'envoie et une requête If-None-Match identique reçoit 304."""

    def __init__(self) -> None:
        self.statuts: dict[str, list[int]] = {}
        self.appels: list[str] = []
        self.entetes: list[dict[str, str]] = []
        self.etag: str | None = None
        serveur = self

        class Gestionnaire(BaseHTTPRequestHandler):
            def do_GET(self):
                serveur.appels.append(self.path)
                serveur.entetes.append(dict(self.headers))
                programmes = serveur.statuts.get(self.path, [])
                statut = programmes.pop(0) if programmes else 200
                if statut == 200 and serveur.etag and self.headers.get("If-None-Match") == serveur.etag:
                    statut = 304
                corps = RSS if statut == 200 else b"" if statut == 304 else b"erreur"
                self.send_response(statut)
                self.send_header("Content-Type", "application/rss+xml")
                if serveur.etag and statut in (200, 304):
                    self.send_header("ETag", serveur.etag)
                    self.send_header("Last-Modified", "Thu, 12 Nov 2026 06:00:00 GMT")
                self.send_header("Content-Length", str(len(corps)))
                self.end_headers()
                self.wfile.write(corps)

            def log_message(self, *args):
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Gestionnaire)
        self.url = f"http://127.0.0.1:{self.http.server_port}"
        threading.Thread(target=self.http.serve_forever, daemon=True).start()

    def source(self) -> Source:
        return source_test(Flux("une", f"{self.url}/une"))


@pytest.fixture
def serveur(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    # Mêmes relances que la prod, sans les secondes d'attente entre deux essais ou deux requêtes.
    monkeypatch.setattr(bronze, "HTTP_BACKOFF_S", 0)
    monkeypatch.setattr(
        bronze,
        "relances",
        dg.RetryPolicy(max_retries=3, delay=0.01, backoff=dg.Backoff.EXPONENTIAL),
    )
    serveur = ServeurLocal()
    yield serveur
    serveur.http.shutdown()
    serveur.http.server_close()


def test_en_tetes_conditionnels_un_flux_inchange_repond_304(serveur, tmp_path):
    serveur.etag = '"v1"'

    premier = collecter(serveur.source(), "2026-10-19")
    assert compteurs(premier, "test")["nb_ecrits"] == 2
    etat = lire_etat(tmp_path, "test", "une")
    assert (etat.etag, etat.last_modified) == ('"v1"', "Thu, 12 Nov 2026 06:00:00 GMT")
    assert "If-None-Match" not in serveur.entetes[0]

    second = collecter(serveur.source(), "2026-10-19")
    assert serveur.entetes[1]["If-None-Match"] == '"v1"'
    assert serveur.entetes[1]["If-Modified-Since"] == "Thu, 12 Nov 2026 06:00:00 GMT"
    assert compteurs(second, "test") == {"nb_recus": 0, "nb_ecrits": 0, "nb_ignores": 0}
    materialisation = second.asset_materializations_for_node("bronze__test")[0]
    assert materialisation.metadata["nb_non_modifies"].value == 1
    assert all(verdicts(second).values())  # un 304 n'est pas une panne
    assert len(articles_ecrits(tmp_path)) == 2
    assert lire_etat(tmp_path, "test", "une").etag == '"v1"'  # gardé pour la fois suivante


def test_source_en_panne_503_reessayee_par_la_session_http(serveur, tmp_path):
    serveur.statuts["/une"] = [503, 503]

    resultat = collecter(serveur.source(), "2026-10-19")

    assert resultat.success
    assert serveur.appels == ["/une"] * 3  # deux 503, puis le 200, dans un seul essai Dagster
    assert compteurs(resultat, "test") == {"nb_recus": 2, "nb_ecrits": 2, "nb_ignores": 0}
    assert len(articles_ecrits(tmp_path)) == 2
    etat = lire_etat(tmp_path, "test", "une")
    assert (etat.actif, etat.echecs_consecutifs) == (True, 0)
    assert etat.derniere_reussite is not None


def test_source_en_panne_503_reessayee_par_dagster_quand_la_session_abandonne(serveur, tmp_path):
    # La session HTTP abandonne après ses 4 requêtes : c'est la RetryPolicy de Dagster qui reprend.
    serveur.statuts["/une"] = [503] * (bronze.HTTP_RETRIES + 1)

    resultat = collecter(serveur.source(), "2026-10-19")

    assert resultat.success
    assert len(serveur.appels) == bronze.HTTP_RETRIES + 2
    assert len(articles_ecrits(tmp_path)) == 2
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 0


def test_source_en_panne_503_sans_fin_epuise_les_relances_sans_sommeil(serveur, tmp_path):
    serveur.statuts["/une"] = [503] * 100

    resultat = collecter(serveur.source(), "2026-10-19", raise_on_error=False)

    assert not resultat.success
    # (requête initiale + relances HTTP) x (essai initial + 3 relances Dagster)
    assert len(serveur.appels) == (bronze.HTTP_RETRIES + 1) * 4
    assert articles_ecrits(tmp_path) == []
    # Une panne passagère de l'éditeur ne met pas le flux en sommeil.
    assert lire_etat(tmp_path, "test", "une").actif
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 0


def test_source_en_panne_404_trois_echecs_de_suite_mettent_le_flux_en_sommeil(serveur, tmp_path):
    serveur.statuts["/une"] = [404] * 20

    for jour, echecs in (("2026-10-19", 1), ("2026-10-20", 2), ("2026-10-21", 3)):
        resultat = collecter(serveur.source(), jour, raise_on_error=False)
        assert not resultat.success
        etat = lire_etat(tmp_path, "test", "une")
        assert etat.echecs_consecutifs == echecs
        assert "404" in etat.derniere_erreur
    assert len(serveur.appels) == 3  # un 404 n'est pas relancé : un seul essai par run
    assert not lire_etat(tmp_path, "test", "une").actif
    with sqlite3.connect(db.chemin_base(tmp_path)) as connexion:  # visible dans la table sources
        assert connexion.execute(
            "SELECT actif, echecs_consecutifs FROM sources WHERE slug = 'test'"
        ).fetchone() == (0, 3)

    # Le run suivant ne contacte plus le flux, et le dit : le check sources_actives échoue.
    suivant = collecter(serveur.source(), "2026-10-21")
    assert suivant.success
    assert len(serveur.appels) == 3
    assert verdicts(suivant)["sources_actives"] is False
    materialisation = suivant.asset_materializations_for_node("bronze__test")[0]
    assert materialisation.metadata["flux_en_sommeil"].value == ["une"]


def test_une_reussite_remet_le_compteur_d_echecs_a_zero(serveur, tmp_path):
    serveur.statuts["/une"] = [404, 404]

    collecter(serveur.source(), "2026-10-19", raise_on_error=False)
    collecter(serveur.source(), "2026-10-20", raise_on_error=False)
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 2

    assert collecter(serveur.source(), "2026-10-21").success
    etat = lire_etat(tmp_path, "test", "une")
    assert (etat.actif, etat.echecs_consecutifs, etat.derniere_erreur) == (True, 0, None)
    assert etat.derniere_reussite is not None


def test_reactiver_remet_le_flux_en_service(serveur, tmp_path):
    synchroniser_sources(tmp_path, [serveur.source()])
    ecrire_etat(
        tmp_path, "test", "une", EtatFlux(actif=False, echecs_consecutifs=3, derniere_erreur="404")
    )
    assert reactiver.main(["inconnue"], racine=tmp_path) == 1  # source inconnue : rien à faire

    assert reactiver.main(["test"], racine=tmp_path) == 0

    assert lire_etat(tmp_path, "test", "une").actif
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 0
    assert collecter(serveur.source(), "2026-10-19").success
    assert serveur.appels == ["/une"]  # le flux est de nouveau contacté


@pytest.mark.parametrize("statut", [500, 502, 503, 504, 408, 429])
def test_erreur_http_transitoire(statut):
    assert bronze.est_transitoire(erreur_http(statut))


@pytest.mark.parametrize("statut", [400, 401, 403, 404, 410])
def test_erreur_http_permanente(statut):
    assert not bronze.est_transitoire(erreur_http(statut))


def test_erreurs_reseau_transitoires_et_url_invalide_permanente():
    assert bronze.est_transitoire(requests.ConnectionError("injoignable"))
    assert bronze.est_transitoire(requests.Timeout("trop lent"))
    assert not bronze.est_transitoire(requests.exceptions.InvalidURL("pas une url"))

    with bronze.creer_session() as session:
        retry = session.get_adapter("https://").max_retries
        assert retry.status_forcelist == (503,)
        assert retry.status == bronze.HTTP_RETRIES
        assert retry.backoff_factor == bronze.HTTP_BACKOFF_S


# ── l'orchestration ──────────────────────────────────────────────────────────────────────────


def test_collecte_planifiee_toutes_les_30_min_sur_la_partition_du_jour():
    assert collecte_toutes_les_30_min.cron_schedule == "*/30 * * * *"
    assert collecte_toutes_les_30_min.execution_timezone == "Europe/Paris"
    assert collecte_toutes_les_30_min.default_status == dg.DefaultScheduleStatus.RUNNING

    heure = datetime(2026, 11, 12, 0, 30, tzinfo=ZoneInfo("Europe/Paris"))
    with dg.build_schedule_context(scheduled_execution_time=heure) as contexte:
        demande = collecte_toutes_les_30_min(contexte)
    assert demande.partition_key == "2026-11-12"  # heure de Paris, pas UTC (11 nov. 23 h 30)

    assert {a.key.path[-1] for a in assets_bronze} == {s.slug for s in charger_sources()}
    assert all(a.group_names_by_key[a.key] == "bronze" for a in assets_bronze)
    assert collecte_toutes_les_30_min.job_name == "gazette_collecte"
    assert defs.resolve_job_def("gazette_collecte")
