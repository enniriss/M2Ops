"""Base SQLite de la gazette et ses migrations versionnées.

    $GAZETTE_DATA_DIR/gazette.db

Chaque fichier migrations/NNN_nom.sql est appliqué une seule fois, dans l'ordre de son numéro ;
la table schema_migrations garde la trace de ceux déjà passés. Une migration commitée ne se
modifie plus : on en ajoute une nouvelle.

    python -m gazette.db          # applique les migrations en attente
"""

import os
import sqlite3
import sys
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

MIGRATIONS_DIR = Path(
    os.environ.get("GAZETTE_MIGRATIONS_DIR", Path(__file__).resolve().parents[1] / "migrations")
)
NOM_BASE = "gazette.db"
ATTENTE_VERROU_S = 30


def chemin_base(racine: Path) -> Path:
    return racine / NOM_BASE


def migrations_disponibles(dossier: Path = MIGRATIONS_DIR) -> list[tuple[int, Path]]:
    migrations = []
    for fichier in sorted(dossier.glob("*.sql")):
        numero = fichier.name.split("_", 1)[0]
        if not numero.isdigit():
            raise ValueError(f"Migration mal nommée (NNN_nom.sql attendu) : {fichier.name}")
        migrations.append((int(numero), fichier))
    numeros = [n for n, _ in migrations]
    if len(numeros) != len(set(numeros)):
        raise ValueError(f"Deux migrations portent le même numéro dans {dossier}")
    return migrations


def instructions_sql(script: str) -> list[str]:
    """Découpe un script en instructions, une par execute() : executescript() validerait la
    transaction en cours avant de commencer, et on veut garder le verrou pris par migrer()."""
    instructions, courante = [], ""
    for morceau in script.split(";"):
        courante += morceau + ";"
        if sqlite3.complete_statement(courante):  # pas de coupure dans une chaîne ou un commentaire
            if courante.strip(" \t\r\n;"):
                instructions.append(courante)
            courante = ""
    return instructions


def versions_appliquees(connexion: sqlite3.Connection) -> set[int]:
    return {v for (v,) in connexion.execute("SELECT version FROM schema_migrations")}


def migrer(connexion: sqlite3.Connection, dossier: Path = MIGRATIONS_DIR) -> list[str]:
    """Applique les migrations pas encore passées ; renvoie les noms de celles appliquées.

    Plusieurs processus peuvent ouvrir une base neuve en même temps (les 8 assets bronze au
    premier run) : un seul applique les migrations, sous verrou d'écriture (BEGIN IMMEDIATE) ;
    les autres attendent le verrou, puis relisent schema_migrations et n'ont plus rien à faire.
    """
    connexion.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " version INTEGER PRIMARY KEY, nom TEXT NOT NULL, appliquee_le TEXT NOT NULL)"
    )
    disponibles = migrations_disponibles(dossier)
    if {v for v, _ in disponibles} <= versions_appliquees(connexion):
        return []  # cas courant : base à jour, pas de verrou

    appliquees = []
    connexion.execute("BEGIN IMMEDIATE")
    try:
        deja = versions_appliquees(connexion)  # relu sous verrou : un autre a pu migrer entre-temps
        for version, fichier in disponibles:
            if version in deja:
                continue
            for instruction in instructions_sql(fichier.read_text(encoding="utf-8")):
                connexion.execute(instruction)
            connexion.execute(
                "INSERT INTO schema_migrations (version, nom, appliquee_le) VALUES (?, ?, ?)",
                (version, fichier.name, datetime.now(timezone.utc).isoformat()),
            )
            appliquees.append(fichier.name)
        connexion.execute("COMMIT")  # toutes les migrations en attente passent, ou aucune
    except BaseException:
        connexion.execute("ROLLBACK")
        raise
    return appliquees


def passer_en_wal(connexion: sqlite3.Connection, attente_s: float) -> None:
    """Le passage en WAL d'une base neuve demande un verrou exclusif, et SQLite répond
    « database is locked » sans attendre (le timeout ne s'applique pas) : on réessaie."""
    limite = time.monotonic() + attente_s
    while True:
        try:
            connexion.execute("PRAGMA journal_mode=WAL")
            return
        except sqlite3.OperationalError as erreur:
            if "locked" not in str(erreur) or time.monotonic() > limite:
                raise
            time.sleep(0.05)


def connecter(racine: Path) -> sqlite3.Connection:
    """Connexion à la base, migrations à jour. Plusieurs runs peuvent écrire en même temps :
    WAL et une attente de verrou plutôt qu'une erreur immédiate."""
    racine.mkdir(parents=True, exist_ok=True)
    connexion = sqlite3.connect(chemin_base(racine), timeout=ATTENTE_VERROU_S, isolation_level=None)
    connexion.row_factory = sqlite3.Row
    passer_en_wal(connexion, ATTENTE_VERROU_S)
    connexion.execute("PRAGMA foreign_keys=ON")
    migrer(connexion)
    return connexion


def main() -> int:
    racine = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
    racine.mkdir(parents=True, exist_ok=True)
    with closing(
        sqlite3.connect(chemin_base(racine), timeout=ATTENTE_VERROU_S, isolation_level=None)
    ) as connexion:
        appliquees = migrer(connexion)
    print(f"Migrations appliquées : {', '.join(appliquees)}" if appliquees else "Base à jour.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
