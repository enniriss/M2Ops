"""Base SQLite de la gazette et ses migrations versionnées.

    $GAZETTE_DATA_DIR/gazette.db

Chaque fichier migrations/NNN_nom.sql est appliqué une seule fois, dans l'ordre de son numéro ;
la table schema_migrations garde la trace de ceux déjà passés. Une migration commitée ne se
modifie plus : on en ajoute une nouvelle.

    python -m gazette.db          # applique les migrations en attente
"""

import os
import sqlite3
from contextlib import closing
import sys
from datetime import datetime, timezone
from pathlib import Path

MIGRATIONS_DIR = Path(
    os.environ.get("GAZETTE_MIGRATIONS_DIR", Path(__file__).resolve().parents[1] / "migrations")
)
NOM_BASE = "gazette.db"


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


def migrer(connexion: sqlite3.Connection, dossier: Path = MIGRATIONS_DIR) -> list[str]:
    """Applique les migrations pas encore passées ; renvoie les noms de celles appliquées."""
    connexion.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations ("
        " version INTEGER PRIMARY KEY, nom TEXT NOT NULL, appliquee_le TEXT NOT NULL)"
    )
    deja = {v for (v,) in connexion.execute("SELECT version FROM schema_migrations")}
    appliquees = []
    for version, fichier in migrations_disponibles(dossier):
        if version in deja:
            continue
        # Script et trace dans la même transaction : une migration passe entière ou pas du tout.
        script = fichier.read_text(encoding="utf-8")
        connexion.executescript(
            "BEGIN;\n"
            f"{script}\n"
            "INSERT INTO schema_migrations (version, nom, appliquee_le) VALUES "
            f"({version}, '{fichier.name}', '{datetime.now(timezone.utc).isoformat()}');\n"
            "COMMIT;"
        )
        appliquees.append(fichier.name)
    return appliquees


def connecter(racine: Path) -> sqlite3.Connection:
    """Connexion à la base, migrations à jour. Plusieurs runs peuvent écrire en même temps :
    WAL et une attente de verrou plutôt qu'une erreur immédiate."""
    racine.mkdir(parents=True, exist_ok=True)
    connexion = sqlite3.connect(chemin_base(racine), timeout=30, isolation_level=None)
    connexion.row_factory = sqlite3.Row
    connexion.execute("PRAGMA journal_mode=WAL")
    connexion.execute("PRAGMA foreign_keys=ON")
    migrer(connexion)
    return connexion


def main() -> int:
    racine = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
    racine.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(chemin_base(racine), isolation_level=None)) as connexion:
        appliquees = migrer(connexion)
    print(f"Migrations appliquées : {', '.join(appliquees)}" if appliquees else "Base à jour.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
