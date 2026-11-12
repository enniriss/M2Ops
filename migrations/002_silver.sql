-- 002 · Couche silver : articles propres, rejets (avec leur règle), passages et état des contrôles.
-- Voir docs/donnees.md pour le contrat de chaque table.

-- Un passage = une exécution de la couche silver (après chaque collecte).
CREATE TABLE passages (
    id           INTEGER PRIMARY KEY,
    partition    TEXT    NOT NULL,                  -- jour traité (AAAA-MM-JJ, heure de Paris)
    debut        TEXT    NOT NULL,                  -- ISO 8601 UTC
    fin          TEXT,
    run_id       TEXT,                              -- run Dagster, pour retrouver ses journaux
    entres       INTEGER NOT NULL DEFAULT 0,
    gardes       INTEGER NOT NULL DEFAULT 0,
    rejetes      INTEGER NOT NULL DEFAULT 0
);

-- Ce qu'on a le droit de republier, et de quoi remonter au brut (id + fichier_brut).
CREATE TABLE articles_propres (
    id           TEXT    PRIMARY KEY,               -- même id que l'article brut (bronze)
    source       TEXT    NOT NULL,                  -- slug du journal (table sources)
    flux         TEXT,
    titre        TEXT    NOT NULL,
    extrait      TEXT    NOT NULL,                  -- 300 caractères au plus
    lien         TEXT,                              -- sans paramètres de suivi
    publie_le    TEXT,                              -- ISO 8601 UTC
    recupere_le  TEXT    NOT NULL,                  -- ISO 8601 UTC
    rubrique     TEXT,
    titre_cle    TEXT    NOT NULL,                  -- titre réduit (minuscules, sans accents) pour les doublons
    partition    TEXT    NOT NULL,
    fichier_brut TEXT    NOT NULL,                  -- chemin du JSON bronze, relatif à $GAZETTE_DATA_DIR
    passage_id   INTEGER NOT NULL REFERENCES passages (id)
);
CREATE INDEX articles_propres_partition ON articles_propres (partition);
CREATE INDEX articles_propres_recupere_le ON articles_propres (recupere_le);
CREATE INDEX articles_propres_lien ON articles_propres (lien);

CREATE TABLE rejets (
    id           TEXT    PRIMARY KEY,               -- même id que l'article brut (bronze)
    source       TEXT    NOT NULL,
    flux         TEXT,
    titre        TEXT,
    lien         TEXT,
    publie_le    TEXT,
    recupere_le  TEXT,
    regle        TEXT    NOT NULL,                  -- R01_publi… (gazette/silver/regles.toml)
    detail       TEXT,                              -- le motif reconnu, le score de ressemblance…
    doublon_de   TEXT,                              -- pour R10-R12 : l'article gardé qu'il répète
    partition    TEXT    NOT NULL,
    fichier_brut TEXT    NOT NULL,
    passage_id   INTEGER NOT NULL REFERENCES passages (id)
);
CREATE INDEX rejets_partition ON rejets (partition);
CREATE INDEX rejets_regle ON rejets (regle);

-- Les JSON bronze déjà passés en silver : un fichier n'est traité qu'une fois.
CREATE TABLE fichiers_traites (
    fichier      TEXT    PRIMARY KEY,
    passage_id   INTEGER NOT NULL REFERENCES passages (id)
);

-- Dernier verdict de chaque contrôle : on alerte quand il devient faux, et quand il redevient vrai.
CREATE TABLE etat_controles (
    controle     TEXT    PRIMARY KEY,
    ok           INTEGER NOT NULL CHECK (ok IN (0, 1)),
    depuis       TEXT    NOT NULL,
    message      TEXT
);
