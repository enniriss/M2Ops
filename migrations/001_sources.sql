-- 001 · Table sources : un flux RSS par ligne, et son état de collecte.
-- Appliquée une seule fois par gazette.db (voir gazette/db.py, table schema_migrations).

CREATE TABLE sources (
    id                 INTEGER PRIMARY KEY,
    slug               TEXT    NOT NULL,                 -- le journal (colonne slug de sources.csv)
    nom                TEXT    NOT NULL,                 -- nom affiché du journal
    type               TEXT    NOT NULL CHECK (type IN ('PQN', 'PQR')),
    region             TEXT,
    flux               TEXT    NOT NULL,                 -- nom du flux au sein du journal
    url_flux           TEXT    NOT NULL UNIQUE,
    actif              INTEGER NOT NULL DEFAULT 1 CHECK (actif IN (0, 1)),
    derniere_reussite  TEXT,                             -- ISO 8601 UTC
    echecs_consecutifs INTEGER NOT NULL DEFAULT 0,
    derniere_erreur    TEXT,
    etag               TEXT,                             -- dernier ETag reçu, renvoyé en If-None-Match
    last_modified      TEXT,                             -- dernier Last-Modified, renvoyé en If-Modified-Since
    UNIQUE (slug, flux)
);
