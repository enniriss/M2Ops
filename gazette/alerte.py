"""Alertes vers le canal de l'équipe : un webhook Discord ou Slack ($GAZETTE_ALERTE_WEBHOOK).

On alerte sur un changement, pas à chaque passage : quand un contrôle devient faux, puis quand
il redevient vrai (table etat_controles). Sans cela, un fait faux enverrait un message toutes
les 30 minutes et plus personne ne les lirait. Un run Dagster en échec alerte aussi
(capteur alerte_run_en_echec dans definitions.py).

    python -m gazette.alerte "message"     # envoie un message de test sur le canal
"""

import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests

WEBHOOK_ENV = "GAZETTE_ALERTE_WEBHOOK"
TIMEOUT_S = 10


def charge_utile(url: str, message: str) -> dict:
    """Discord attend « content », Slack (et la plupart des autres) « text »."""
    hote = urlsplit(url).netloc
    if "discord" in hote:
        return {"content": message[:2000], "allowed_mentions": {"parse": []}}
    return {"text": message}


def envoyer(message: str, url: str | None = None) -> bool:
    """Envoie le message ; False si aucun webhook n'est configuré. Lève si l'envoi échoue."""
    url = url or os.environ.get(WEBHOOK_ENV)
    if not url:
        return False
    reponse = requests.post(url, json=charge_utile(url, message), timeout=TIMEOUT_S)
    reponse.raise_for_status()
    return True


def changements(connexion, resultats, maintenant: datetime | None = None) -> list[str]:
    """Met à jour etat_controles ; renvoie les messages des contrôles qui ont changé d'état.

    Un contrôle jamais vu qui est vrai ne dit rien ; jamais vu et faux, il alerte.
    """
    quand = (maintenant or datetime.now(timezone.utc)).isoformat()
    anciens = {
        ligne["controle"]: bool(ligne["ok"])
        for ligne in connexion.execute("SELECT controle, ok FROM etat_controles")
    }
    messages = []
    for r in resultats:
        avant = anciens.get(r.nom, True)
        if avant != r.ok:
            messages.append(
                f"✅ Gazette · contrôle {r.nom} de nouveau vrai : {r.message}" if r.ok
                else f"🔴 Gazette · contrôle {r.nom} faux : {r.message}"
            )
        if r.nom not in anciens or avant != r.ok:
            connexion.execute(
                "INSERT INTO etat_controles (controle, ok, depuis, message) VALUES (?, ?, ?, ?) "
                "ON CONFLICT (controle) DO UPDATE SET ok = excluded.ok, depuis = excluded.depuis, "
                "message = excluded.message",
                (r.nom, int(r.ok), quand, r.message),
            )
    return messages


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    message = " ".join(argv) or "🔔 Gazette · message de test du canal d'alerte"
    try:
        envoye = envoyer(message)
    except requests.RequestException as erreur:
        print(f"Échec de l'envoi : {erreur}", file=sys.stderr)
        return 1
    if not envoye:
        print(f"Aucun webhook : définir {WEBHOOK_ENV} (fichier .env).", file=sys.stderr)
        return 1
    print("Alerte envoyée.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
