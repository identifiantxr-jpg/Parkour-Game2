"""Relais du jeu de parkour : fait passer les messages entre les joueurs d'une même partie.

Chaque joueur se connecte en WebSocket (comme un site web : ça passe sur les Wi-Fi d'école).
- Messages texte (JSON), du joueur vers le relais :
    {"t": "heberger"}                      -> crée une partie, renvoie son code ; l'hôte a le n° 1
    {"t": "rejoindre", "code": "KZ4P", "id": 123456}
- Messages texte du relais vers le joueur :
    {"t": "bienvenue", "code": "KZ4P", "id": 1}
    {"t": "arrivee", "id": 123456}  /  {"t": "depart", "id": 123456}   (envoyés à l'hôte)
    {"t": "erreur", "msg": "..."}   /  {"t": "fin"} (l'hôte est parti)
- Messages binaires : 4 octets (n° du destinataire, 0 = tous) + données du jeu ;
  le relais les renvoie avec, à la place, le n° de l'expéditeur.

Lancer en local : python relais.py   (port 8080, ou la variable PORT donnée par Render)
"""

import asyncio
import json
import os
import random
import struct
from http import HTTPStatus

from websockets.asyncio.server import serve

MAX_JOUEURS = 8
LETTRES = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"

parties = {}  # code -> {id: websocket}


def nouveau_code():
    while True:
        code = "".join(random.choice(LETTRES) for _ in range(4))
        if code not in parties:
            return code


async def envoyer(ws, donnees):
    try:
        await ws.send(donnees)
    except Exception:
        pass


async def joueur(ws):
    code = None
    mon_id = None
    try:
        # premier message : héberger ou rejoindre
        debut = json.loads(await asyncio.wait_for(ws.recv(), timeout=30))
        if debut.get("t") == "heberger":
            code = nouveau_code()
            mon_id = 1
            parties[code] = {1: ws}
        elif debut.get("t") == "rejoindre":
            code = str(debut.get("code", "")).strip().upper()
            mon_id = int(debut.get("id", 0))
            p = parties.get(code)
            if p is None:
                await ws.send(json.dumps({"t": "erreur", "msg": "Aucune partie avec ce code."}))
                return
            if len(p) >= MAX_JOUEURS:
                await ws.send(json.dumps({"t": "erreur", "msg": "La partie est pleine (8 joueurs)."}))
                return
            if mon_id <= 1 or mon_id in p:
                await ws.send(json.dumps({"t": "erreur", "msg": "Numéro de joueur refusé, réessaie."}))
                return
            p[mon_id] = ws
        else:
            return
        await ws.send(json.dumps({"t": "bienvenue", "code": code, "id": mon_id}))
        if mon_id != 1:
            await envoyer(parties[code][1], json.dumps({"t": "arrivee", "id": mon_id}))
        print(f"partie {code} : joueur {mon_id} entre ({len(parties[code])} joueurs)", flush=True)

        async for m in ws:
            if not isinstance(m, (bytes, bytearray)) or len(m) < 4:
                continue
            p = parties.get(code)
            if p is None:
                break
            dest = struct.unpack("<i", m[:4])[0]
            paquet = struct.pack("<i", mon_id) + m[4:]
            if dest == 0:
                cibles = [w for i, w in p.items() if i != mon_id]
            elif dest < 0:
                # tous sauf -dest
                cibles = [w for i, w in p.items() if i != mon_id and i != -dest]
            else:
                cibles = [p[dest]] if dest in p else []
            for w in cibles:
                await envoyer(w, paquet)
    except Exception:
        pass
    finally:
        p = parties.get(code) if code else None
        if p is not None and p.get(mon_id) is ws:
            del p[mon_id]
            print(f"partie {code} : joueur {mon_id} sort ({len(p)} joueurs)", flush=True)
            if mon_id == 1:
                # l'hôte est parti : la partie est finie pour tout le monde
                del parties[code]
                for w in list(p.values()):
                    await envoyer(w, json.dumps({"t": "fin"}))
                    await w.close()
            elif 1 in p:
                await envoyer(p[1], json.dumps({"t": "depart", "id": mon_id}))


def page_simple(connexion, requete):
    # une visite normale (navigateur, contrôle de Render) : petite page « ça marche »
    if requete.headers.get("Upgrade", "").lower() != "websocket":
        return connexion.respond(HTTPStatus.OK, f"Relais du jeu : OK ({len(parties)} parties)\n")
    return None


async def main():
    port = int(os.environ.get("PORT", "8080"))
    async with serve(joueur, "0.0.0.0", port, process_request=page_simple, max_size=2**20):
        print(f"Relais prêt sur le port {port}", flush=True)
        await asyncio.get_running_loop().create_future()


if __name__ == "__main__":
    asyncio.run(main())
