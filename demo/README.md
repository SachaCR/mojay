# Démo Mojay : un producteur, plusieurs lecteurs

Deux petits programmes pour voir Mojay fonctionner en local, sans serveur :

- **`mojay-demo-producer`** publie des événements de commande en JSON dans le
  topic `demo-events` (3 partitions), jusqu'à Ctrl-C. La clé de chaque
  message est le client : les événements d'un client restent dans la même
  partition, dans l'ordre.
- **`mojay-demo-consumer`** rejoint le groupe `demo-loggers` et affiche les
  messages qu'il reçoit, ainsi que les partitions reçues ou cédées.

## Lancer la démo

Depuis la racine du dépôt, dans un premier terminal :

```bash
cargo run -p mojay-demo-producer
```

Puis, dans un à trois autres terminaux :

```bash
cargo run -p mojay-demo-consumer
```

Tous partagent le dossier `./demo-data`, que Git ignore.

## Ce qu'on observe

- **Un seul lecteur** reçoit les 3 partitions.
- **Un deuxième lecteur** : en moins d'une seconde, le premier cède une
  partition (`- partition 2 cédée`), que le second reçoit (`+ partition 2 reçue`).
- **Trois lecteurs** : une partition chacun.
- **Ctrl-C sur un lecteur** : il commite, rend ses partitions, et les autres
  les reprennent exactement où il s'était arrêté, sans doublon.
- **Un lecteur tué** (`kill -9`) : les autres reprennent ses partitions au
  dernier commit (au plus une seconde de messages relus).
- **Relancer un lecteur** : il reprend là où le groupe s'était arrêté.

Pour suivre l'état du topic et le retard du groupe pendant ce temps :

```bash
cargo run -p mojay-cli -- -d ./demo-data describe demo-events
```

## Options utiles

| Programme | Option | Effet |
| --- | --- | --- |
| producteur | `--rate 1000` | 1 000 événements par seconde (un résumé par seconde au lieu d'une ligne par événement) |
| consumer | `--group autre` | Un autre groupe : il relit tout, indépendamment de `demo-loggers` |
| consumer | `--latest` | Sans commit, ne lire que les nouveaux messages |
| les deux | `--data chemin` | Un autre dossier de données |

## Espace disque

Le topic est créé avec une rétention d'environ **500 Mio au total** (la
limite s'applique par partition : 500 Mio / 3), avec des segments de 16 Mio.
Le producteur peut donc tourner indéfiniment : les plus vieux segments sont
supprimés au fur et à mesure.

Pour repartir de zéro : arrêter les programmes, puis supprimer `./demo-data`.
