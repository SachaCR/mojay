# `mojay` : la ligne de commande de Mojay

`mojay` sert à créer des topics, envoyer et lire des messages, et regarder ce
qui est réellement stocké dans un dossier de données Mojay. C'est l'outil de
débogage et d'exploration : votre application, elle, utilise la bibliothèque.

```bash
mojay create-topic orders --partitions 3
echo '{"commande": 1}' | mojay produce orders --key client-42
mojay tail orders --follow
```

## Installation

Depuis la racine du dépôt :

```bash
cargo install --path cli
```

Le binaire `mojay` est alors dans `~/.cargo/bin`. Sans l'installer, on peut
aussi le lancer avec `cargo run -p mojay-cli -- <commande>`.

## Le dossier de données

Toutes les commandes travaillent sur un dossier de données. On le choisit, par
ordre de priorité, avec :

1. l'option `--data <dossier>` (ou `-d`), placée avant ou après la commande ;
2. la variable d'environnement `MOJAY_DATA` ;
3. par défaut, `./data`.

Seule `create-topic` crée le dossier s'il n'existe pas. Les autres commandes
refusent un dossier qui n'est pas un dossier Mojay : une faute de frappe ne
crée jamais un dossier vide ailleurs.

```console
$ mojay -d ailleurs topics
erreur : ailleurs n'est pas un dossier de données Mojay (option --data ou variable MOJAY_DATA)
```

## Commandes

| Commande | Rôle | Modifie les données ? |
| --- | --- | --- |
| [`create-topic`](#create-topic) | Crée un topic | Oui |
| [`topics`](#topics) | Liste les topics | Non |
| [`describe`](#describe) | Configuration, partitions, segments, retard des groupes | Non |
| [`produce`](#produce) | Envoie des messages | Oui |
| [`tail`](#tail) | Affiche les messages | Non |
| [`dump`](#dump) | Affiche le contenu brut d'un segment | Non |

`mojay --help` et `mojay <commande> --help` donnent la liste complète des options.

### `create-topic`

```bash
mojay create-topic <nom> [options]
```

| Option | Défaut | Effet |
| --- | --- | --- |
| `-p`, `--partitions <n>` | `1` | Nombre de partitions. Fixe : en ajouter changerait la partition de chaque clé. |
| `--segment-size <taille>` | `64Mio` | Taille d'un segment avant de passer au suivant. |
| `--fsync <politique>` | `always` | `always` (chaque message est sur le disque avant d'être lisible), `never` (laissé à l'OS), ou un intervalle comme `100ms`. |
| `--retention-size <taille>` | aucune | Taille maximale d'**une partition** ; au-delà, les plus vieux segments sont supprimés. |
| `--retention-time <durée>` | aucune | Âge maximal d'un segment, depuis sa dernière écriture. |

Le nom accepte lettres, chiffres, `.`, `_` et `-`. La configuration est fixée à
la création.

```console
$ mojay create-topic events -p 4 --fsync 100ms --retention-size 10Gio --retention-time 7d
topic « events » créé (4 partitions)
```

### `topics`

Liste les topics, un par ligne, triés par nom.

### `describe`

Montre la configuration d'un topic, l'état de chaque partition et le retard de
chaque groupe de consumers. C'est la commande à garder sous la main pendant
qu'une application tourne.

```console
$ mojay describe orders
topic       orders
partitions  3
segments    64 Mio au plus
fsync       always
rétention   7 j par segment

partition         début       limite     messages  segments       taille
0                     0            2            2         1        132 o
1                     0            2            2         1        105 o
2                     0            1            1         1         61 o

groupe               partition       commité       retard
billing              0                     2            0
billing              1                     1            1
billing              2                     1            0
```

Ici, le groupe `billing` a tout lu, puis un nouveau message est arrivé sur la
partition 1 : il a un message de retard.

- **début** : le premier offset disponible ; il avance quand la rétention supprime des segments.
- **limite** : le premier offset pas encore lisible ; on peut lire `début..limite`.
- **retard** : les messages que le groupe n'a pas encore commités sur cette partition.

### `produce`

Envoie un message avec la valeur donnée, ou, sans valeur, **une ligne de
l'entrée standard par message**. Affiche `partition:offset` pour chaque message.

```bash
mojay produce <topic> [valeur] [options]
```

| Option | Effet |
| --- | --- |
| `-k`, `--key <clé>` | Clé du message. Elle choisit la partition (murmur2, comme Kafka) : les messages de même clé restent ensemble, dans l'ordre. |
| `-p`, `--partition <n>` | Partition imposée. Sans clé ni partition, les messages sont répartis en round-robin. |
| `-H`, `--header <clé=valeur>` | Un header. Répétable. |
| `-q`, `--quiet` | N'affiche rien. |

```console
$ mojay produce orders '{"commande": 1}' -k client-42 -H source=web
0:0
$ printf 'premier\ndeuxième\ntroisième\n' | mojay produce orders
0:1
1:0
2:0
```

Un topic n'a qu'un seul writer à la fois : si une application y écrit déjà,
`produce` échoue avec « le topic a déjà un writer ».

### `tail`

Affiche les messages d'un topic. `tail` est un simple lecteur : il ne rejoint
aucun groupe et ne commite rien, il est donc sans effet sur vos consumers.

```bash
mojay tail <topic> [options]
```

| Option | Effet |
| --- | --- |
| `-p`, `--partition <n>` | Une seule partition (par défaut : toutes). |
| `-n`, `--last <n>` | Seulement les n derniers messages de chaque partition. |
| `--offset <n>` | Commencer à cet offset (avec `--partition`). |
| `-f`, `--follow` | Continuer à afficher les nouveaux messages, jusqu'à Ctrl-C. |
| `--full` | Valeurs complètes (sinon tronquées à 120 caractères). |

Chaque ligne donne `partition:offset`, l'horodatage UTC, la clé, la valeur et
les headers :

```console
$ mojay tail orders
0:0  2026-11-20T13:06:31.884Z  key="client-42"  value="{\"commande\": 1}"  source="web"
0:1  2026-11-20T13:06:31.896Z  key=null  value="premier"
1:0  2026-11-20T13:06:31.899Z  key=null  value="deuxième"
2:0  2026-11-20T13:06:31.903Z  key=null  value="troisième"
```

Le texte s'affiche entre guillemets, avec les caractères spéciaux échappés ; une
valeur binaire s'affiche en hexadécimal (`0xff0010`) ; une clé ou une valeur
absente s'affiche `null`.

### `dump`

Affiche le contenu brut des segments, enregistrement par enregistrement : leur
position dans le fichier, leur offset, leur taille et leur contenu. C'est l'outil
pour vérifier à l'œil ce qui est stocké, ou comprendre un fichier abîmé.
`dump` ne répare jamais rien.

```bash
mojay dump <topic> [-p <partition>] [--index] [--full]
mojay dump <fichier.log>
```

```console
$ mojay dump orders -p 0 --index
== data/topics/orders/p0/00000000000000000000.log (132 o, base 0)
en-tête valide, format v1
  position     offset  taille  horodatage                contenu
        16          0      74  2026-11-20T13:06:31.884Z  key="client-42" value="{\"commande\": 1}" source="web"
        90          1      42  2026-11-20T13:06:31.896Z  key=null value="premier"
2 enregistrement(s) lisible(s)
index : 0 entrée(s)
```

Sur un fichier abîmé, `dump` montre tout ce qui précède le problème, puis où et
pourquoi la suite est illisible :

```console
$ mojay dump data/topics/orders/p0/00000000000000000000.log
…
1 enregistrement(s) lisible(s)
!! fin illisible à la position 90 : 42 o (CRC invalide)
```

`--index` affiche aussi les entrées de l'index et signale celles qui sont
incohérentes (la lecture normale les ignore).

## Unités

| Type | Exemples | Unités acceptées |
| --- | --- | --- |
| Taille | `4096`, `512o`, `64Kio`, `16Mio`, `10Gio`, `64MiB` | `o`/`B`, `Kio`/`KiB`/`K`, `Mio`/`MiB`/`M`, `Gio`/`GiB`/`G`, `Tio`/`TiB`/`T`, toujours en base 1024 |
| Durée | `250ms`, `30s`, `15m`, `12h`, `7d`, `7j` | `ms`, `s`, `m`/`min`, `h`, `d`/`j` ; l'unité est obligatoire |

## Codes de sortie

| Code | Signification |
| --- | --- |
| `0` | Succès. Une sortie fermée en cours de route (`mojay tail orders \| head`) compte comme un succès. |
| `1` | Erreur, expliquée sur la sortie d'erreur (`erreur : …`). |
| `2` | Arguments invalides : `clap` affiche l'usage de la commande. |
