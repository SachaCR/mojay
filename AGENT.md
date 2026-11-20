# AGENT.md — règles de développement de Mojay

Mojay est une bibliothèque Rust qui offre topics, partitions, offsets et consumer
groups dans un dossier local, sans serveur : « SQLite pour Kafka ». Elle stocke
les données des autres. **La fiabilité passe avant tout, la lisibilité juste après,
la performance ensuite.**

La note de conception fait référence : [docs/conception.md](docs/conception.md),
versionnée avec le code. Un changement de format disque ou de comportement visible
commence par sa mise à jour, dans le même commit que le code. La note en ligne
(https://claude.ai/artifact/674k8FuSABpT9t2fr7fA1w) sert aux discussions.

## 1. Architecture : une responsabilité par module

Les couches ne dépendent que des couches en dessous d'elles, jamais l'inverse.

| Couche | Module | Responsabilité | Ne fait pas |
| --- | --- | --- | --- |
| Format | `record` | Encoder et décoder un enregistrement | Aucune I/O fichier |
| Stockage | `segment` | Un fichier `.log` et son index, la récupération | Ne connaît ni topic ni partition |
| Partition | `log` | Une suite de segments, la rotation, la politique fsync | Aucun verrou inter-processus |
| Topic | `topic` | Partitions, verrou du writer, limites de lecture | Ne lit pas d'octets bruts |
| Groupes | `group` | Consumers, possession des partitions, offsets commités | Ne touche pas aux segments |
| API | `db` | Le point d'entrée public | Aucune logique métier propre |

Modules de support, chacun avec une seule responsabilité :

- `index` (logique pure) : format et règle de l'index clairsemé d'un segment.
- `partitioner` (logique pure) : choix de la partition, murmur2 compatible Kafka.
- `meta` : fichiers `clé = valeur` (`mojay.toml`, `meta.toml`), lecture stricte.
- `read_limits` : les limites de lecture partagées en `mmap`. Seul module `unsafe`.
- `lock` : verrous `flock` inter-processus.
- `durable` : écritures atomiques et `fsync` de dossier.

- Un module expose le minimum : `pub(crate)` par défaut, `pub` seulement pour l'API publique.
- La logique pure (calculs, encodage, quotas d'un groupe) est séparée des effets
  (fichiers, verrous, horloge), pour pouvoir la tester sans disque.
- Pas de module fourre-tout (`utils`, `helpers`). Une fonction vit avec la
  responsabilité qu'elle sert.
- Une nouvelle couche ou un nouveau fichier sur disque se décide d'abord dans la
  note de conception.

## 2. Lisibilité : du code écrit pour un humain

- Des noms explicites, qui disent ce que la chose *est* ou *fait* : `next_offset`,
  `rebuild_from`, pas `n`, `tmp` ou `process`.
- Des fonctions courtes, qui font une seule chose. Au-delà d'environ 50 lignes ou
  3 niveaux d'imbrication, découper.
- Pas d'astuce : le code le plus évident est le meilleur. Une optimisation n'est
  acceptée que si une mesure la justifie, et elle est commentée.
- Les commentaires expliquent le *pourquoi* (invariant, cas de crash, choix de
  conception), pas le *quoi*.
- Chaque élément `pub` a un commentaire de doc `///`, avec ses erreurs possibles.
- Langue : identifiants en anglais, commentaires et messages d'erreur en français.
- Les constantes ont un nom : pas de nombre magique (`HEADER_SIZE`, pas `16`).

## 3. Fiabilité : ne jamais perdre ni mentir sur les données

**Erreurs**
- Toute erreur passe par `crate::Error`, avec un variant précis. Pas de `String` en guise d'erreur dans l'API publique.
- Pas de `unwrap`, `expect` ni `panic!` hors tests. Seule exception : un invariant
  prouvé localement, par exemple une tranche de taille connue convertie en tableau.
- Des octets lus sur le disque ne sont jamais dignes de confiance : longueurs,
  CRC et offsets sont vérifiés avant usage, et une donnée invalide devient une
  erreur, jamais un panic.

**Durabilité et crashs**
- L'ordre des écritures est une règle de correction : écrire les données,
  appliquer la politique `fsync`, *puis* les rendre lisibles (limite de lecture, `read_limit`).
- Un petit fichier se remplace de façon atomique : fichier temporaire, `fsync`,
  `rename`, puis `fsync` du dossier.
- La récupération ne tronque que ce qui provient d'une écriture interrompue :
  la fin du segment actif. Une anomalie dans un segment scellé est signalée
  (`Error::Corrupt`), jamais réparée en silence.
- L'index est un cache : tout ce qu'il contient doit pouvoir être reconstruit depuis le `.log`.
- Tout changement du format disque incrémente sa version et reste lisible, ou
  échoue clairement sur un ancien fichier.

**Multi-processus**
- Verrous avec `flock`, jamais avec `fcntl` : ces derniers sont relâchés dès que
  le processus ferme un descripteur quelconque sur le fichier.
- Un verrou se libère par `flock(LOCK_UN)` explicite, pas seulement en fermant le
  fichier : un enfant créé par `fork` partage le verrou jusqu'à son `exec`.
- Lectures positionnelles (`pread`) : pas de curseur de fichier partagé.
- Un lecteur ne modifie jamais rien sur le disque : seul le writer répare.
- `unsafe` est interdit, sauf dans le module qui mappe le fichier `read_limits`. Chaque bloc
  `unsafe` porte un commentaire `// SAFETY:` qui justifie l'invariant.

## 4. Tests

- Chaque comportement a un test. Un bug corrigé arrive avec le test qui le reproduit.
- Le format et la récupération se testent de façon exhaustive : coupure à chaque
  octet, octet modifié à chaque position, index absent ou faux.
- Le comportement multi-processus se teste avec de vrais processus, pas seulement des threads.
- Les tests utilisent des dossiers temporaires (`tempfile`), jamais un chemin fixe.

## 5. Avant de considérer une tâche terminée

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
cargo test
```

Les trois doivent passer sans avertissement. Les dépendances restent minimales :
chaque nouvelle crate se justifie (maintenance, licence, taille).
