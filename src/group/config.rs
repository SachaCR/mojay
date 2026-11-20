//! Configuration d'un consumer.

use std::time::Duration;

/// Où commencer à lire une partition sans offset commité utilisable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    /// Au plus ancien message disponible.
    Earliest,
    /// Après le dernier message lisible : seuls les nouveaux messages seront lus.
    Latest,
}

/// Configuration d'un consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerConfig {
    /// Où commencer quand le groupe n'a rien commité pour une partition, ou
    /// quand l'offset commité n'existe plus (supprimé par la rétention).
    pub start_from: StartFrom,
    /// `Some(intervalle)` : les positions sont commitées automatiquement, au
    /// plus tôt à cet intervalle, au début de chaque `poll`, quand une
    /// partition est cédée à un autre membre, et à la fermeture. Tout message
    /// rendu par un `poll` est alors considéré comme traité au `poll` suivant.
    ///
    /// `None` : commit manuel avec [`Consumer::commit`](crate::Consumer::commit).
    /// Une partition cédée à un autre membre n'est alors pas commitée : il la
    /// reprend au dernier commit (doublons possibles, aucune perte).
    pub auto_commit: Option<Duration>,
    /// Taille approximative maximale des messages rendus par un `poll`.
    pub max_poll_bytes: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            start_from: StartFrom::Earliest,
            auto_commit: None,
            max_poll_bytes: 1024 * 1024,
        }
    }
}
