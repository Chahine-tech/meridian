use thiserror::Error;

// ---------------------------------------------------------------------------
// NamespaceId
// ---------------------------------------------------------------------------

/// Validated namespace identifier.
///
/// Rules: `[a-z0-9_-]{1,64}` — lowercase alphanumeric, hyphens, underscores.
/// Enforced on construction so downstream code can trust the invariant.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NamespaceId(String);

#[derive(Debug, Error)]
pub enum NamespaceError {
    #[error("namespace is empty")]
    Empty,
    #[error("namespace too long: {0} chars (max 64)")]
    TooLong(usize),
    #[error("namespace contains invalid character at position {0}: '{1}'")]
    InvalidChar(usize, char),
}

impl NamespaceId {
    pub fn new(s: &str) -> Result<Self, NamespaceError> {
        if s.is_empty() {
            return Err(NamespaceError::Empty);
        }
        if s.len() > 64 {
            return Err(NamespaceError::TooLong(s.len()));
        }
        for (i, c) in s.chars().enumerate() {
            if !matches!(c, 'a'..='z' | '0'..='9' | '_' | '-') {
                return Err(NamespaceError::InvalidChar(i, c));
            }
        }
        Ok(Self(s.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::str::FromStr for NamespaceId {
    type Err = NamespaceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_namespaces() {
        for s in ["my-room", "chat_1", "a", "abc123", "x-y-z"] {
            assert!(NamespaceId::new(s).is_ok(), "should accept: {s}");
        }
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(NamespaceId::new(""), Err(NamespaceError::Empty)));
    }

    #[test]
    fn rejects_too_long() {
        let s = "a".repeat(65);
        assert!(matches!(NamespaceId::new(&s), Err(NamespaceError::TooLong(65))));
    }

    #[test]
    fn rejects_uppercase() {
        assert!(matches!(
            NamespaceId::new("MyRoom"),
            Err(NamespaceError::InvalidChar(0, 'M'))
        ));
    }

    #[test]
    fn rejects_slash() {
        assert!(matches!(
            NamespaceId::new("a/b"),
            Err(NamespaceError::InvalidChar(1, '/'))
        ));
    }

    #[test]
    fn rejects_space() {
        assert!(matches!(
            NamespaceId::new("my room"),
            Err(NamespaceError::InvalidChar(2, ' '))
        ));
    }

    #[test]
    fn display_roundtrip() {
        let ns = NamespaceId::new("my-namespace").unwrap();
        assert_eq!(ns.to_string(), "my-namespace");
        assert_eq!(ns.as_str(), "my-namespace");
    }

    #[test]
    fn fromstr_roundtrip() {
        let ns: NamespaceId = "test-123".parse().unwrap();
        assert_eq!(ns.as_str(), "test-123");
    }
}
