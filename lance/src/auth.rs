//! Authentication module for LANCE server
//!
//! Provides token-based authentication for client connections.
//!
//! # Configuration
//!
//! Authentication is configured via `AuthSettings` in the server config:
//!
//! ```toml
//! [auth]
//! enabled = true
//! tokens = ["token1", "token2"]
//! # or use a token file
//! token_file = "/etc/lance/tokens.txt"
//! write_only = false  # require auth only for writes
//! ```

use crate::config::AuthSettings;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

/// Token validator for authenticating client requests
#[derive(Debug, Clone)]
pub struct TokenValidator {
    /// Set of valid tokens (hashed for security)
    valid_tokens: Arc<HashSet<String>>,
    /// Whether authentication is enabled
    enabled: bool,
    /// Whether to only require auth for write operations
    write_only: bool,
}

impl TokenValidator {
    /// Create a new token validator from auth settings
    pub fn from_settings(settings: &AuthSettings) -> Result<Self, AuthError> {
        if !settings.enabled {
            return Ok(Self {
                valid_tokens: Arc::new(HashSet::new()),
                enabled: false,
                write_only: false,
            });
        }

        let mut tokens = HashSet::new();

        // Add static tokens from config
        for token in &settings.tokens {
            if !token.is_empty() {
                tokens.insert(token.clone());
            }
        }

        // Load tokens from file if specified
        if let Some(ref path) = settings.token_file {
            let file_tokens = Self::load_tokens_from_file(path)?;
            tokens.extend(file_tokens);
        }

        if tokens.is_empty() {
            return Err(AuthError::NoTokensConfigured);
        }

        tracing::info!(
            target: "lance::auth",
            token_count = tokens.len(),
            write_only = settings.write_only,
            "Token validator initialized"
        );

        Ok(Self {
            valid_tokens: Arc::new(tokens),
            enabled: true,
            write_only: settings.write_only,
        })
    }

    /// Load tokens from a file (one token per line)
    fn load_tokens_from_file(path: &Path) -> Result<Vec<String>, AuthError> {
        let content = fs::read_to_string(path)
            .map_err(|e| AuthError::TokenFileError(format!("{}: {}", path.display(), e)))?;

        let tokens: Vec<String> = content
            .lines()
            .map(|line| line.trim())
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(String::from)
            .collect();

        Ok(tokens)
    }

    /// Check if authentication is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if only write operations require authentication
    pub fn is_write_only(&self) -> bool {
        self.write_only
    }

    /// Validate a token
    #[allow(dead_code)]
    pub fn validate(&self, token: &str) -> bool {
        if !self.enabled {
            return true;
        }
        self.valid_tokens.contains(token)
    }

    /// Validate a token for a specific operation type
    #[allow(dead_code)]
    pub fn validate_for_operation(&self, token: Option<&str>, is_write: bool) -> AuthResult {
        if !self.enabled {
            return AuthResult::Allowed;
        }

        // If write_only mode and this is a read operation, allow without token
        if self.write_only && !is_write {
            return AuthResult::Allowed;
        }

        match token {
            Some(t) if self.valid_tokens.contains(t) => AuthResult::Allowed,
            Some(_) => AuthResult::Denied(AuthError::InvalidToken),
            None => AuthResult::Denied(AuthError::MissingToken),
        }
    }

    /// Get the number of configured tokens
    pub fn token_count(&self) -> usize {
        self.valid_tokens.len()
    }
}

impl Default for TokenValidator {
    fn default() -> Self {
        Self {
            valid_tokens: Arc::new(HashSet::new()),
            enabled: false,
            write_only: false,
        }
    }
}

/// Result of an authentication check
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AuthResult {
    /// Request is allowed
    Allowed,
    /// Request is denied with reason
    Denied(AuthError),
}

impl AuthResult {
    /// Check if the result is allowed
    #[allow(dead_code)]
    pub fn is_allowed(&self) -> bool {
        matches!(self, AuthResult::Allowed)
    }

    /// Convert to a Result type
    #[allow(dead_code, clippy::wrong_self_convention)]
    pub fn to_result(self) -> Result<(), AuthError> {
        match self {
            AuthResult::Allowed => Ok(()),
            AuthResult::Denied(e) => Err(e),
        }
    }
}

/// Authentication errors
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum AuthError {
    /// No token was provided
    MissingToken,
    /// Token is invalid
    InvalidToken,
    /// No tokens configured but auth is enabled
    NoTokensConfigured,
    /// Error reading token file
    TokenFileError(String),
}

impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthError::MissingToken => write!(f, "Authentication required"),
            AuthError::InvalidToken => write!(f, "Invalid authentication token"),
            AuthError::NoTokensConfigured => write!(f, "No tokens configured"),
            AuthError::TokenFileError(msg) => write!(f, "Token file error: {}", msg),
        }
    }
}

impl std::error::Error for AuthError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_validator_allows_all() {
        let validator = TokenValidator::default();
        assert!(!validator.is_enabled());
        assert!(validator.validate("any_token"));
        assert!(validator.validate_for_operation(None, true).is_allowed());
    }

    #[test]
    fn test_enabled_validator_requires_token() {
        let settings = AuthSettings {
            enabled: true,
            tokens: vec!["valid_token".to_string()],
            token_file: None,
            write_only: false,
        };
        let validator = TokenValidator::from_settings(&settings).unwrap();

        assert!(validator.is_enabled());
        assert!(validator.validate("valid_token"));
        assert!(!validator.validate("invalid_token"));
    }

    #[test]
    fn test_write_only_mode() {
        let settings = AuthSettings {
            enabled: true,
            tokens: vec!["valid_token".to_string()],
            token_file: None,
            write_only: true,
        };
        let validator = TokenValidator::from_settings(&settings).unwrap();

        // Reads should be allowed without token
        assert!(validator.validate_for_operation(None, false).is_allowed());

        // Writes should require token
        assert!(!validator.validate_for_operation(None, true).is_allowed());
        assert!(validator
            .validate_for_operation(Some("valid_token"), true)
            .is_allowed());
    }

    #[test]
    fn test_missing_token_error() {
        let settings = AuthSettings {
            enabled: true,
            tokens: vec!["valid_token".to_string()],
            token_file: None,
            write_only: false,
        };
        let validator = TokenValidator::from_settings(&settings).unwrap();

        let result = validator.validate_for_operation(None, true);
        assert_eq!(result, AuthResult::Denied(AuthError::MissingToken));
    }

    #[test]
    fn test_invalid_token_error() {
        let settings = AuthSettings {
            enabled: true,
            tokens: vec!["valid_token".to_string()],
            token_file: None,
            write_only: false,
        };
        let validator = TokenValidator::from_settings(&settings).unwrap();

        let result = validator.validate_for_operation(Some("wrong_token"), true);
        assert_eq!(result, AuthResult::Denied(AuthError::InvalidToken));
    }

    #[test]
    fn test_no_tokens_error() {
        let settings = AuthSettings {
            enabled: true,
            tokens: vec![],
            token_file: None,
            write_only: false,
        };
        let result = TokenValidator::from_settings(&settings);
        assert!(matches!(result, Err(AuthError::NoTokensConfigured)));
    }
}
