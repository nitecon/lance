//! TLS Support for LANCE Client
//!
//! Provides TLS/mTLS configuration for LANCE client connections.
//!
//! # Example
//!
//! ```rust
//! use lnc_client::TlsClientConfig;
//!
//! // Basic TLS configuration (uses system root certificates)
//! let tls_config = TlsClientConfig::new();
//!
//! // TLS with custom CA
//! let tls_config = TlsClientConfig::new()
//!     .with_ca_cert("/path/to/ca.pem");
//!
//! // mTLS with client certificate
//! let tls_config = TlsClientConfig::new()
//!     .with_ca_cert("/path/to/ca.pem")
//!     .with_client_cert("/path/to/client.pem", "/path/to/client-key.pem");
//!
//! // Check if mTLS is configured
//! assert!(tls_config.is_mtls());
//! ```

use std::path::{Path, PathBuf};

/// TLS configuration for client connections
#[derive(Debug, Clone, Default)]
pub struct TlsClientConfig {
    /// Path to CA certificate for server verification
    pub ca_cert_path: Option<PathBuf>,
    /// Path to client certificate (for mTLS)
    pub client_cert_path: Option<PathBuf>,
    /// Path to client private key (for mTLS)
    pub client_key_path: Option<PathBuf>,
    /// Server name for SNI (defaults to address hostname)
    pub server_name: Option<String>,
    /// Skip server certificate verification (dangerous, testing only)
    pub danger_accept_invalid_certs: bool,
}

impl TlsClientConfig {
    /// Create a new TLS configuration with default settings
    ///
    /// Uses system root certificates for server verification.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the CA certificate path for server verification
    pub fn with_ca_cert(mut self, path: impl AsRef<Path>) -> Self {
        self.ca_cert_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Set client certificate and key for mTLS authentication
    pub fn with_client_cert(
        mut self,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> Self {
        self.client_cert_path = Some(cert_path.as_ref().to_path_buf());
        self.client_key_path = Some(key_path.as_ref().to_path_buf());
        self
    }

    /// Set the server name for SNI
    ///
    /// If not set, the hostname from the connection address is used.
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Skip server certificate verification (DANGEROUS)
    ///
    /// This should only be used for testing. It disables certificate
    /// verification, making the connection vulnerable to MITM attacks.
    #[cfg(any(test, feature = "dangerous-testing"))]
    pub fn danger_accept_invalid_certs(mut self) -> Self {
        self.danger_accept_invalid_certs = true;
        self
    }

    /// Check if mTLS is configured
    pub fn is_mtls(&self) -> bool {
        self.client_cert_path.is_some() && self.client_key_path.is_some()
    }

    /// Convert to lnc-network TlsConfig
    pub fn to_network_config(&self) -> lnc_network::TlsConfig {
        if self.is_mtls() {
            lnc_network::TlsConfig::mtls(
                self.client_cert_path.as_ref().map(|p| p.to_string_lossy().to_string()).unwrap_or_default(),
                self.client_key_path.as_ref().map(|p| p.to_string_lossy().to_string()).unwrap_or_default(),
                self.ca_cert_path.as_ref().map(|p| p.to_string_lossy().to_string()).unwrap_or_default(),
            )
        } else if let Some(ref ca_path) = self.ca_cert_path {
            lnc_network::TlsConfig::client(Some(ca_path.to_string_lossy().to_string()))
        } else {
            lnc_network::TlsConfig::client(None::<String>)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let config = TlsClientConfig::new();
        assert!(config.ca_cert_path.is_none());
        assert!(config.client_cert_path.is_none());
        assert!(!config.is_mtls());
    }

    #[test]
    fn test_tls_config_with_ca() {
        let config = TlsClientConfig::new()
            .with_ca_cert("/path/to/ca.pem");
        
        assert_eq!(config.ca_cert_path.as_ref().map(|p| p.to_str()), Some(Some("/path/to/ca.pem")));
        assert!(!config.is_mtls());
    }

    #[test]
    fn test_tls_config_mtls() {
        let config = TlsClientConfig::new()
            .with_ca_cert("/path/to/ca.pem")
            .with_client_cert("/path/to/cert.pem", "/path/to/key.pem");
        
        assert!(config.is_mtls());
    }

    #[test]
    fn test_tls_config_with_server_name() {
        let config = TlsClientConfig::new()
            .with_server_name("lance.example.com");
        
        assert_eq!(config.server_name, Some("lance.example.com".to_string()));
    }

    #[test]
    fn test_client_config_with_tls() {
        use crate::ClientConfig;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:1992".parse().unwrap();
        let tls = TlsClientConfig::new()
            .with_server_name("lance.example.com");

        let config = ClientConfig::new(addr).with_tls(tls);

        assert!(config.is_tls_enabled());
        assert!(config.tls.is_some());
        assert_eq!(config.tls.as_ref().unwrap().server_name, Some("lance.example.com".to_string()));
    }

    #[test]
    fn test_client_config_without_tls() {
        use crate::ClientConfig;
        use std::net::SocketAddr;

        let addr: SocketAddr = "127.0.0.1:1992".parse().unwrap();
        let config = ClientConfig::new(addr);

        assert!(!config.is_tls_enabled());
        assert!(config.tls.is_none());
    }
}
