//! TLS support for LANCE connections
//!
//! This module provides TLS configuration and connection handling for secure
//! client-server communication using rustls.
//!
//! # Feature Flag
//!
//! TLS support requires the `tls` feature:
//! ```toml
//! lnc-network = { version = "0.1", features = ["tls"] }
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use lnc_network::tls::{TlsConfig, TlsAcceptor, TlsConnector};
//!
//! // Server-side
//! let config = TlsConfig::server("cert.pem", "key.pem")?;
//! let acceptor = TlsAcceptor::new(config)?;
//!
//! // Client-side
//! let config = TlsConfig::client(Some("ca.pem"))?;
//! let connector = TlsConnector::new(config)?;
//! ```

use std::path::Path;

#[cfg(feature = "tls")]
use std::fs::File;
#[cfg(feature = "tls")]
use std::io::BufReader;
#[cfg(feature = "tls")]
use std::sync::Arc;

/// TLS configuration for LANCE connections
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to certificate file (PEM format)
    pub cert_path: Option<String>,
    /// Path to private key file (PEM format)
    pub key_path: Option<String>,
    /// Path to CA certificate for verification
    pub ca_path: Option<String>,
    /// Whether to verify peer certificates
    pub verify_peer: bool,
    /// Server name for SNI (client only)
    pub server_name: Option<String>,
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self {
            cert_path: None,
            key_path: None,
            ca_path: None,
            verify_peer: true,
            server_name: None,
        }
    }
}

impl TlsConfig {
    /// Create a new TLS config for server mode
    pub fn server(cert_path: impl AsRef<Path>, key_path: impl AsRef<Path>) -> Self {
        Self {
            cert_path: Some(cert_path.as_ref().to_string_lossy().into_owned()),
            key_path: Some(key_path.as_ref().to_string_lossy().into_owned()),
            ca_path: None,
            verify_peer: false,
            server_name: None,
        }
    }

    /// Create a new TLS config for client mode
    pub fn client(ca_path: Option<impl AsRef<Path>>) -> Self {
        Self {
            cert_path: None,
            key_path: None,
            ca_path: ca_path.map(|p| p.as_ref().to_string_lossy().into_owned()),
            verify_peer: true,
            server_name: None,
        }
    }

    /// Create a new TLS config for mutual TLS (mTLS)
    pub fn mtls(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        ca_path: impl AsRef<Path>,
    ) -> Self {
        Self {
            cert_path: Some(cert_path.as_ref().to_string_lossy().into_owned()),
            key_path: Some(key_path.as_ref().to_string_lossy().into_owned()),
            ca_path: Some(ca_path.as_ref().to_string_lossy().into_owned()),
            verify_peer: true,
            server_name: None,
        }
    }

    /// Set the server name for SNI
    pub fn with_server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    /// Disable peer certificate verification (NOT RECOMMENDED for production)
    pub fn with_insecure(mut self) -> Self {
        self.verify_peer = false;
        self
    }
}

/// Error type for TLS operations
#[derive(Debug)]
pub enum TlsError {
    /// Certificate file not found or invalid
    CertificateError(String),
    /// Private key file not found or invalid
    KeyError(String),
    /// CA certificate file not found or invalid
    CaError(String),
    /// TLS handshake failed
    HandshakeError(String),
    /// I/O error during TLS operation
    IoError(std::io::Error),
    /// Configuration error
    ConfigError(String),
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TlsError::CertificateError(msg) => write!(f, "Certificate error: {}", msg),
            TlsError::KeyError(msg) => write!(f, "Key error: {}", msg),
            TlsError::CaError(msg) => write!(f, "CA error: {}", msg),
            TlsError::HandshakeError(msg) => write!(f, "Handshake error: {}", msg),
            TlsError::IoError(e) => write!(f, "I/O error: {}", e),
            TlsError::ConfigError(msg) => write!(f, "Config error: {}", msg),
        }
    }
}

impl std::error::Error for TlsError {}

impl From<std::io::Error> for TlsError {
    fn from(e: std::io::Error) -> Self {
        TlsError::IoError(e)
    }
}

/// Result type for TLS operations
pub type TlsResult<T> = std::result::Result<T, TlsError>;

/// TLS acceptor for server-side connections
#[derive(Clone)]
pub struct TlsAcceptor {
    config: TlsConfig,
    #[cfg(feature = "tls")]
    inner: Arc<tokio_rustls::TlsAcceptor>,
}

impl TlsAcceptor {
    /// Create a new TLS acceptor with the given configuration
    pub fn new(config: TlsConfig) -> TlsResult<Self> {
        if config.cert_path.is_none() {
            return Err(TlsError::ConfigError(
                "Server TLS config requires certificate path".into(),
            ));
        }
        if config.key_path.is_none() {
            return Err(TlsError::ConfigError(
                "Server TLS config requires key path".into(),
            ));
        }

        #[cfg(feature = "tls")]
        {
            let inner = Self::build_acceptor(&config)?;
            Ok(Self {
                config,
                inner: Arc::new(inner),
            })
        }

        #[cfg(not(feature = "tls"))]
        {
            Ok(Self { config })
        }
    }

    #[cfg(feature = "tls")]
    fn build_acceptor(config: &TlsConfig) -> TlsResult<tokio_rustls::TlsAcceptor> {
        use rustls::pki_types::CertificateDer;

        let cert_path = config.cert_path.as_ref().ok_or_else(|| {
            TlsError::ConfigError("Certificate path required".into())
        })?;
        let key_path = config.key_path.as_ref().ok_or_else(|| {
            TlsError::ConfigError("Key path required".into())
        })?;

        // Load certificates
        let cert_file = File::open(cert_path)
            .map_err(|e| TlsError::CertificateError(format!("{}: {}", cert_path, e)))?;
        let mut cert_reader = BufReader::new(cert_file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
            .filter_map(|r| r.ok())
            .collect();

        if certs.is_empty() {
            return Err(TlsError::CertificateError("No certificates found".into()));
        }

        // Load private key
        let key_file = File::open(key_path)
            .map_err(|e| TlsError::KeyError(format!("{}: {}", key_path, e)))?;
        let mut key_reader = BufReader::new(key_file);
        let key = rustls_pemfile::private_key(&mut key_reader)
            .map_err(|e| TlsError::KeyError(format!("Failed to parse key: {}", e)))?
            .ok_or_else(|| TlsError::KeyError("No private key found".into()))?;

        // Build server config
        let server_config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| TlsError::ConfigError(format!("Failed to build config: {}", e)))?;

        Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server_config)))
    }

    /// Get the TLS configuration
    pub fn config(&self) -> &TlsConfig {
        &self.config
    }

    /// Accept a TLS connection (requires `tls` feature)
    #[cfg(feature = "tls")]
    pub async fn accept<S>(&self, stream: S) -> TlsResult<tokio_rustls::server::TlsStream<S>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        self.inner
            .accept(stream)
            .await
            .map_err(|e| TlsError::HandshakeError(e.to_string()))
    }
}

/// TLS connector for client-side connections
#[derive(Clone)]
pub struct TlsConnector {
    config: TlsConfig,
    #[cfg(feature = "tls")]
    inner: Arc<tokio_rustls::TlsConnector>,
}

impl TlsConnector {
    /// Create a new TLS connector with the given configuration
    pub fn new(config: TlsConfig) -> TlsResult<Self> {
        #[cfg(feature = "tls")]
        {
            let inner = Self::build_connector(&config)?;
            Ok(Self {
                config,
                inner: Arc::new(inner),
            })
        }

        #[cfg(not(feature = "tls"))]
        {
            Ok(Self { config })
        }
    }

    #[cfg(feature = "tls")]
    fn build_connector(config: &TlsConfig) -> TlsResult<tokio_rustls::TlsConnector> {
        use rustls::pki_types::CertificateDer;
        use rustls::RootCertStore;

        let mut root_store = RootCertStore::empty();

        // Load custom CA if provided
        if let Some(ref ca_path) = config.ca_path {
            let ca_file = File::open(ca_path)
                .map_err(|e| TlsError::CaError(format!("{}: {}", ca_path, e)))?;
            let mut ca_reader = BufReader::new(ca_file);
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
                .filter_map(|r| r.ok())
                .collect();

            for cert in certs {
                root_store
                    .add(cert)
                    .map_err(|e| TlsError::CaError(format!("Failed to add CA: {}", e)))?;
            }
        } else {
            // Use system root certificates
            root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }

        // Build client config
        let client_config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        Ok(tokio_rustls::TlsConnector::from(Arc::new(client_config)))
    }

    /// Get the TLS configuration
    pub fn config(&self) -> &TlsConfig {
        &self.config
    }

    /// Connect with TLS (requires `tls` feature)
    #[cfg(feature = "tls")]
    pub async fn connect<S>(
        &self,
        server_name: &str,
        stream: S,
    ) -> TlsResult<tokio_rustls::client::TlsStream<S>>
    where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        use rustls::pki_types::ServerName;

        let name = ServerName::try_from(server_name.to_string())
            .map_err(|e| TlsError::ConfigError(format!("Invalid server name: {}", e)))?;

        self.inner
            .connect(name, stream)
            .await
            .map_err(|e| TlsError::HandshakeError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_server() {
        let config = TlsConfig::server("cert.pem", "key.pem");
        assert_eq!(config.cert_path, Some("cert.pem".to_string()));
        assert_eq!(config.key_path, Some("key.pem".to_string()));
        assert!(!config.verify_peer);
    }

    #[test]
    fn test_tls_config_client() {
        let config = TlsConfig::client(Some("ca.pem"));
        assert!(config.cert_path.is_none());
        assert_eq!(config.ca_path, Some("ca.pem".to_string()));
        assert!(config.verify_peer);
    }

    #[test]
    fn test_tls_config_mtls() {
        let config = TlsConfig::mtls("cert.pem", "key.pem", "ca.pem");
        assert_eq!(config.cert_path, Some("cert.pem".to_string()));
        assert_eq!(config.key_path, Some("key.pem".to_string()));
        assert_eq!(config.ca_path, Some("ca.pem".to_string()));
        assert!(config.verify_peer);
    }

    #[test]
    fn test_tls_acceptor_requires_cert() {
        let config = TlsConfig::default();
        let result = TlsAcceptor::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_acceptor_requires_key() {
        let config = TlsConfig {
            cert_path: Some("cert.pem".into()),
            ..Default::default()
        };
        let result = TlsAcceptor::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_connector_default() {
        let config = TlsConfig::client(None::<&str>);
        let result = TlsConnector::new(config);
        assert!(result.is_ok());
    }
}
