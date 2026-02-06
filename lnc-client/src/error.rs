use std::fmt;

use std::net::SocketAddr;

/// Errors that can occur during client operations
#[derive(Debug)]
pub enum ClientError {
    /// Failed to establish a connection to the server
    ConnectionFailed(std::io::Error),
    /// Connection was closed by the server
    ConnectionClosed,
    /// I/O error during communication
    IoError(std::io::Error),
    /// Protocol-level error (malformed data, invalid state)
    ProtocolError(String),
    /// Received an unexpected or invalid response from the server
    InvalidResponse(String),
    /// Operation timed out
    Timeout,
    /// CRC checksum mismatch indicating data corruption
    CrcMismatch {
        /// Expected CRC value
        expected: u32,
        /// Actual CRC value received
        actual: u32,
    },
    /// Server is applying backpressure, client should slow down
    ServerBackpressure,
    /// Server returned an error message
    ServerError(String),
    /// Server is not the leader, redirect to the specified address
    NotLeader {
        /// Address of the current leader, if known
        leader_addr: Option<SocketAddr>,
    },
    /// TLS handshake or configuration error
    TlsError(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionFailed(e) => write!(f, "Connection failed: {}", e),
            Self::ConnectionClosed => write!(f, "Connection closed by server"),
            Self::IoError(e) => write!(f, "I/O error: {}", e),
            Self::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            Self::InvalidResponse(msg) => write!(f, "Invalid response: {}", msg),
            Self::Timeout => write!(f, "Operation timed out"),
            Self::CrcMismatch { expected, actual } => {
                write!(
                    f,
                    "CRC mismatch: expected {:#x}, got {:#x}",
                    expected, actual
                )
            },
            Self::ServerBackpressure => write!(f, "Server signaled backpressure"),
            Self::ServerError(msg) => write!(f, "Server error: {}", msg),
            Self::NotLeader { leader_addr } => match leader_addr {
                Some(addr) => write!(f, "Not leader, redirect to {}", addr),
                None => write!(f, "Not leader, leader unknown"),
            },
            Self::TlsError(msg) => write!(f, "TLS error: {}", msg),
        }
    }
}

/// Parse a NOT_LEADER error message and extract the redirect address if present
pub fn parse_not_leader_error(msg: &str) -> Option<Option<SocketAddr>> {
    if !msg.starts_with("NOT_LEADER:") {
        return None;
    }

    if msg.contains("leader unknown") {
        return Some(None);
    }

    // Parse "NOT_LEADER: redirect to X.X.X.X:PORT"
    if let Some(addr_str) = msg.strip_prefix("NOT_LEADER: redirect to ") {
        if let Ok(addr) = addr_str.trim().parse::<SocketAddr>() {
            return Some(Some(addr));
        }
    }

    Some(None)
}

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ConnectionFailed(e) | Self::IoError(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ClientError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl From<lnc_core::LanceError> for ClientError {
    fn from(err: lnc_core::LanceError) -> Self {
        Self::ProtocolError(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, ClientError>;
