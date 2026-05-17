// SPDX-FileCopyrightText: 2026 Brice Arnould
//
// SPDX-License-Identifier: MIT OR Apache-2.0

//! TLS identity and connection helpers for Syncthing BEP.
//!
//! This crate handles certificate generation, TLS configuration, and peer
//! device ID extraction. It does **not** touch the filesystem — callers are
//! responsible for persisting and loading identity bytes.

use std::fmt;
use std::sync::Arc;

use tokio::sync::OnceCell;

use bepository_bep::DeviceId;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::{
    ClientConfig, DigitallySignedStruct, DistinguishedName, Error as RustlsError, ServerConfig,
    SignatureScheme,
};
use secrecy::{ExposeSecret, SecretSlice};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_rustls::{TlsAcceptor, TlsConnector};

#[derive(Debug, Error)]
pub enum TlsError {
    #[error("certificate generation failed: {0}")]
    CertGen(#[from] rcgen::Error),

    #[error("TLS error: {0}")]
    Rustls(#[from] rustls::Error),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("peer did not present a certificate")]
    NoPeerCertificate,
}

/// A Syncthing TLS identity: certificate, private key, and derived device ID.
///
/// Does not implement `Clone` (to prevent accidental key duplication) or
/// `Debug` with key material (to prevent accidental logging of secrets).
pub struct Identity {
    cert_der: Vec<u8>,
    key_der: SecretSlice<u8>,
    device_id: DeviceId,

    client_config: OnceCell<Arc<ClientConfig>>,
    server_config: OnceCell<Arc<ServerConfig>>,
}

impl Identity {
    /// Generate a fresh self-signed identity.
    ///
    /// The certificate includes the "syncthing" DNS SAN required by the
    /// Syncthing protocol.
    pub fn generate() -> Result<Self, TlsError> {
        let cert = rcgen::generate_simple_self_signed(vec!["syncthing".to_string()])?;
        let cert_der = cert.cert.der().to_vec();
        let key_der = SecretSlice::from(cert.signing_key.serialize_der());
        Self::from_der(cert_der, key_der)
    }

    /// Reconstruct an identity from previously serialized DER bytes.
    ///
    /// `key_der` accepts both `Vec<u8>` and `SecretSlice<u8>` — pass
    /// whichever you have. The key is always stored as `SecretSlice<u8>`
    /// internally.
    pub fn from_der(cert_der: Vec<u8>, key_der: SecretSlice<u8>) -> Result<Self, TlsError> {
        let hash: [u8; 32] = Sha256::digest(&cert_der).into();
        let device_id = DeviceId::from_bytes(hash);
        Ok(Self {
            cert_der,
            key_der,
            device_id,
            client_config: OnceCell::new(),
            server_config: OnceCell::new(),
        })
    }

    /// The DER-encoded certificate (public).
    pub fn cert_der(&self) -> &[u8] {
        &self.cert_der
    }

    /// The DER-encoded private key, wrapped in `SecretSlice` for safe handling.
    pub fn key_der(&self) -> &SecretSlice<u8> {
        &self.key_der
    }

    /// The device ID derived from this identity's certificate.
    pub fn device_id(&self) -> &DeviceId {
        &self.device_id
    }

    /// Get a cached rustls `ClientConfig` for outgoing connections.
    pub async fn client_config(&self) -> Result<Arc<ClientConfig>, TlsError> {
        self.client_config
            .get_or_try_init(|| async {
                let cert = CertificateDer::from(self.cert_der.clone());
                let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    self.key_der.expose_secret().to_vec(),
                ));
                let mut config = ClientConfig::builder()
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(AcceptAnyCert))
                    .with_client_auth_cert(vec![cert], key)?;
                config.alpn_protocols = vec![b"bep/1.0".to_vec()];
                Ok(Arc::new(config))
            })
            .await
            .map(Arc::clone)
    }

    /// Get a cached rustls `ServerConfig` for incoming connections.
    pub async fn server_config(&self) -> Result<Arc<ServerConfig>, TlsError> {
        self.server_config
            .get_or_try_init(|| async {
                let cert = CertificateDer::from(self.cert_der.clone());
                let key =
                    PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.expose_secret()));
                let mut config = ServerConfig::builder()
                    .with_client_cert_verifier(Arc::new(AcceptAnyClientCert))
                    .with_single_cert(vec![cert], key.clone_key())?;
                config.alpn_protocols = vec![b"bep/1.0".to_vec()];
                Ok(Arc::new(config))
            })
            .await
            .map(Arc::clone)
    }
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("device_id", &self.device_id)
            .finish_non_exhaustive()
    }
}

/// A TLS stream paired with the peer's verified device ID.
pub struct BepStream<T> {
    pub stream: T,
    pub peer_device_id: DeviceId,
}

impl<T: fmt::Debug> fmt::Debug for BepStream<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BepStream")
            .field("peer_device_id", &self.peer_device_id)
            .finish_non_exhaustive()
    }
}

/// Extract a `DeviceId` from a peer's certificate chain.
pub fn peer_device_id(certs: &[CertificateDer<'_>]) -> Result<DeviceId, TlsError> {
    let leaf = certs.first().ok_or(TlsError::NoPeerCertificate)?;
    let hash: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
    Ok(DeviceId::from_bytes(hash))
}

/// Connect to a Syncthing peer over TLS and return a `BepStream`.
#[tracing::instrument(level = "info", skip(identity), fields(addr = %addr))]
pub async fn connect(
    addr: &str,
    identity: &Identity,
) -> Result<BepStream<tokio_rustls::client::TlsStream<TcpStream>>, TlsError> {
    let addr = addr.strip_prefix("tcp://").unwrap_or(addr);
    let tcp = TcpStream::connect(addr).await?;
    let config = identity.client_config().await?;
    let connector = TlsConnector::from(config);
    let server_name = ServerName::try_from("syncthing").map_err(|_| {
        TlsError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "invalid server name",
        ))
    })?;
    let tls_stream = connector.connect(server_name, tcp).await?;

    let (_, conn) = tls_stream.get_ref();
    let certs = conn
        .peer_certificates()
        .ok_or(TlsError::NoPeerCertificate)?;
    let peer_id = peer_device_id(certs)?;

    tracing::info!(remote_device = %peer_id, "tls connected");

    Ok(BepStream {
        stream: tls_stream,
        peer_device_id: peer_id,
    })
}

/// Accept an incoming TLS connection and return a `BepStream`.
#[tracing::instrument(level = "info", skip(tcp_stream, identity))]
pub async fn accept(
    tcp_stream: TcpStream,
    identity: &Identity,
) -> Result<BepStream<tokio_rustls::server::TlsStream<TcpStream>>, TlsError> {
    let config = identity.server_config().await?;
    let acceptor = TlsAcceptor::from(config);
    let tls_stream = acceptor.accept(tcp_stream).await?;

    let (_, conn) = tls_stream.get_ref();
    let certs = conn
        .peer_certificates()
        .ok_or(TlsError::NoPeerCertificate)?;
    let peer_id = peer_device_id(certs)?;

    tracing::info!(remote_device = %peer_id, "tls accepted");

    Ok(BepStream {
        stream: tls_stream,
        peer_device_id: peer_id,
    })
}

// ---------------------------------------------------------------------------
// Certificate verifiers — Syncthing uses self-signed certs; identity is
// established via device IDs (cert hash), not CA chains.
// ---------------------------------------------------------------------------

macro_rules! impl_skip_signature_verification {
    () => {
        fn verify_tls12_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn verify_tls13_signature(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, RustlsError> {
            Ok(HandshakeSignatureValid::assertion())
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            all_schemes()
        }
    };
}

#[derive(Debug)]
struct AcceptAnyCert;

impl ServerCertVerifier for AcceptAnyCert {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    impl_skip_signature_verification!();
}

#[derive(Debug)]
struct AcceptAnyClientCert;

impl ClientCertVerifier for AcceptAnyClientCert {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, RustlsError> {
        Ok(ClientCertVerified::assertion())
    }

    impl_skip_signature_verification!();

    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }
}

fn all_schemes() -> Vec<SignatureScheme> {
    vec![
        SignatureScheme::RSA_PKCS1_SHA256,
        SignatureScheme::RSA_PKCS1_SHA384,
        SignatureScheme::RSA_PKCS1_SHA512,
        SignatureScheme::ECDSA_NISTP256_SHA256,
        SignatureScheme::ECDSA_NISTP384_SHA384,
        SignatureScheme::ECDSA_NISTP521_SHA512,
        SignatureScheme::RSA_PSS_SHA256,
        SignatureScheme::RSA_PSS_SHA384,
        SignatureScheme::RSA_PSS_SHA512,
        SignatureScheme::ED25519,
    ]
}
