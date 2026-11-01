//! TLS with rustls and ring, with the URL options of mysql_async:
//! `require_ssl` turns it on, `verify_ca=false` takes any certificate,
//! `verify_identity=false` skips the host name check, and
//! `built_in_roots=false` trusts no built-in root (then only
//! `verify_ca=false` works).
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

use crate::TlsOpts;

pub(crate) const NO_ROOTS: &str = "TLS: no root certificate to trust. \
    Turn on built_in_roots, or set verify_ca=false";

/// The client config of a pool, or why it can not be built.
pub(crate) fn config(o: TlsOpts) -> Result<Arc<ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    if o.built_in_roots {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let roots = Arc::new(roots);
    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS: {e}"))?;
    if o.verify_ca && o.verify_identity {
        return Ok(Arc::new(
            builder.with_root_certificates(roots).with_no_client_auth(),
        ));
    }
    // like mysql_async: with no roots the WebPKI verifier can not be
    // built, but `verify_ca=false` does not need it
    let inner = WebPkiServerVerifier::builder_with_provider(roots, provider.clone())
        .build()
        .ok();
    if inner.is_none() && o.verify_ca {
        return Err(NO_ROOTS.into());
    }
    let verifier = Lax {
        inner,
        verify_ca: o.verify_ca,
        provider,
    };
    Ok(Arc::new(
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(verifier))
            .with_no_client_auth(),
    ))
}

/// A verifier for `verify_ca=false` (take any certificate) or
/// `verify_identity=false` (check the chain, not the host name). The
/// handshake signatures are always checked.
#[derive(Debug)]
struct Lax {
    inner: Option<Arc<WebPkiServerVerifier>>,
    verify_ca: bool,
    provider: Arc<rustls::crypto::CryptoProvider>,
}

impl ServerCertVerifier for Lax {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        let Some(inner) = self.inner.as_ref().filter(|_| self.verify_ca) else {
            return Ok(ServerCertVerified::assertion());
        };
        match inner.verify_server_cert(end_entity, intermediates, server_name, ocsp, now) {
            Err(Error::InvalidCertificate(rustls::CertificateError::NotValidForName))
            | Err(Error::InvalidCertificate(rustls::CertificateError::NotValidForNameContext {
                ..
            })) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}
