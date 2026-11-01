//! TLS with rustls and ring, with the URL options of mysql_async:
//! `require_ssl` turns it on, `verify_ca=false` takes any certificate,
//! `verify_identity=false` skips the host name check, and
//! `built_in_roots=false` trusts no built-in root, and `ssl_ca` (a PEM
//! file) adds the CA certificates in it, eg. a private CA.
use std::sync::Arc;

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, Error, RootCertStore, SignatureScheme};

use crate::TlsOpts;

pub(crate) const NO_ROOTS: &str = "TLS: no root certificate to trust. \
    Set ssl_ca, turn on built_in_roots, or set verify_ca=false";

/// The CA certificates to trust: the Mozilla roots (unless
/// `built_in_roots=false`) and the ones in the `ssl_ca` file, like
/// mysql_async.
fn roots(o: &TlsOpts) -> Result<RootCertStore, String> {
    use rustls::pki_types::pem::{self, PemObject};
    let mut roots = RootCertStore::empty();
    if o.built_in_roots {
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let Some(path) = &o.ca else {
        return Ok(roots);
    };
    let bad = |e: pem::Error| match e {
        pem::Error::Io(e) => format!("TLS: can not read ssl_ca {path}: {e}"),
        e => format!("TLS: ssl_ca {path} is not a valid PEM file: {e}"),
    };
    let mut added = 0;
    for cert in CertificateDer::pem_file_iter(path).map_err(bad)? {
        roots
            .add(cert.map_err(bad)?)
            .map_err(|e| format!("TLS: ssl_ca {path} has a bad certificate: {e}"))?;
        added += 1;
    }
    if added == 0 {
        return Err(format!("TLS: ssl_ca {path} has no certificate"));
    }
    Ok(roots)
}

/// The client config of a pool, or why it can not be built.
pub(crate) fn config(o: &TlsOpts) -> Result<Arc<ClientConfig>, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = Arc::new(roots(o)?);
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

#[cfg(test)]
mod tests {
    use super::*;

    const CA: &str = "-----BEGIN CERTIFICATE-----
MIIBYjCCAQigAwIBAgIUbdnvPvK5wtqJWfkfXG4e1PePxPIwCgYIKoZIzj0EAwIw
FjEUMBIGA1UEAwwLZG94IHRlc3QgQ0EwIBcNMjYxMTAxMTAxMTEyWhgPMjEyNjEw
MDgxMDExMTJaMBYxFDASBgNVBAMMC2RveCB0ZXN0IENBMFkwEwYHKoZIzj0CAQYI
KoZIzj0DAQcDQgAEUIa+jSXp8ihHNCTjcAtZT+ss5ptKhu8JXQjwaVVoyAc509CA
N/+mYY+A+y6JRFa6xvGjvTJpgGxuLKkAPSUBpaMyMDAwHQYDVR0OBBYEFLheyab9
KiPGWgk9ukVg6QasQVvYMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSAAw
RQIhAKlO10qt+1/EhKsqlj9veRNQo4oW87Q+NhsedytMaFDqAiB9S9nbm8g3vhf6
k63S0NUNDNyDU/ztrFQoZLEiQVEdRg==
-----END CERTIFICATE-----\n";

    fn opts(ca: &str) -> TlsOpts {
        TlsOpts {
            verify_ca: true,
            verify_identity: true,
            built_in_roots: false,
            ca: Some(ca.into()),
        }
    }

    #[test]
    fn ssl_ca_adds_the_certificates_of_the_file() {
        let dir = std::env::temp_dir().join(format!("dox-mysql-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = |name: &str, text: &str| {
            let p = dir.join(name);
            std::fs::write(&p, text).unwrap();
            p.to_string_lossy().into_owned()
        };
        let good = file("good.pem", CA);
        assert_eq!(roots(&opts(&good)).unwrap().len(), 1);
        assert!(config(&opts(&good)).is_ok());
        // the built-in roots stay unless built_in_roots=false
        let both = TlsOpts {
            built_in_roots: true,
            ..opts(&good)
        };
        let built_in = webpki_roots::TLS_SERVER_ROOTS.len();
        assert_eq!(roots(&both).unwrap().len(), built_in + 1);
        let err = |p: &str| config(&opts(p)).unwrap_err();
        let missing = dir.join("missing.pem").to_string_lossy().into_owned();
        assert!(err(&missing).starts_with(&format!("TLS: can not read ssl_ca {missing}: ")));
        let empty = file("empty.pem", "");
        assert_eq!(
            err(&empty),
            format!("TLS: ssl_ca {empty} has no certificate")
        );
        let broken = file("broken.pem", "-----BEGIN CERTIFICATE-----\nnope\n");
        assert!(err(&broken).starts_with(&format!("TLS: ssl_ca {broken} is not a valid PEM file")));
        let not_der = file(
            "not_der.pem",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        );
        assert!(err(&not_der).starts_with(&format!("TLS: ssl_ca {not_der} has a bad certificate")));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
