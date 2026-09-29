// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::time::Duration;

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use serde::{Deserialize, Serialize};

#[cfg(feature = "aws")]
use crate::cloud_auth;

use otel_arrow_dfe_engine::shared::capability::auth::bearer_token_provider::BearerTokenProvider;

#[cfg(any(feature = "azure", feature = "aws"))]
use object_store::path::Path;
#[cfg(any(feature = "azure", feature = "aws"))]
use object_store::prefix::PrefixStore;
#[cfg(any(feature = "azure", feature = "aws"))]
use object_store::{BackoffConfig, RetryConfig};

/// Azure object storage
#[cfg(feature = "azure")]
pub mod azure;

/// Retry settings for object-store-backed storage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "RetryOptionsUnchecked")]
pub struct RetryOptions {
    /// The maximum number of times to retry a request. Set to 0 to disable retries.
    pub max_retries: usize,

    /// Initial exponential backoff duration.
    #[serde(with = "humantime_serde")]
    pub init_backoff: Duration,

    /// Maximum exponential backoff duration.
    #[serde(with = "humantime_serde")]
    pub max_backoff: Duration,

    /// Exponential backoff multiplier.
    pub backoff_base: f64,

    /// Maximum elapsed time after which no further retries are attempted.
    #[serde(with = "humantime_serde")]
    pub retry_timeout: Duration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetryOptionsUnchecked {
    #[serde(default = "default_max_retries")]
    max_retries: usize,

    #[serde(default = "default_init_backoff")]
    #[serde(with = "humantime_serde")]
    init_backoff: Duration,

    #[serde(default = "default_max_backoff")]
    #[serde(with = "humantime_serde")]
    max_backoff: Duration,

    #[serde(default = "default_backoff_base")]
    backoff_base: f64,

    #[serde(default = "default_retry_timeout")]
    #[serde(with = "humantime_serde")]
    retry_timeout: Duration,
}

impl TryFrom<RetryOptionsUnchecked> for RetryOptions {
    type Error = object_store::Error;

    fn try_from(value: RetryOptionsUnchecked) -> Result<Self, Self::Error> {
        let retry = Self {
            max_retries: value.max_retries,
            init_backoff: value.init_backoff,
            max_backoff: value.max_backoff,
            backoff_base: value.backoff_base,
            retry_timeout: value.retry_timeout,
        };
        retry.validate()?;
        Ok(retry)
    }
}

// Keep these values aligned with object_store::RetryConfig::default() and
// object_store::BackoffConfig::default().
const fn default_max_retries() -> usize {
    10
}

const fn default_init_backoff() -> Duration {
    Duration::from_millis(100)
}

const fn default_max_backoff() -> Duration {
    Duration::from_secs(15)
}

const fn default_backoff_base() -> f64 {
    2.0
}

const fn default_retry_timeout() -> Duration {
    Duration::from_secs(3 * 60)
}

impl RetryOptions {
    /// Validate the options against object_store retry/backoff constraints.
    pub fn validate(&self) -> Result<(), object_store::Error> {
        if !self.backoff_base.is_finite() || self.backoff_base <= 1.0 {
            return Err(object_store::Error::Generic {
                store: "retry",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "backoff_base must be finite and greater than 1.0",
                )),
            });
        }

        if self.init_backoff == Duration::ZERO {
            return Err(object_store::Error::Generic {
                store: "retry",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "init_backoff must be greater than 0",
                )),
            });
        }

        if self.init_backoff > self.max_backoff {
            return Err(object_store::Error::Generic {
                store: "retry",
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "init_backoff must be less than or equal to max_backoff",
                )),
            });
        }

        Ok(())
    }

    /// Convert these options to the retry configuration used by the object_store crate.
    #[cfg(any(feature = "azure", feature = "aws"))]
    pub fn to_object_store_retry_config(&self) -> Result<RetryConfig, object_store::Error> {
        self.validate()?;
        Ok(RetryConfig {
            backoff: BackoffConfig {
                init_backoff: self.init_backoff,
                max_backoff: self.max_backoff,
                base: self.backoff_base,
            },
            max_retries: self.max_retries,
            retry_timeout: self.retry_timeout,
        })
    }
}

/// Supported object storage types
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageType {
    /// File storage
    File {
        /// The root directory for writing files
        base_uri: String,
    },

    /// Azure storage
    #[cfg(feature = "azure")]
    #[non_exhaustive]
    Azure {
        /// The base URI for the azure storage backend. Many are supported:
        ///
        /// - Blob: `https://<account>.blob.core.windows.net/<container>`
        /// - Fabric: `https://<account>.dfs.fabric.microsoft.com`
        /// - More: See [object_store::azure::MicrosoftAzureBuilder::with_url]
        base_uri: String,

        /// Optional blob service endpoint that replaces the one derived from
        /// the account in `base_uri`, for example a sovereign cloud or a
        /// private endpoint. It must be `https://`: the Azure client refuses
        /// plain HTTP, so the Azurite emulator works only when it serves TLS
        /// (`https://127.0.0.1:10000/devstoreaccount1`). `base_uri` keeps
        /// the public-cloud form above,
        /// `https://<account>.blob.core.windows.net/<container>[/<prefix>]`,
        /// which names the account, the container and any prefix; another
        /// host in `base_uri` is refused.
        #[serde(default)]
        endpoint: Option<String>,
    },

    /// AWS S3 storage
    #[cfg(feature = "aws")]
    #[non_exhaustive]
    S3 {
        /// The S3 bucket URI, e.g. `s3://my-bucket/prefix`
        base_uri: String,

        /// AWS region, e.g. `us-east-1`. If not provided, falls back to
        /// environment and default AWS provider chain behavior.
        region: Option<String>,

        /// Optional custom endpoint URL (for S3-compatible stores like
        /// LocalStack).
        endpoint: Option<String>,

        /// Whether to allow HTTP (non-TLS) connections.
        allow_http: Option<bool>,

        /// Whether to use virtual hosted-style requests.
        /// Set to false for S3-compatible stores that require path-style.
        virtual_hosted_style_request: Option<bool>,

        /// Whether requests are signed with SigV4 `UNSIGNED-PAYLOAD` instead
        /// of a SHA-256 of every uploaded byte, which saves that hashing CPU
        /// and relies on TLS for the payload's integrity. Unset,
        /// `AWS_UNSIGNED_PAYLOAD` decides, and without it every payload is
        /// signed.
        unsigned_payload: Option<bool>,

        /// The auth settings, see [cloud_auth::aws::AuthMethod]
        auth: cloud_auth::aws::AuthMethod,
    },
}

impl StorageType {
    /// The backend's name, for logs: `file`, `azure` or `s3`.
    ///
    /// Never any of the variant's fields, which may name credentials.
    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::File { .. } => "file",
            #[cfg(feature = "azure")]
            Self::Azure { .. } => "azure",
            #[cfg(feature = "aws")]
            Self::S3 { .. } => "s3",
        }
    }

    /// Whether this storage backend obtains credentials from a bearer token capability.
    #[must_use]
    pub const fn requires_bearer_token_provider(&self) -> bool {
        #[cfg(feature = "azure")]
        if matches!(self, Self::Azure { .. }) {
            return true;
        }

        false
    }
}

/// Extract the path prefix from a cloud storage URI that builders discard.
///
/// Cloud storage builders (`AmazonS3Builder::with_url`, `MicrosoftAzureBuilder::with_url`)
/// parse the bucket/container from the URL but discard any path after it. This function
/// extracts that discarded path so it can be used with `PrefixStore`.
///
/// Examples:
/// - `s3://bucket/telemetry` -> `Some(Path::from("telemetry"))`
/// - `s3://bucket` -> `None`
/// - `az://container/prefix` -> `Some(Path::from("prefix"))`
/// - `https://account.blob.core.windows.net/container/prefix` -> `Some(Path::from("prefix"))`
#[cfg(any(feature = "azure", feature = "aws"))]
fn extract_path_prefix(base_uri: &str) -> Result<Option<Path>, object_store::Error> {
    let url = url::Url::parse(base_uri).map_err(|e| object_store::Error::Generic {
        store: "cloud",
        source: Box::new(e),
    })?;

    let path = url.path();

    // For scheme-based URIs (s3://, az://, abfs://), the path starts with '/'
    // and the entire path after the leading slash (minus any trailing '/') is
    // treated as the prefix.
    // For HTTPS Azure URIs, the first path segment is the container name, and
    // the prefix is the remainder of the path after that container segment
    // (minus any trailing '/').
    let is_https = url.scheme() == "https" || url.scheme() == "http";

    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }

    let prefix = if is_https {
        // For HTTPS URLs like https://account.blob.core.windows.net/container/prefix/sub
        // Skip the first segment (container) and use the rest
        match trimmed.find('/') {
            Some(idx) => {
                let after_container = &trimmed[idx + 1..];
                let after_container = after_container.trim_end_matches('/');
                if after_container.is_empty() {
                    return Ok(None);
                }
                after_container
            }
            None => return Ok(None), // Only container, no prefix
        }
    } else {
        // For scheme-based URIs (s3://bucket/prefix, az://container/prefix)
        // the host is the bucket/container, path is the prefix
        trimmed.trim_end_matches('/')
    };

    if prefix.is_empty() {
        Ok(None)
    } else {
        Ok(Some(Path::from(prefix)))
    }
}

/// Wrap an object store with a `PrefixStore` if the URI contains a path prefix.
#[cfg(any(feature = "azure", feature = "aws"))]
fn wrap_with_prefix(
    store: impl ObjectStore,
    base_uri: &str,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    if let Some(prefix) = extract_path_prefix(base_uri)? {
        Ok(Arc::new(PrefixStore::new(store, prefix)))
    } else {
        Ok(Arc::new(store))
    }
}

/// Fetch an object store based on the provide storage
pub fn from_storage_type(
    storage: &StorageType,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    from_storage_type_with_retry(storage, None)
}

/// Fetch an object store based on the provided storage, applying retry settings when supported.
pub fn from_storage_type_with_retry(
    storage: &StorageType,
    retry: Option<&RetryOptions>,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    from_storage_type_with_retry_and_token_provider(storage, retry, None)
}

/// The bearer token provider `storage` needs, taken from a node's bound
/// capabilities, or `None` for a backend that obtains no bearer token.
///
/// Shared by every exporter that writes through an object store.
pub fn required_token_provider(
    storage: &StorageType,
    capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<Option<Box<dyn BearerTokenProvider>>, otel_arrow_dfe_config::error::Error> {
    if !storage.requires_bearer_token_provider() {
        return Ok(None);
    }
    capabilities
        .require_shared::<otel_arrow_dfe_engine::capability::auth::bearer_token_provider::BearerTokenProvider>()
        .map(Some)
        .map_err(|e| otel_arrow_dfe_config::error::Error::InvalidUserConfig {
            error: e.to_string(),
        })
}

/// Build the object store an exporter writes through, reported as that
/// exporter's configuration error when it cannot be built.
///
/// Retry settings apply only to cloud backends; given for local file
/// storage they are validated and otherwise ignored. Each exporter logs
/// that under its own event name, as before this helper existed.
pub fn exporter_store(
    exporter: otel_arrow_dfe_engine::node::NodeId,
    storage: &StorageType,
    retry: Option<&RetryOptions>,
    token_provider: Option<Box<dyn BearerTokenProvider>>,
) -> Result<Arc<dyn ObjectStore>, otel_arrow_dfe_engine::error::Error> {
    build_store(storage, retry, token_provider).map_err(|e| {
        otel_arrow_dfe_engine::error::Error::ExporterError {
            exporter,
            kind: otel_arrow_dfe_engine::error::ExporterErrorKind::Configuration,
            error: format!("error initializing object store {e}"),
            source_detail: otel_arrow_dfe_engine::error::format_error_sources(&e),
        }
    })
}

/// Whether the S3 client `storage` builds signs `UNSIGNED-PAYLOAD`, resolved
/// from the section and the process environment; `None` for another backend.
#[must_use]
pub fn s3_unsigned_payload(storage: &StorageType) -> Option<bool> {
    #[cfg(feature = "aws")]
    if matches!(storage, StorageType::S3 { .. }) {
        let builder = s3_builder(
            storage,
            None,
            object_store::aws::AmazonS3Builder::from_env(),
        )
        .ok()?;
        return Some(signs_unsigned_payload(&builder));
    }
    let _ = storage;
    None
}

/// Whether a client built by `builder` signs `UNSIGNED-PAYLOAD`.
///
/// The builder returns an environment value unparsed; object_store's boolean
/// parser is private, so its accepted spellings are repeated here.
#[cfg(feature = "aws")]
fn signs_unsigned_payload(builder: &object_store::aws::AmazonS3Builder) -> bool {
    builder
        .get_config_value(&object_store::aws::AmazonS3ConfigKey::UnsignedPayload)
        .is_some_and(|v| {
            matches!(
                v.to_ascii_lowercase().as_str(),
                "1" | "true" | "on" | "yes" | "y"
            )
        })
}

/// Fetch an object store and use the supplied bearer token provider for Azure storage.
pub fn from_storage_type_with_retry_and_token_provider(
    storage: &StorageType,
    retry: Option<&RetryOptions>,
    token_provider: Option<Box<dyn BearerTokenProvider>>,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    build_store(storage, retry, token_provider)
}

fn build_store(
    storage: &StorageType,
    retry: Option<&RetryOptions>,
    token_provider: Option<Box<dyn BearerTokenProvider>>,
) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
    #[cfg(not(feature = "azure"))]
    let _ = token_provider;

    if let Some(retry) = retry {
        retry.validate()?;
    }

    match storage {
        StorageType::File { base_uri } => {
            #[cfg(any(test, feature = "test-utils"))]
            {
                if base_uri.starts_with("testdelayed://") {
                    return test::delayed_test_object_store(base_uri);
                }
            }

            let object_store = LocalFileSystem::new_with_prefix(base_uri)?;
            Ok(Arc::new(object_store))
        }

        #[cfg(feature = "azure")]
        StorageType::Azure { base_uri, endpoint } => {
            use object_store::azure::MicrosoftAzureBuilder;

            let token_provider = token_provider.ok_or_else(|| object_store::Error::Generic {
                store: "Azure",
                source: "Azure storage requires a bound bearer_token_provider capability".into(),
            })?;
            let credential_provider = azure::AzureTokenCredentialProvider::new(token_provider);

            let mut builder = MicrosoftAzureBuilder::new()
                .with_url(base_uri)
                .with_credentials(Arc::new(credential_provider));
            if let Some(endpoint) = endpoint {
                // The Azure client refuses plain HTTP on every request.
                if endpoint
                    .get(..7)
                    .is_some_and(|scheme| scheme.eq_ignore_ascii_case("http://"))
                {
                    return Err(object_store::Error::Generic {
                        store: "Azure",
                        source: format!(
                            "endpoint {endpoint} must use https://; the Azure client refuses \
                             plain HTTP, such as Azurite's default endpoint"
                        )
                        .into(),
                    });
                }
                builder = builder.with_endpoint(endpoint.clone());
            }
            if let Some(retry) = retry {
                builder = builder.with_retry(retry.to_object_store_retry_config()?);
            }

            let store = builder.build().map_err(|e| object_store::Error::Generic {
                store: "Azure",
                source: format!(
                    "cannot build the client for base_uri {base_uri}, which must name the \
                     account and container as \
                     https://<account>.blob.core.windows.net/<container>[/<prefix>], also \
                     when an endpoint is set: {e}"
                )
                .into(),
            })?;
            wrap_with_prefix(store, base_uri)
        }

        #[cfg(feature = "aws")]
        StorageType::S3 { base_uri, .. } => {
            let store = s3_builder(
                storage,
                retry,
                object_store::aws::AmazonS3Builder::from_env(),
            )?
            .build()?;
            wrap_with_prefix(store, base_uri)
        }
    }
}

/// The S3 client builder for `storage`, starting from `base`
/// (`AmazonS3Builder::from_env()` outside tests), whose settings the
/// section's override; another backend is an error.
#[cfg(feature = "aws")]
fn s3_builder(
    storage: &StorageType,
    retry: Option<&RetryOptions>,
    base: object_store::aws::AmazonS3Builder,
) -> Result<object_store::aws::AmazonS3Builder, object_store::Error> {
    let StorageType::S3 {
        base_uri,
        region,
        endpoint,
        allow_http,
        virtual_hosted_style_request,
        unsigned_payload,
        auth,
    } = storage
    else {
        return Err(object_store::Error::Generic {
            store: "S3",
            source: "not an S3 storage configuration".into(),
        });
    };
    let mut builder = base.with_url(base_uri);
    if let Some(region) = region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = endpoint {
        builder = builder.with_endpoint(endpoint);
    }
    if let Some(allow) = allow_http {
        builder = builder.with_allow_http(*allow);
    }
    if let Some(vhost) = virtual_hosted_style_request {
        builder = builder.with_virtual_hosted_style_request(*vhost);
    }
    if let Some(unsigned) = unsigned_payload {
        builder = builder.with_unsigned_payload(*unsigned);
    }
    if let Some(retry) = retry {
        builder = builder.with_retry(retry.to_object_store_retry_config()?);
    }
    Ok(cloud_auth::aws::configure_builder(builder, auth))
}

#[cfg(any(test, feature = "test-utils"))]
#[allow(dead_code, unused_imports)]
mod test {
    use futures::stream::BoxStream;
    use object_store::path::Path;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    };
    use serde_json::json;
    use std::fmt::Display;
    use std::time::Duration;
    use tokio::time::sleep;
    use url::Url;

    use super::*;

    /// Scenario: an exporter resolves the token provider for local file
    /// storage with no capability bound, then builds its store once with a
    /// usable root and once with a root that does not exist.
    /// Guarantees: file storage needs no bearer token capability, a usable
    /// store is returned, and a store that cannot be built is reported as
    /// that exporter's configuration error naming the object store, which
    /// is the one mapping every object-store exporter shares.
    #[test]
    fn exporter_store_wiring_is_shared() {
        let dir = tempfile::tempdir().expect("tempdir");
        let storage = StorageType::File {
            base_uri: dir.path().to_string_lossy().into_owned(),
        };
        let capabilities = otel_arrow_dfe_engine::capability::registry::Capabilities::empty();
        assert!(
            required_token_provider(&storage, &capabilities)
                .expect("file storage needs no capability")
                .is_none()
        );
        let id = || otel_arrow_dfe_engine::node::NodeId {
            index: 0,
            name: "exporter".into(),
        };
        assert!(exporter_store(id(), &storage, None, None).is_ok());

        let broken = StorageType::File {
            base_uri: dir.path().join("missing").to_string_lossy().into_owned(),
        };
        match exporter_store(id(), &broken, None, None) {
            Err(otel_arrow_dfe_engine::error::Error::ExporterError { kind, error, .. }) => {
                assert_eq!(
                    kind,
                    otel_arrow_dfe_engine::error::ExporterErrorKind::Configuration
                );
                assert!(
                    error.starts_with("error initializing object store"),
                    "{error}"
                );
            }
            Err(other) => panic!("expected an exporter configuration error, got {other}"),
            Ok(_) => panic!("a missing root cannot be opened"),
        }
    }

    #[test]
    fn retry_options_deserialize_duration_strings() {
        let retry: RetryOptions = serde_json::from_value(json!({
            "max_retries": 10,
            "init_backoff": "200ms",
            "max_backoff": "30s",
            "backoff_base": 2.0,
            "retry_timeout": "2min"
        }))
        .unwrap();

        assert_eq!(retry.max_retries, 10);
        assert_eq!(retry.init_backoff, Duration::from_millis(200));
        assert_eq!(retry.max_backoff, Duration::from_secs(30));
        assert_eq!(retry.backoff_base, 2.0);
        assert_eq!(retry.retry_timeout, Duration::from_secs(120));
    }

    #[test]
    fn retry_options_deserialize_uses_object_store_defaults() {
        let retry: RetryOptions = serde_json::from_value(json!({
            "max_retries": 5
        }))
        .unwrap();

        assert_eq!(retry.max_retries, 5);
        assert_eq!(retry.init_backoff, Duration::from_millis(100));
        assert_eq!(retry.max_backoff, Duration::from_secs(15));
        assert_eq!(retry.backoff_base, 2.0);
        assert_eq!(retry.retry_timeout, Duration::from_secs(180));
    }

    #[test]
    fn retry_options_deserialize_rejects_invalid_backoff() {
        let result = serde_json::from_value::<RetryOptions>(json!({
            "max_retries": 5,
            "init_backoff": "100ms",
            "max_backoff": "15s",
            "backoff_base": 1.0,
            "retry_timeout": "3min"
        }));

        assert!(result.is_err());
    }

    #[test]
    #[cfg(any(feature = "azure", feature = "aws"))]
    fn retry_options_translate_to_object_store_retry_config() {
        let retry = RetryOptions {
            max_retries: 3,
            init_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            backoff_base: 1.5,
            retry_timeout: Duration::from_secs(60),
        };

        let translated = retry.to_object_store_retry_config().unwrap();
        assert_eq!(translated.max_retries, 3);
        assert_eq!(translated.backoff.init_backoff, Duration::from_millis(50));
        assert_eq!(translated.backoff.max_backoff, Duration::from_secs(5));
        assert_eq!(translated.backoff.base, 1.5);
        assert_eq!(translated.retry_timeout, Duration::from_secs(60));
    }

    #[test]
    fn retry_options_validate_rejects_invalid_backoff_base() {
        let retry = RetryOptions {
            max_retries: 3,
            init_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            backoff_base: 0.99,
            retry_timeout: Duration::from_secs(60),
        };

        assert!(retry.validate().is_err());
    }

    #[test]
    fn retry_options_validate_rejects_backoff_base_one() {
        let retry = RetryOptions {
            max_retries: 3,
            init_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            backoff_base: 1.0,
            retry_timeout: Duration::from_secs(60),
        };

        assert!(retry.validate().is_err());
    }

    #[test]
    fn retry_options_validate_rejects_zero_initial_backoff() {
        let retry = RetryOptions {
            max_retries: 3,
            init_backoff: Duration::ZERO,
            max_backoff: Duration::from_secs(5),
            backoff_base: 2.0,
            retry_timeout: Duration::from_secs(60),
        };

        assert!(retry.validate().is_err());
    }

    #[test]
    fn retry_options_validate_rejects_inverted_backoff_durations() {
        let retry = RetryOptions {
            max_retries: 3,
            init_backoff: Duration::from_secs(6),
            max_backoff: Duration::from_secs(5),
            backoff_base: 2.0,
            retry_timeout: Duration::from_secs(60),
        };

        assert!(retry.validate().is_err());
    }

    #[test]
    fn file_storage_accepts_absent_retry_and_explicit_retry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = StorageType::File {
            base_uri: temp_dir.path().to_string_lossy().to_string(),
        };
        let retry = RetryOptions {
            max_retries: 1,
            init_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
            backoff_base: 2.0,
            retry_timeout: Duration::from_secs(1),
        };

        let _ = from_storage_type(&storage).unwrap();
        let _ = from_storage_type_with_retry(&storage, Some(&retry)).unwrap();
    }

    #[test]
    fn file_storage_rejects_invalid_retry() {
        let temp_dir = tempfile::tempdir().unwrap();
        let storage = StorageType::File {
            base_uri: temp_dir.path().to_string_lossy().to_string(),
        };
        let retry = RetryOptions {
            max_retries: 1,
            init_backoff: Duration::ZERO,
            max_backoff: Duration::from_millis(10),
            backoff_base: 2.0,
            retry_timeout: Duration::from_secs(1),
        };

        assert!(from_storage_type_with_retry(&storage, Some(&retry)).is_err());
    }

    #[cfg(any(feature = "azure", feature = "aws"))]
    fn valid_retry_options() -> RetryOptions {
        RetryOptions {
            max_retries: 3,
            init_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            backoff_base: 2.0,
            retry_timeout: Duration::from_secs(60),
        }
    }

    /// Creates an instance of object store that will have it's writes delayed by some amount.
    /// The amount to delay should be in the querystring parameters of the uri
    pub(super) fn delayed_test_object_store(
        uri: &str,
    ) -> Result<Arc<dyn ObjectStore>, object_store::Error> {
        let url = Url::parse(uri).map_err(|e| object_store::Error::Generic {
            store: "test_delayed",
            source: Box::new(e),
        })?;

        let path = url.path().to_string();

        // On Windows, url.path() returns "/C:/..." for file paths; strip the leading slash
        // so that LocalFileSystem receives a valid Windows path.
        #[cfg(windows)]
        let path = path
            .strip_prefix('/')
            .map(|s| s.to_string())
            .unwrap_or(path);

        let delay = url
            .query_pairs()
            .find(|(k, _)| k == "delay")
            .map(|(_, v)| {
                let s = v.as_ref();
                humantime::parse_duration(s).unwrap_or(Duration::from_millis(0))
            })
            .unwrap_or(Duration::from_millis(0));

        let fs_store = LocalFileSystem::new_with_prefix(path)?;
        Ok(Arc::new(DelayedObjectStore::new(fs_store, delay)))
    }

    /// An implementation of object store that does a little delay before it writes data. This can
    /// be used for testing various write timeout scenarios
    #[derive(Debug)]
    pub struct DelayedObjectStore<S> {
        inner: Arc<S>,
        delay: Duration,
    }

    impl<S> DelayedObjectStore<S> {
        pub fn new(inner: S, delay: Duration) -> Self {
            Self {
                inner: Arc::new(inner),
                delay,
            }
        }
    }

    impl<S> Display for DelayedObjectStore<S> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // Show inner type name + delay
            write!(
                f,
                "DelayedObjectStore(inner={}, delay={:?})",
                std::any::type_name::<S>(),
                self.delay
            )
        }
    }

    #[async_trait::async_trait]
    impl<S> ObjectStore for DelayedObjectStore<S>
    where
        S: ObjectStore + Send + Sync + 'static,
    {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> Result<PutResult> {
            sleep(self.delay).await;
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(&self, location: &Path, opts: GetOptions) -> Result<GetResult> {
            self.inner.get_opts(location, opts).await
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    #[test]
    fn test_get_testdelayed_file_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().to_str().unwrap().replace('\\', "/");
        let base_uri = format!("testdelayed:///{path}");
        let storage = StorageType::File { base_uri };
        assert!(from_storage_type(&storage).is_ok());
    }

    #[test]
    fn test_get_file_storage() {
        let tmp = tempfile::tempdir().unwrap();
        let base_uri = tmp.path().to_str().unwrap().to_string();
        let storage = StorageType::File { base_uri };
        assert!(from_storage_type(&storage).is_ok());
    }

    /// Scenario: each storage backend compiled in is asked for its name.
    /// Guarantees: the name is the lowercase backend (`file`, `azure`, `s3`)
    /// and never carries a field of the variant, so a start event can name
    /// the backend without risking a credential in the log.
    #[test]
    fn each_storage_backend_names_its_kind() {
        let file = StorageType::File {
            base_uri: "/tmp/secret-path".to_string(),
        };
        assert_eq!(file.kind(), "file");

        #[cfg(feature = "azure")]
        {
            let azure = StorageType::Azure {
                base_uri: "https://mystorageaccount.blob.core.windows.net/container".to_string(),
                endpoint: None,
            };
            assert_eq!(azure.kind(), "azure");
        }

        #[cfg(feature = "aws")]
        {
            let s3 = StorageType::S3 {
                base_uri: "s3://my-bucket/telemetry".to_string(),
                region: None,
                endpoint: None,
                allow_http: None,
                virtual_hosted_style_request: None,
                unsigned_payload: None,
                auth: cloud_auth::aws::AuthMethod::Default,
            };
            assert_eq!(s3.kind(), "s3");
        }
    }

    /// Scenario: Each supported storage backend is asked whether it needs a bearer token capability.
    /// Guarantees: Only Azure demands the binding, so file and S3 pipelines start without one.
    #[test]
    fn only_azure_requires_a_bearer_token_provider() {
        let file = StorageType::File {
            base_uri: "/tmp/otap".to_string(),
        };
        assert!(!file.requires_bearer_token_provider());

        #[cfg(feature = "azure")]
        {
            let azure = StorageType::Azure {
                base_uri: "https://mystorageaccount.blob.core.windows.net/container".to_string(),
                endpoint: None,
            };
            assert!(azure.requires_bearer_token_provider());
        }

        #[cfg(feature = "aws")]
        {
            let s3 = StorageType::S3 {
                base_uri: "s3://my-bucket/telemetry".to_string(),
                region: None,
                endpoint: None,
                allow_http: None,
                virtual_hosted_style_request: None,
                unsigned_payload: None,
                auth: cloud_auth::aws::AuthMethod::Default,
            };
            assert!(!s3.requires_bearer_token_provider());
        }
    }

    /// Scenario: Azure storage is constructed without a bearer token capability.
    /// Guarantees: Construction fails before any unauthenticated storage client is returned.
    #[test]
    #[cfg(feature = "azure")]
    fn test_get_azure_storage_requires_token_provider() {
        crate::crypto::ensure_crypto_provider();
        let storage = StorageType::Azure {
            base_uri: "https://mystorageaccount.blob.core.windows.net/container".to_string(),
            endpoint: None,
        };
        assert!(from_storage_type(&storage).is_err());
    }

    /// Scenario: Azure storage with retry settings lacks a bearer token capability.
    /// Guarantees: Retry configuration does not bypass the required auth capability.
    #[test]
    #[cfg(feature = "azure")]
    fn test_get_azure_storage_with_retry() {
        crate::crypto::ensure_crypto_provider();
        let storage = StorageType::Azure {
            base_uri: "https://mystorageaccount.blob.core.windows.net/container".to_string(),
            endpoint: None,
        };
        let retry = valid_retry_options();
        assert!(from_storage_type_with_retry(&storage, Some(&retry)).is_err());
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_get_s3_storage() {
        crate::crypto::ensure_crypto_provider();
        let storage = StorageType::S3 {
            base_uri: "s3://my-bucket/test".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:4566".to_string()),
            allow_http: Some(true),
            virtual_hosted_style_request: Some(false),
            unsigned_payload: None,
            auth: cloud_auth::aws::AuthMethod::StaticCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".into(),
                session_token: None,
            },
        };
        assert!(from_storage_type(&storage).is_ok());
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_get_s3_storage_with_retry() {
        crate::crypto::ensure_crypto_provider();
        let storage = StorageType::S3 {
            base_uri: "s3://my-bucket/test".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:4566".to_string()),
            allow_http: Some(true),
            virtual_hosted_style_request: Some(false),
            unsigned_payload: None,
            auth: cloud_auth::aws::AuthMethod::StaticCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".into(),
                session_token: None,
            },
        };
        let retry = valid_retry_options();
        assert!(from_storage_type_with_retry(&storage, Some(&retry)).is_ok());
    }

    #[test]
    fn test_file_config() {
        let json = json!({
            "file": {
                "base_uri": "/tmp/test"
            }
        })
        .to_string();

        let expected = StorageType::File {
            base_uri: "/tmp/test".to_string(),
        };
        test_deserialize(&json, expected);
    }

    /// Scenario: Azure storage config contains only its blob storage URI.
    /// Guarantees: Identity configuration is not required inside the exporter storage block.
    #[test]
    #[cfg(feature = "azure")]
    fn test_azure_config() {
        let json = json!({
            "azure": {
                "base_uri": "https://mystorageaccount.blob.core.windows.net/container"
            }
        })
        .to_string();

        let expected = StorageType::Azure {
            base_uri: "https://mystorageaccount.blob.core.windows.net/container".to_string(),
            endpoint: None,
        };
        test_deserialize(&json, expected);
    }

    /// Scenario: Azure storage config names an explicit blob service endpoint.
    /// Guarantees: The endpoint is kept beside the base URI, so an emulator or
    /// private endpoint can be addressed without changing the account naming.
    #[test]
    #[cfg(feature = "azure")]
    fn test_azure_config_with_endpoint() {
        let json = json!({
            "azure": {
                "base_uri": "https://devstoreaccount1.blob.core.windows.net/container/otel",
                "endpoint": "https://127.0.0.1:10000/devstoreaccount1"
            }
        })
        .to_string();

        let expected = StorageType::Azure {
            base_uri: "https://devstoreaccount1.blob.core.windows.net/container/otel".to_string(),
            endpoint: Some("https://127.0.0.1:10000/devstoreaccount1".to_string()),
        };
        test_deserialize(&json, expected);
    }

    /// Scenario: Azure storage config uses the removed inline auth block.
    /// Guarantees: Legacy credentials are rejected instead of being silently ignored.
    #[test]
    #[cfg(feature = "azure")]
    fn test_azure_config_rejects_inline_auth() {
        let json = json!({
            "azure": {
                "base_uri": "https://mystorageaccount.blob.core.windows.net/container",
                "auth": {
                    "type": "managed_identity"
                }
            }
        })
        .to_string();

        assert!(serde_json::from_str::<StorageType>(&json).is_err());
    }

    /// An S3 storage config for `base_uri` and `endpoint` with the given
    /// `unsigned_payload`.
    #[cfg(feature = "aws")]
    fn s3(base_uri: &str, endpoint: Option<&str>, unsigned_payload: Option<bool>) -> StorageType {
        StorageType::S3 {
            base_uri: base_uri.to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: endpoint.map(str::to_string),
            allow_http: Some(true),
            virtual_hosted_style_request: None,
            unsigned_payload,
            auth: cloud_auth::aws::AuthMethod::Default,
        }
    }

    /// Whether the S3 client `storage` builds over the `AWS_*` settings `env`
    /// signs `UNSIGNED-PAYLOAD`.
    #[cfg(feature = "aws")]
    fn signs_unsigned(storage: &StorageType, env: &[(&str, &str)]) -> bool {
        let base = env.iter().fold(
            object_store::aws::AmazonS3Builder::new(),
            |builder, (key, value)| {
                builder.with_config(key.to_ascii_lowercase().parse().expect("an S3 key"), *value)
            },
        );
        signs_unsigned_payload(&s3_builder(storage, None, base).expect("an S3 builder"))
    }

    /// Scenario: S3 storage configs that set `unsigned_payload` or leave it
    /// unset, against AWS, an HTTPS endpoint and a plain HTTP endpoint.
    /// Guarantees: the option parses, and every payload is signed unless the
    /// option says otherwise, whatever the endpoint.
    #[test]
    #[cfg(feature = "aws")]
    fn unsigned_payload_is_signed_unless_set() {
        let json = json!({
            "s3": {
                "base_uri": "s3://my-bucket/telemetry",
                "unsigned_payload": false,
                "auth": { "type": "default" }
            }
        })
        .to_string();
        let mut expected = s3("s3://my-bucket/telemetry", None, Some(false));
        if let StorageType::S3 {
            region, allow_http, ..
        } = &mut expected
        {
            *region = None;
            *allow_http = None;
        }
        test_deserialize(&json, expected);

        let https = Some("https://s3.eu-west-1.amazonaws.com");
        let http = Some("http://localhost:9000");
        for endpoint in [None, https, http] {
            let signs = |set| signs_unsigned(&s3("s3://b", endpoint, set), &[]);
            assert!(!signs(None));
            assert!(signs(Some(true)));
            assert!(!signs(Some(false)));
        }
    }

    /// Scenario: an S3 storage section with `unsigned_payload` unset or set,
    /// over settings that carry `AWS_UNSIGNED_PAYLOAD`.
    /// Guarantees: the variable decides an unset option in both directions,
    /// and the section's value wins over it.
    #[test]
    #[cfg(feature = "aws")]
    fn unsigned_payload_follows_the_environment_unless_set() {
        let unset = s3("s3://b", None, None);
        assert!(signs_unsigned(&unset, &[("AWS_UNSIGNED_PAYLOAD", "true")]));
        assert!(!signs_unsigned(
            &unset,
            &[("AWS_UNSIGNED_PAYLOAD", "false")]
        ));
        assert!(!signs_unsigned(
            &s3("s3://b", None, Some(false)),
            &[("AWS_UNSIGNED_PAYLOAD", "true")]
        ));
        assert!(signs_unsigned(
            &s3("s3://b", None, Some(true)),
            &[("AWS_UNSIGNED_PAYLOAD", "false")]
        ));
    }

    /// Scenario: `AWS_UNSIGNED_PAYLOAD` spelled in every form object_store's
    /// boolean parser accepts, with the section's option unset.
    /// Guarantees: the reported value matches what the client signs with.
    #[test]
    #[cfg(feature = "aws")]
    fn unsigned_payload_accepts_object_store_boolean_spellings() {
        let unset = s3("s3://b", None, None);
        for on in ["true", "TRUE", "1", "on", "Yes", "y"] {
            assert!(
                signs_unsigned(&unset, &[("AWS_UNSIGNED_PAYLOAD", on)]),
                "{on}"
            );
        }
        for off in ["false", "0", "OFF", "no", "n"] {
            assert!(
                !signs_unsigned(&unset, &[("AWS_UNSIGNED_PAYLOAD", off)]),
                "{off}"
            );
        }
    }

    /// Scenario: the effective `unsigned_payload` of file storage and of S3
    /// sections that set it.
    /// Guarantees: file storage has none; an S3 section's explicit value is
    /// what its client uses.
    #[test]
    #[cfg(feature = "aws")]
    fn s3_unsigned_payload_reports_the_resolved_value() {
        let file = StorageType::File {
            base_uri: "/tmp/x".into(),
        };
        assert_eq!(s3_unsigned_payload(&file), None);
        for set in [false, true] {
            assert_eq!(
                s3_unsigned_payload(&s3("s3://b", None, Some(set))),
                Some(set)
            );
        }
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_s3_config_with_default_auth() {
        let json = json!({
            "s3": {
                "base_uri": "s3://my-bucket/telemetry",
                "auth": {
                    "type": "default"
                }
            }
        })
        .to_string();

        let expected = StorageType::S3 {
            base_uri: "s3://my-bucket/telemetry".to_string(),
            region: None,
            endpoint: None,
            allow_http: None,
            virtual_hosted_style_request: None,
            unsigned_payload: None,
            auth: cloud_auth::aws::AuthMethod::Default,
        };
        test_deserialize(&json, expected);
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_s3_config_with_static_credentials() {
        let json = json!({
            "s3": {
                "base_uri": "s3://my-bucket/telemetry",
                "region": "us-east-1",
                "endpoint": "http://localhost:4566",
                "allow_http": true,
                "virtual_hosted_style_request": false,
                "auth": {
                    "type": "static_credentials",
                    "access_key_id": "test",
                    "secret_access_key": "test",
                    "session_token": "token"
                }
            }
        })
        .to_string();

        let expected = StorageType::S3 {
            base_uri: "s3://my-bucket/telemetry".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: Some("http://localhost:4566".to_string()),
            allow_http: Some(true),
            virtual_hosted_style_request: Some(false),
            unsigned_payload: None,
            auth: cloud_auth::aws::AuthMethod::StaticCredentials {
                access_key_id: "test".to_string(),
                secret_access_key: "test".into(),
                session_token: Some("token".into()),
            },
        };
        test_deserialize(&json, expected);
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_s3_config_with_web_identity() {
        let json = json!({
            "s3": {
                "base_uri": "s3://my-bucket/telemetry",
                "region": "us-east-1",
                "auth": {
                    "type": "web_identity",
                    "role_arn": "arn:aws:iam::123456789012:role/TestRole",
                    "token_file_path": "/var/run/secrets/token"
                }
            }
        })
        .to_string();

        let expected = StorageType::S3 {
            base_uri: "s3://my-bucket/telemetry".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: None,
            allow_http: None,
            virtual_hosted_style_request: None,
            unsigned_payload: None,
            auth: cloud_auth::aws::AuthMethod::WebIdentity {
                role_arn: Some("arn:aws:iam::123456789012:role/TestRole".to_string()),
                token_file_path: Some("/var/run/secrets/token".to_string()),
            },
        };
        test_deserialize(&json, expected);
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_s3_config_with_assume_role() {
        let json = json!({
            "s3": {
                "base_uri": "s3://my-bucket/telemetry",
                "region": "us-east-1",
                "auth": {
                    "type": "assume_role",
                    "role_arn": "arn:aws:iam::123456789012:role/CrossAccountRole",
                    "external_id": "my-external-id",
                    "session_name": "otap-session"
                }
            }
        })
        .to_string();

        let expected = StorageType::S3 {
            base_uri: "s3://my-bucket/telemetry".to_string(),
            region: Some("us-east-1".to_string()),
            endpoint: None,
            allow_http: None,
            virtual_hosted_style_request: None,
            unsigned_payload: None,
            auth: cloud_auth::aws::AuthMethod::AssumeRole {
                role_arn: "arn:aws:iam::123456789012:role/CrossAccountRole".to_string(),
                external_id: Some("my-external-id".to_string()),
                session_name: Some("otap-session".to_string()),
            },
        };
        test_deserialize(&json, expected);
    }

    // --- extract_path_prefix tests ---

    #[test]
    #[cfg(feature = "aws")]
    fn test_extract_prefix_s3_with_prefix() {
        let prefix = extract_path_prefix("s3://my-bucket/telemetry").unwrap();
        assert_eq!(prefix, Some(Path::from("telemetry")));
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_extract_prefix_s3_nested_prefix() {
        let prefix = extract_path_prefix("s3://my-bucket/a/b/c").unwrap();
        assert_eq!(prefix, Some(Path::from("a/b/c")));
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_extract_prefix_s3_no_prefix() {
        let prefix = extract_path_prefix("s3://my-bucket").unwrap();
        assert_eq!(prefix, None);
    }

    #[test]
    #[cfg(feature = "aws")]
    fn test_extract_prefix_s3_trailing_slash() {
        let prefix = extract_path_prefix("s3://my-bucket/telemetry/").unwrap();
        assert_eq!(prefix, Some(Path::from("telemetry")));
    }

    #[test]
    #[cfg(feature = "azure")]
    fn test_extract_prefix_az_with_prefix() {
        let prefix = extract_path_prefix("az://container/prefix").unwrap();
        assert_eq!(prefix, Some(Path::from("prefix")));
    }

    #[test]
    #[cfg(feature = "azure")]
    fn test_extract_prefix_az_no_prefix() {
        let prefix = extract_path_prefix("az://container").unwrap();
        assert_eq!(prefix, None);
    }

    #[test]
    #[cfg(feature = "azure")]
    fn test_extract_prefix_azure_https_with_prefix() {
        let prefix =
            extract_path_prefix("https://account.blob.core.windows.net/container/prefix").unwrap();
        assert_eq!(prefix, Some(Path::from("prefix")));
    }

    #[test]
    #[cfg(feature = "azure")]
    fn test_extract_prefix_azure_https_nested_prefix() {
        let prefix =
            extract_path_prefix("https://account.blob.core.windows.net/container/a/b/c").unwrap();
        assert_eq!(prefix, Some(Path::from("a/b/c")));
    }

    #[test]
    #[cfg(feature = "azure")]
    fn test_extract_prefix_azure_https_no_prefix() {
        let prefix =
            extract_path_prefix("https://account.blob.core.windows.net/container").unwrap();
        assert_eq!(prefix, None);
    }

    #[test]
    #[cfg(feature = "azure")]
    fn test_extract_prefix_azure_https_container_trailing_slash() {
        let prefix =
            extract_path_prefix("https://account.blob.core.windows.net/container/").unwrap();
        assert_eq!(prefix, None);
    }

    #[cfg(test)]
    fn test_deserialize(json: &str, expected: StorageType) {
        let deserialized: StorageType =
            serde_json::from_str(json).expect("Failed to deserialize Config");
        assert_eq!(deserialized, expected);
    }
}
