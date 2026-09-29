// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//! An object store that runs hooks around the two calls a Parquet write
//! makes, a put and a multipart creation, and delegates everything else.

use std::fmt;
use std::sync::Arc;

use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

/// A value a hook keeps alive until the store call it ran before has
/// returned or been dropped.
pub type HookGuard = Box<dyn Send>;

/// What a [`HookStore`] runs around its inner store's write calls.
///
/// Every hook does nothing by default.
#[async_trait::async_trait]
pub trait StoreHooks: fmt::Debug + Send + Sync + 'static {
    /// Runs before a put reaches the inner store; an error fails the put.
    async fn before_put(
        &self,
        _location: &Path,
        _payload: &PutPayload,
    ) -> object_store::Result<Option<HookGuard>> {
        Ok(None)
    }

    /// Runs before a multipart upload is created; an error fails the
    /// creation.
    async fn before_multipart(&self, _location: &Path) -> object_store::Result<Option<HookGuard>> {
        Ok(None)
    }

    /// Runs when the inner store fails to create a multipart upload.
    fn multipart_failed(&self, _location: &Path, _error: &object_store::Error) {}

    /// The upload the caller receives in place of the one the inner store
    /// created.
    fn wrap_upload(
        &self,
        _location: &Path,
        upload: Box<dyn MultipartUpload>,
    ) -> Box<dyn MultipartUpload> {
        upload
    }
}

/// An object store running `hooks` around `inner`'s put and multipart
/// creation.
#[derive(Debug)]
pub struct HookStore<H> {
    inner: Arc<dyn ObjectStore>,
    hooks: H,
}

impl<H> HookStore<H> {
    /// `inner`, with `hooks` run around its write calls.
    pub fn new(inner: Arc<dyn ObjectStore>, hooks: H) -> Self {
        Self { inner, hooks }
    }

    /// The hooks.
    pub fn hooks(&self) -> &H {
        &self.hooks
    }

    /// The store the calls are delegated to.
    pub fn inner(&self) -> &Arc<dyn ObjectStore> {
        &self.inner
    }
}

impl<H> fmt::Display for HookStore<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.inner.fmt(f)
    }
}

#[async_trait::async_trait]
impl<H: StoreHooks> ObjectStore for HookStore<H> {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _guard = self.hooks.before_put(location, &payload).await?;
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let _guard = self.hooks.before_multipart(location).await?;
        let upload = self
            .inner
            .put_multipart_opts(location, options)
            .await
            .inspect_err(|error| self.hooks.multipart_failed(location, error))?;
        Ok(self.hooks.wrap_upload(location, upload))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}
