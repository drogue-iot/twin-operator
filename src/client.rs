use async_trait::async_trait;
use drogue_bazaar::auth::openid::TokenConfig;
use drogue_bazaar::{core::tls::ClientConfig, reqwest::ClientFactory};
use drogue_client::core::PropagateCurrentContext;
use drogue_client::error::{ClientError, ErrorInformation};
use drogue_client::openid::{
    AccessTokenProvider, NoTokenProvider, OpenIdTokenProvider, TokenInjector, TokenProvider,
};
use drogue_doppelgaenger_model::Thing;
use reqwest::{IntoUrl, Method, RequestBuilder, Response, StatusCode};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tracing::instrument;
use url::Url;

#[async_trait]
pub trait IntoTokenProvider {
    type Error;
    async fn into_token_provider(self) -> Result<Arc<dyn TokenProvider>, Self::Error>;
}

#[async_trait]
impl IntoTokenProvider for AccessTokenProvider {
    type Error = Infallible;
    async fn into_token_provider(self) -> Result<Arc<dyn TokenProvider>, Self::Error> {
        Ok(Arc::new(self))
    }
}

#[async_trait]
impl IntoTokenProvider for OpenIdTokenProvider {
    type Error = Infallible;
    async fn into_token_provider(self) -> Result<Arc<dyn TokenProvider>, Self::Error> {
        Ok(Arc::new(self))
    }
}

#[async_trait]
impl IntoTokenProvider for TokenConfig {
    type Error = anyhow::Error;
    async fn into_token_provider(self) -> Result<Arc<dyn TokenProvider>, Self::Error> {
        Ok(Arc::new(self.discover_from().await?))
    }
}

#[derive(Clone, Debug)]
pub struct TwinClientBuilder {
    api: Url,
    token_provider: Option<Arc<dyn TokenProvider>>,
    client: ClientFactory,
}

impl TwinClientBuilder {
    #[allow(unused)]
    pub fn new<U>(api: U) -> Result<Self, reqwest::Error>
    where
        U: IntoUrl,
    {
        Ok(Self::from_url(api.into_url()?))
    }

    pub fn from_url(api: Url) -> Self {
        Self {
            api,
            token_provider: None,
            client: ClientFactory::new(),
        }
    }

    pub fn client(mut self, config: ClientConfig) -> Self {
        self.client = ClientFactory::from(config);
        self
    }

    pub async fn token_provider<TP>(mut self, token_provider: TP) -> Result<Self, TP::Error>
    where
        TP: IntoTokenProvider,
    {
        let token_provider = token_provider.into_token_provider().await?;
        log::info!(
            "Testing access token: {:?}",
            token_provider.provide_access_token().await
        );
        self.token_provider = Some(token_provider);
        Ok(self)
    }

    pub fn build(self) -> anyhow::Result<TwinClient> {
        let client = self.client.build()?;
        Ok(TwinClient::new(
            client,
            self.api,
            self.token_provider
                .unwrap_or_else(|| Arc::new(NoTokenProvider)),
        ))
    }
}

#[derive(Clone, Debug)]
pub struct TwinClient {
    client: reqwest::Client,
    api: Url,
    token_provider: Arc<dyn TokenProvider>,
}

impl TwinClient {
    pub fn new(client: reqwest::Client, api: Url, token_provider: Arc<dyn TokenProvider>) -> Self {
        Self {
            client,
            api,
            token_provider,
        }
    }

    async fn request<R, F, FR, ResFut>(
        &self,
        method: Method,
        url: Url,
        request_handler: F,
        response_handler: FR,
    ) -> Result<R, ClientError>
    where
        F: FnOnce(RequestBuilder) -> RequestBuilder,
        FR: FnOnce(Response) -> ResFut,
        ResFut: Future<Output = Result<R, ClientError>>,
    {
        let request = self
            .client
            .request(method, url)
            .propagate_current_context()
            .inject_token(self.token_provider.as_ref())
            .await?;

        let request = request_handler(request);
        let response = request.send().await?;

        response_handler(response).await
    }

    fn url(&self, path: &[&str]) -> Result<Url, ClientError> {
        let mut url = self.api.clone();
        url.path_segments_mut()
            .map_err(|()| ClientError::Request("Failed to build path".to_string()))?
            .extend(path);
        Ok(url)
    }

    async fn get<R>(&self, path: &[&str]) -> Result<Option<R>, ClientError>
    where
        R: DeserializeOwned,
    {
        self.request(Method::GET, self.url(path)?, empty, read_response)
            .await
    }

    #[instrument(
        skip_all, err,
        fields(application=application.as_ref(), name=thing.as_ref())
    )]
    pub async fn get_thing<A: AsRef<str>, T: AsRef<str>>(
        &self,
        application: A,
        thing: T,
    ) -> Result<Option<Thing>, ClientError> {
        self.get(&[
            "api",
            "v1alpha1",
            "things",
            application.as_ref(),
            "things",
            thing.as_ref(),
        ])
        .await
    }

    #[instrument(
        skip_all, ret, err,
        fields(application=thing.metadata.application, name=thing.metadata.name)
    )]
    pub async fn create_thing(&self, thing: Thing) -> Result<(), ClientError> {
        self.request(
            Method::POST,
            self.url(&["api", "v1alpha1", "things"])?,
            json(thing),
            create_response::<Thing>,
        )
        .await
        .map(|_| ())
    }

    #[instrument(
        skip_all, ret, err,
        fields(application=thing.metadata.application, name=thing.metadata.name)
    )]
    pub async fn update_thing(&self, thing: Thing) -> Result<(), ClientError> {
        self.request(
            Method::PUT,
            self.url(&["api", "v1alpha1", "things"])?,
            json(thing),
            update_response::<Thing>,
        )
        .await
        .map(|_| ())
    }

    #[instrument(
        skip_all, ret, err,
        fields(application=application.as_ref(), name=thing.as_ref())
    )]
    pub async fn delete_thing<A: AsRef<str>, T: AsRef<str>>(
        &self,
        application: A,
        thing: T,
    ) -> Result<bool, ClientError> {
        self.request(
            Method::DELETE,
            self.url(&[
                "api",
                "v1alpha1",
                "things",
                application.as_ref(),
                "things",
                thing.as_ref(),
            ])?,
            empty,
            delete_response,
        )
        .await
    }
}

#[inline]
fn empty(request: RequestBuilder) -> RequestBuilder {
    request
}

fn json<S: Serialize>(payload: S) -> impl FnOnce(RequestBuilder) -> RequestBuilder {
    move |r| r.json(&payload)
}

async fn create_response<T: DeserializeOwned>(
    response: Response,
) -> Result<Option<T>, ClientError> {
    log::debug!("Eval create response: {:#?}", response);
    match response.status() {
        StatusCode::NO_CONTENT | StatusCode::CREATED | StatusCode::ACCEPTED => Ok(None),
        // the token API responds 200 on token creations, sending back the content.
        StatusCode::OK => Ok(Some(response.json().await?)),
        _ => default_response(response).await,
    }
}

async fn read_response<T: DeserializeOwned>(response: Response) -> Result<Option<T>, ClientError> {
    log::debug!("Eval get response: {:#?}", response);
    match response.status() {
        StatusCode::OK => Ok(Some(response.json().await?)),
        StatusCode::NOT_FOUND => Ok(None),
        _ => default_response(response).await,
    }
}

async fn delete_response(response: Response) -> Result<bool, ClientError> {
    log::debug!("Eval delete response: {:#?}", response);
    match response.status() {
        StatusCode::OK | StatusCode::NO_CONTENT => Ok(true),
        StatusCode::NOT_FOUND => Ok(false),
        _ => default_response(response).await,
    }
}

async fn update_response<T: DeserializeOwned>(
    response: Response,
) -> Result<Option<T>, ClientError> {
    log::debug!("Eval update response: {:#?}", response);
    match response.status() {
        StatusCode::NO_CONTENT | StatusCode::ACCEPTED => Ok(None),
        StatusCode::OK => Ok(Some(response.json().await?)),
        _ => default_response(response).await,
    }
}

async fn default_response<T>(response: Response) -> Result<T, ClientError> {
    let code = response.status();
    match response.json::<ErrorInformation>().await {
        Ok(info) => Err(ClientError::Service { code, error: info }),
        Err(_) => Err(ClientError::Response(code)),
    }
}
