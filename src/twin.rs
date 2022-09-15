use crate::{
    client::{TwinClient, TwinClientBuilder},
    reconciler::{Outcome, Reconciler},
};
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::Utc;
use drogue_bazaar::auth::openid::TokenConfig;
use drogue_client::{
    error::ClientError,
    meta::v1::CommonMetadataMut,
    registry::{self, v1::Device},
};
use drogue_doppelgaenger_model::{Changed, Code, Deleting, SyntheticFeature, SyntheticType, Thing};
use hyper::StatusCode;
use serde_json::Value;
use std::collections::HashMap;
use url::Url;

const FINALIZER: &str = "twin";

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ClientConfig {
    pub url: Url,
    #[serde(flatten)]
    pub token: TokenConfig,
    #[serde(flatten)]
    pub client: drogue_bazaar::core::tls::ClientConfig,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct TwinConfig {
    pub client: ClientConfig,
    #[serde(default)]
    pub reconciler: ReconcilerConfig,
}

#[derive(Clone, Debug, Default, serde::Deserialize)]
pub struct ReconcilerConfig {
    pub application: String,
    #[serde(default)]
    pub label_selector: HashMap<String, String>,
}

pub struct TwinReconciler {
    client: TwinClient,
    config: ReconcilerConfig,
    registry: registry::v1::Client,
}

impl TwinReconciler {
    pub async fn new(config: TwinConfig, registry: registry::v1::Client) -> anyhow::Result<Self> {
        let TwinConfig {
            client,
            reconciler: config,
        } = config;
        let client = TwinClientBuilder::from_url(client.url.clone())
            .client(client.client.clone())
            .token_provider(client.token)
            .await?
            .build()?;
        log::info!("Twin client: {client:?}");
        Ok(Self {
            config,
            client,
            registry,
        })
    }
}

#[async_trait]
impl Reconciler for TwinReconciler {
    async fn changed(&self, device: &Device) -> anyhow::Result<Outcome> {
        if !self.matches(&device) {
            log::debug!("Device doesn't match selector");
            return self.removing(device).await;
        }
        if device.metadata.deletion_timestamp.is_some() {
            log::debug!("Device is soft-deleted");
            return self.removing(device).await;
        }
        self.ensure(&device).await
    }

    async fn missing(&self, device: &str) -> anyhow::Result<Outcome> {
        log::info!("Deleting twin device: {}", device);

        let thing = Self::sensor_thing(device);

        // ensure the device is deleted in the twin state
        match self
            .client
            .delete_thing(&self.config.application, &thing)
            .await
        {
            Ok(_) | Err(ClientError::Response(StatusCode::NOT_FOUND)) => Ok(Outcome::Complete),
            Err(err) => Err(anyhow!(err)),
        }
    }
}

impl TwinReconciler {
    fn matches(&self, device: &Device) -> bool {
        for (k, v) in &self.config.label_selector {
            match device.metadata.labels.get(k) {
                Some(l) if l == v => {}
                _ => {
                    return false;
                }
            }
        }

        true
    }

    fn sensor_thing(device: &str) -> String {
        format!("{}/sensor", device)
    }

    /// Ensure that the device is provisioned
    async fn ensure(&self, device: &Device) -> anyhow::Result<Outcome> {
        log::info!("Ensuring twin device: {}", device.metadata.name);

        // ensure that the finalizer is set

        let mut device = device.clone();

        if device.metadata.ensure_finalizer(FINALIZER) {
            return match self.registry.update_device(&device).await {
                Ok(_) => Ok(Outcome::Retry),
                Err(ClientError::Response(StatusCode::CONFLICT)) => Ok(Outcome::Retry),
                Err(err) => Err(anyhow!(err).context("add finalizer")),
            };
        }

        // ensure sensor thing
        if let Outcome::Retry = self.ensure_sensor(&mut device).await? {
            // retry now
            return Ok(Outcome::Retry);
        }

        // ensure device thing
        self.ensure_device(&mut device).await
    }

    async fn ensure_device(&self, device: &Device) -> anyhow::Result<Outcome> {
        let thing = self
            .client
            .get_thing(&self.config.application, &device.metadata.name)
            .await?;

        match thing {
            // not created yet, retry
            // FIXME: possibly delay
            None => Ok(Outcome::Retry),
            Some(mut thing) => {
                thing.metadata.annotations.insert(
                    "io.drogue/group".to_string(),
                    "btmesh/eclipsecon2022".to_string(),
                );

                match self.client.update_thing(thing).await {
                    Ok(_) => Ok(Outcome::Complete),
                    Err(ClientError::Response(StatusCode::NOT_FOUND | StatusCode::CONFLICT)) => {
                        Ok(Outcome::Retry)
                    }
                    Err(err) => Err(anyhow!(err).context("failed to update device thing")),
                }
            }
        }
    }

    async fn ensure_sensor(&self, device: &Device) -> anyhow::Result<Outcome> {
        let thing = Self::sensor_thing(&device.metadata.name);
        let thing = self
            .client
            .get_thing(&self.config.application, &thing)
            .await?;

        match thing {
            Some(mut thing) => {
                self.configure_sensor(&mut thing);
                match self.client.update_thing(thing).await {
                    Ok(_) => Ok(Outcome::Complete),
                    Err(ClientError::Response(StatusCode::CONFLICT | StatusCode::NOT_FOUND)) => {
                        Ok(Outcome::Retry)
                    }
                    Err(err) => Err(anyhow!(err)),
                }
            }
            None => {
                let mut thing = Thing::new(
                    &self.config.application,
                    Self::sensor_thing(&device.metadata.name),
                );
                self.configure_sensor(&mut thing);

                match self.client.create_thing(thing).await {
                    Ok(_) => Ok(Outcome::Complete),
                    Err(ClientError::Response(StatusCode::CONFLICT)) => Ok(Outcome::Retry),
                    Err(err) => Err(anyhow!(err)),
                }
            }
        }
    }

    /// Remove the device, and remove the finalizer
    async fn removing(&self, device: &Device) -> anyhow::Result<Outcome> {
        // handle the device as missing (which deletes it in the twin state)
        self.missing(&device.metadata.name).await?;

        // now remove the finalizer
        let mut device = device.clone();
        device.metadata.remove_finalizer(FINALIZER);
        match self.registry.update_device(&device).await {
            Ok(_) | Err(ClientError::Response(StatusCode::NOT_FOUND)) => {}
            Err(err) => return Err(anyhow!(err).context("remove finalizer")),
        }

        Ok(Outcome::Complete)
    }

    fn configure_sensor(&self, thing: &mut Thing) {
        thing.reconciliation.changed.insert(
            "hierarchy".to_string(),
            Changed::from(Code::JavaScript(
                include_str!("../js/hierarchy.js").to_string(),
            )),
        );

        thing.reconciliation.deleting.insert(
            "hierarchy".to_string(),
            Deleting {
                code: Code::JavaScript(include_str!("../js/hierarchy.js").to_string()),
            },
        );

        syn(
            thing,
            "acceleration",
            include_str!("../js/syn_acceleration.js"),
        );
        syn(
            thing,
            "batteryLevel",
            include_str!("../js/syn_batteryLevel.js"),
        );
        syn(thing, "noise", include_str!("../js/syn_noise.js"));
        syn(
            thing,
            "temperature",
            include_str!("../js/syn_temperature.js"),
        );
    }
}

fn syn(thing: &mut Thing, name: &str, code: &str) {
    thing.synthetic_state.insert(
        name.to_string(),
        SyntheticFeature {
            r#type: SyntheticType::JavaScript(code.to_string()),
            value: Value::Null,
            last_update: Utc::now(),
        },
    );
}
