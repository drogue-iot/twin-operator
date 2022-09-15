use crate::reconciler::{Outcome, Reconciler};
use cloudevents::{AttributesReader, Event};
use drogue_client::registry::v1::Device;
use futures::stream::StreamExt;
use paho_mqtt as mqtt;
use tokio::time::MissedTickBehavior;
use tokio::{join, time::Duration};

pub type DrogueClient = drogue_client::registry::v1::Client;

pub struct Operator<R>
where
    R: Reconciler,
{
    reconciler: R,
    client: mqtt::AsyncClient,
    group_id: Option<String>,
    application: String,
    registry: DrogueClient,
    interval: Duration,
}

impl<R> Operator<R>
where
    R: Reconciler,
{
    pub fn new(
        reconciler: R,
        client: mqtt::AsyncClient,
        group_id: Option<String>,
        application: String,
        registry: DrogueClient,
        interval: Duration,
    ) -> Self {
        Self {
            reconciler,
            client,
            group_id,
            application,
            registry,
            interval,
        }
    }

    pub async fn provision_devices(&self, devices: Vec<Device>) -> anyhow::Result<()> {
        for device in devices {
            self.handle_changed_device(&device).await?;
        }

        Ok(())
    }

    pub async fn reconcile_devices(&self) {
        log::info!("Reconciling devices with interval {:?}", self.interval);
        let mut interval = tokio::time::interval(self.interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            interval.tick().await;

            let devices = self
                .registry
                .list_devices(&self.application, None)
                .await
                .unwrap_or(None)
                .unwrap_or(Vec::new());

            self.provision_devices(devices)
                .await
                .expect("Periodic reconcile failed");
        }
    }

    pub async fn run(&mut self) -> Result<(), anyhow::Error> {
        if let Some(group_id) = &self.group_id {
            self.client.subscribe(
                format!("$shared/{}/app/{}", &group_id, &self.application),
                1,
            );
        } else {
            self.client
                .subscribe(format!("app/{}", &self.application), 1);
        }

        let stream = self.client.get_stream(100);
        join!(self.reconcile_devices(), self.process_events(stream));
        Ok(())
    }

    pub async fn process_events(
        &self,
        mut stream: paho_mqtt::AsyncReceiver<Option<mqtt::Message>>,
    ) {
        log::info!("Processing events events");
        loop {
            if let Some(m) = stream.next().await {
                if let Some(m) = m {
                    match serde_json::from_slice::<Event>(m.payload()) {
                        Ok(e) => {
                            self.handle_event(e).await.expect("Processing failed");
                        }
                        Err(e) => {
                            log::warn!("Error parsing event: {:?}", e);
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn handle_missing_device(&self, device: &str) -> anyhow::Result<Outcome> {
        log::info!("Handle missing device: {device}");
        self.reconciler.missing(device).await
    }

    async fn handle_changed_device(&self, device: &Device) -> anyhow::Result<Outcome> {
        log::info!("Handle changed device: {}", device.metadata.name);
        self.reconciler.changed(device).await
    }

    async fn handle_event(&self, event: Event) -> anyhow::Result<()> {
        const REGISTRY_TYPE: &str = "io.drogue.registry.v1";

        if event.ty() != REGISTRY_TYPE {
            return Ok(());
        }

        let device = event.extension("device").map(|e| e.to_string());
        log::info!("Device: {device:?}");

        let device = if let Some(device) = device {
            device
        } else {
            // missing information, skipping event
            return Ok(());
        };

        loop {
            let outcome =
                if let Some(device) = self.registry.get_device(&self.application, &device).await? {
                    self.handle_changed_device(&device).await?
                } else {
                    self.handle_missing_device(&device).await?
                };

            match outcome {
                Outcome::Complete => {
                    log::info!("Reconciled device");
                    break;
                }
                Outcome::Retry => {
                    log::info!("Need to retry device");
                    continue;
                }
            }
        }

        Ok(())
    }
}
