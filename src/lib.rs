mod client;
mod operator;
mod reconciler;
mod twin;

pub use operator::*;

use crate::twin::{TwinConfig, TwinReconciler};
use anyhow::Context;
use drogue_bazaar::app::{Startup, StartupExt};
use drogue_client::openid::AccessTokenProvider;
use paho_mqtt as mqtt;
use std::time::Duration;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct Config {
    operator: OperatorConfig,
    twin: TwinConfig,
}

#[derive(Clone, Debug, serde::Deserialize)]
pub struct OperatorConfig {
    /// Mqtt server uri (tcp://host:port)
    mqtt_uri: String,

    /// Mqtt group id for shared subscription (for horizontal scaling)
    #[serde(default)]
    mqtt_group_id: Option<String>,

    /// API URL
    api: String,

    /// Name of specific application to manage firmware updates for (will use all accessible from service account by default)
    application: String,

    /// Token for authenticating to Drogue IoT
    token: String,

    /// User for authenticating to Drogue IoT
    user: String,

    /// Path to CA
    ca_path: Option<String>,

    /// Disable TLS
    #[serde(default)]
    disable_tls: bool,

    /// Ignore cert validation
    #[serde(default)]
    insecure_tls: bool,

    /// Interval reconciling devices
    #[serde(default, with = "humantime_serde")]
    interval: Option<Duration>,
}

pub async fn run(config: Config, startup: &mut dyn Startup) -> anyhow::Result<()> {
    log::info!("Config: {config:#?}");

    let twin_config = config.twin;
    let config = config.operator;

    let mqtt_uri = config.mqtt_uri;

    let mqtt_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(mqtt_uri)
        .client_id("twin-operator")
        .persistence(mqtt::PersistenceType::None)
        .finalize();
    let mqtt_client = mqtt::AsyncClient::new(mqtt_opts)?;

    let tp = AccessTokenProvider {
        user: config.user.clone(),
        token: config.token.clone(),
    };

    let url = reqwest::Url::parse(&config.api)?;
    let drg = DrogueClient::new(reqwest::Client::new(), url, tp);

    let mut conn_opts = mqtt::ConnectOptionsBuilder::new();
    conn_opts.user_name(config.user);
    conn_opts.password(config.token);
    conn_opts.keep_alive_interval(Duration::from_secs(30));
    conn_opts.automatic_reconnect(Duration::from_millis(100), Duration::from_secs(5));

    if !config.disable_tls {
        let ca = config
            .ca_path
            .unwrap_or("/etc/ssl/certs/ca-bundle.crt".to_string());
        let ssl_opts = if config.insecure_tls {
            mqtt::SslOptionsBuilder::new()
                .trust_store(&ca)?
                .enable_server_cert_auth(false)
                .verify(false)
                .finalize()
        } else {
            mqtt::SslOptionsBuilder::new().trust_store(&ca)?.finalize()
        };
        conn_opts.ssl_options(ssl_opts);
    }

    let conn_opts = conn_opts.finalize();

    mqtt_client.set_disconnected_callback(|c, _, _| {
        log::info!("Disconnected");
        let t = c.reconnect();
        if let Err(e) = t.wait_for(Duration::from_secs(10)) {
            log::warn!("Error reconnecting to broker ({:?}), exiting", e);
            std::process::exit(1);
        }
    });

    mqtt_client.set_connection_lost_callback(|c| {
        log::info!("Connection lost");
        let t = c.reconnect();
        if let Err(e) = t.wait_for(Duration::from_secs(10)) {
            log::warn!("Error reconnecting to broker ({:?}), exiting", e);
            std::process::exit(1);
        }
    });

    mqtt_client
        .connect(conn_opts)
        .await
        .context("Failed to connect to MQTT endpoint")?;

    log::info!("Starting server");

    let mut app = Operator::new(
        TwinReconciler::new(twin_config, drg.clone()).await?,
        mqtt_client,
        config.mqtt_group_id,
        config.application,
        drg,
        config.interval.unwrap_or(Duration::from_secs(60)),
    );

    startup.spawn(async move { app.run().await });

    Ok(())
}
