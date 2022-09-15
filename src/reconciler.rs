use async_trait::async_trait;
use drogue_client::registry::v1::Device;

pub enum Outcome {
    Complete,
    Retry,
}

#[async_trait]
pub trait Reconciler {
    async fn changed(&self, device: &Device) -> anyhow::Result<Outcome>;
    async fn missing(&self, device: &str) -> anyhow::Result<Outcome>;
}
