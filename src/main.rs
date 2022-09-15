use drogue_bazaar::runtime;
use twin_operator::run;

drogue_bazaar::project!("Drogue Doppelgänger");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    runtime!(PROJECT).exec(run).await
}
