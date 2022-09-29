use drogue_doppelgaenger_model::SyntheticType;
use indexmap::IndexMap;
use serde::de::{Error, MapAccess};
use serde::{de, Deserialize, Deserializer};
use std::fmt::Formatter;
use std::fs;
use std::fs::File;
use std::path::Path;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThingTemplate {
    #[serde(default, skip_serializing_if = "Reconciliation::is_empty")]
    pub reconciliation: Reconciliation,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub synthetics: IndexMap<String, Synthetic>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Synthetic {
    JavaScript(Source),
    Alias(String),
}

impl From<Synthetic> for SyntheticType {
    fn from(syn: Synthetic) -> Self {
        match syn {
            Synthetic::JavaScript(source) => Self::JavaScript(source.0),
            Synthetic::Alias(alias) => Self::Alias(alias),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase", transparent)]
pub struct Source(String);

impl<'de> Deserialize<'de> for Source {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StringOrStruct;

        #[derive(serde::Deserialize)]
        struct File {
            path: String,
        }

        impl<'de> de::Visitor<'de> for StringOrStruct {
            type Value = Source;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                write!(
                    formatter,
                    "Expected either string content, or an object with a path field"
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(Source(v.to_string()))
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let file: File =
                    Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(Source(fs::read_to_string(&file.path).map_err(|e| {
                    Error::custom(format!(
                        "failed to load content from external source ({}): {e}",
                        file.path
                    ))
                })?))
            }
        }

        deserializer.deserialize_any(StringOrStruct)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum Code {
    JavaScript(Source),
}

impl From<Code> for drogue_doppelgaenger_model::Code {
    fn from(code: Code) -> Self {
        match code {
            Code::JavaScript(source) => Self::JavaScript(source.0),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Timer {
    pub code: Code,
    #[serde(with = "humantime_serde")]
    pub period: Duration,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Reconciliation {
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub changed: IndexMap<String, Code>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub deleting: IndexMap<String, Code>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub timers: IndexMap<String, Timer>,
}

impl Reconciliation {
    pub fn is_empty(&self) -> bool {
        self.changed.is_empty() && self.deleting.is_empty()
    }
}

pub fn load<P: AsRef<Path>>(path: P) -> anyhow::Result<ThingTemplate> {
    Ok(serde_yaml::from_reader(File::open(path)?)?)
}
