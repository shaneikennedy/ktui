use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
use rdkafka::config::ClientConfig;
use rdkafka::{
    consumer::{Consumer, StreamConsumer},
    error::KafkaError,
    message::Message,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

pub struct KafkaClient {
    admin_client: AdminClient<rdkafka::client::DefaultClientContext>,
    bootstrap_servers: String,
    active_consumers: Arc<Mutex<HashMap<String, Arc<StreamConsumer>>>>,
}

impl KafkaClient {
    pub fn new(bootstrap_servers: &str) -> Result<Self> {
        let admin_client: AdminClient<_> = ClientConfig::new()
            .set("bootstrap.servers", bootstrap_servers)
            .set("security.protocol", "PLAINTEXT")
            .set("log.connection.close", "false")
            .set_log_level(rdkafka::config::RDKafkaLogLevel::Emerg)
            .create()?;

        Ok(Self {
            admin_client,
            bootstrap_servers: bootstrap_servers.to_string(),
            active_consumers: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn list_topics(&self) -> Result<Vec<String>> {
        let metadata = self
            .admin_client
            .inner()
            .fetch_metadata(None, Duration::from_secs(5)).context("Problem fetching metadata")?;
        let topics: Vec<String> = metadata
            .topics()
            .iter()
            .map(|topic| topic.name().to_string())
            .collect();
        Ok(topics)
    }

    pub async fn get_topic_config(&self, topic: &str) -> Result<HashMap<String, String>> {
        let configs = self
            .admin_client
            .describe_configs(&[ResourceSpecifier::Topic(topic)], &AdminOptions::new())
            .await?;

        let mut result = HashMap::new();
        if let Some(Ok(config)) = configs.first() {
            for entry in &config.entries {
                if let Some(value) = &entry.value {
                    result.insert(entry.name.to_string(), value.to_string());
                }
            }
        }
        Ok(result)
    }

    pub async fn stop_consumer(&self, topic: &str) {
        let mut consumers = self.active_consumers.lock().await;
        if let Some(consumer) = consumers.remove(topic) {
            consumer.unsubscribe();
        }
    }

    pub async fn stop_all_consumers(&self) {
        let mut consumers = self.active_consumers.lock().await;
        for (_, consumer) in consumers.drain() {
            consumer.unsubscribe();
        }
    }

    pub async fn consume_topic_messages(
        &self,
        topic: &str,
        tx: mpsc::Sender<String>,
    ) -> Result<(), KafkaError> {
        self.stop_consumer(topic).await;

        let mut consumer_config = ClientConfig::new();
        consumer_config.set_log_level(rdkafka::config::RDKafkaLogLevel::Emerg);
        consumer_config
            .set("group.id", format!("ktui-consumer-{}", topic))
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("log.connection.close", "false");

        let consumer: StreamConsumer = consumer_config.create()?;

        consumer.subscribe(&[topic])?;

        let consumer_arc = Arc::new(consumer);
        let mut consumers = self.active_consumers.lock().await;
        consumers.insert(topic.to_string(), Arc::clone(&consumer_arc));

        let consumer_clone = Arc::clone(&consumer_arc);

        tokio::spawn(async move {
            loop {
                match consumer_clone.recv().await {
                    Ok(message) => {
                        if let Some(payload) = message.payload() {
                            if let Ok(text) = String::from_utf8(payload.to_vec()) {
                                let _ = tx.send(text).await;
                            }
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        });

        Ok(())
    }
}
