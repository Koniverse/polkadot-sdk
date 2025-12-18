use rdkafka::{
    config::ClientConfig,
    producer::{FutureProducer, FutureRecord},
};
use serde_json::Value;

#[derive(Clone)]
pub struct KafkaProducer {
    producer: FutureProducer,
}

impl KafkaProducer {
    pub fn new(brokers: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;

        Ok(Self { producer })
    }

    pub async fn send_message(
        &self, 
        topic: &str, 
        key: &str, 
        data: Value
    ) -> Result<(), rdkafka::error::KafkaError> {
        let payload = serde_json::to_string(&data)
            .map_err(|_| rdkafka::error::KafkaError::MessageProduction(
                rdkafka::error::RDKafkaErrorCode::InvalidMessage
            ))?;

        let record = FutureRecord::to(topic)
            .key(key)
            .payload(&payload);

        self.producer.send(record, std::time::Duration::from_secs(5)).await
            .map(|_| ())
            .map_err(|(e, _)| e)
    }
}