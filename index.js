const fs = require("fs");
const Kafka = require("node-rdkafka");

function readConfig(fileName) {
  const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
  return data.reduce((config, line) => {
    const [key, value] = line.split("=");
    if (key && value) {
      config[key] = value;
    }
    return config;
  }, {});
}

function produce(topic, config, key, value) {
  // creates a new producer instance
  const producer = new Kafka.Producer(config);
  producer.connect();
  producer.on("ready", () => {
    console.log("Producer is ready!");

    setInterval(() => {
      const key = `key-${Math.random()}`;
      const value = `value-${Math.random()}`;

      producer.produce(
        topic,
        -1,
        Buffer.from(value),
        Buffer.from(key),
        Date.now()
      );

      console.log(
        `Produced message to topic ${topic}: key = ${key}, value = ${value}`
      );
    }, 2000); // Send a message every 2 seconds
  });
}

function consume(topic, config) {
  // set the consumer's group ID, offset and initialize it
  config["group.id"] = "nodejs-group-1";
  const topicConfig = { "auto.offset.reset": "earliest" };
  const consumer = new Kafka.KafkaConsumer(config, topicConfig);
  consumer.connect();

  consumer
    .on("ready", () => {
      console.log("Consumer is ready, subscribing to topic...");
      consumer.subscribe([topic]);

      setInterval(() => {
        consumer.consume(1); // Poll one message at a time
      }, 1000); // Poll every second
    })
    .on("data", (message) => {
      console.log(
        `Consumed message from topic ${
          message.topic
        }: key = ${message.key.toString()}, value = ${message.value.toString()}`
      );
    })
    .on("event.error", (err) => {
      console.error("Consumer error:", err);
    });

  consumer.on("disconnected", () => {
    console.log("Consumer disconnected");
  });
}

function main() {
  const config = readConfig("client.properties");
  const topic = "notification-topic";

  produce(topic, config);

  consume(topic, config);
}

main();
