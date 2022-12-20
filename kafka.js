import * as kafka from 'kafka-node';
var Consumer = kafka.Consumer
var client = new kafka.KafkaClient({ kafkaHost: '10.10.10.60:9094' })
console.log("connected");
var consumer = new Consumer(
  client,
  [
    {topic: 'event_all_10s'}
  ]
)
// consumer.on('message', async function(message) {
//   console.log(message);
// })

export default consumer;