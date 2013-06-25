Bagheera Integration
====================

Production Telemetry Data is submitted to a [Bagheera][1] server.

Bagheera is a REST service that accepts submissions via HTTP.

As of version 0.11, submissions are first saved to a Kafka queue, which is then
processed by one or more Kafka consumers.

As a preliminary way to integrate the prototype telemetry server with the
existing pipeline, we use the `[KafkaReplayConsumer][2]` to replay the
production requests against the prototype server.

This results in no data loss of production data, and an optionally-sampled
stream of data being directed to a second server.

The simple approach ro running such a replay consumer would be to use the
packaged `consumer` script distributed with [Bagheera][1] with a command like:

```bash
# Set variables
export BAGHEERA_HOME=/path/to/bagheera
export KAFKA_TOPIC=my_topic
export KAFKA_GID=replay_${KAFKA_TOPIC}_20130624 # ensure the gid is unique to this consumer!
export REPLAY_HOST=www.example.com
export SAMPLE_RATE=0.01 # use '1' to replay all requests, or a float less than one to sample.

# Run the command
sudo -u bagheera_user $BAGHEERA_HOME/bin/consumer com.mozilla.bagheera.consumer.KafkaReplayConsumer \
 -t $KAFKA_TOPIC \
 -gid $KAFKA_GID \
 -p $BAGHEERA_HOME/conf/kafka.consumer.properties \
 --copy-keys true \
 --dest "http://$REPLAY_HOST/submit/telemetry/%k" \
 --sample $SAMPLE_RATE \
 --delete false
```

Unfortunately, a quirk of our network security is that outbound HTTP requests
are not allowed, so we need to specify an HTTP proxy.  This can easily be done
at the JVM level, so we can invoke the full command manually.  We end up with:

```bash
export PROXY_HOST=example.proxy.mozilla.com
export PROXY_PORT=9999
sudo -u bagheera java -Dhttp.proxyHost=$PROXY_HOST -Dhttp.proxyPort=$PROXY_PORT ...<snip long list of JVM args copied from the 'consumer' script>... -cp <snip long classpath> com.mozilla.bagheera.consumer.KafkaReplayConsumer -t $KAFKA_TOPIC -gid $KAFKA_GID -p $BAGHEERA_HOME/conf/kafka.consumer.properties --copy-keys true --dest "http://$REPLAY_HOST/submit/telemetry/%k" --sample $SAMPLE_RATE --delete false
```

[1]: https://github.com/mozilla-metrics/bagheera "Bagheera"
[2]: https://github.com/mozilla-metrics/bagheera/blob/master/src/main/java/com/mozilla/bagheera/consumer/KafkaReplayConsumer.java "KafkaReplayConsumer"
