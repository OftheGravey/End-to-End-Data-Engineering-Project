/opt/bitnami/kafka/bin/kafka-console-consumer.sh \
    --topic pg-changes.public.authors \
    --bootstrap-server localhost:9092 \
    --from-beginning