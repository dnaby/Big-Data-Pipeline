from kafka.admin import KafkaAdminClient, NewTopic


# Log
print("Creating topic...")

# Configuration
bootstrap_servers = 'kafka:9092'
topic = 'openweathermap'
partitions = 1
replication_factor = 1

# Create Kafka admin client
admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

# Create topic
topic = NewTopic(name=topic, num_partitions=partitions, replication_factor=replication_factor)
admin_client.create_topics([topic])

# Close the admin client
admin_client.close()

print("Topic created✅")