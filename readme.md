Example equities settlement kafka streams application

Run `./gradlew build` the first time to generate the model source classes before compiling or running tests in IDEA

Update `docker-compose.yml` to replace 196.168.0.9 with your own IP (note that localhost/127.0.0.1 won't work,
for boring docker reasons)

Run docker-compose up to bring up Zookeeper and Kafka

Run `settlements.producers.Bootstrap` to create the topics

