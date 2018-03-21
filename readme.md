Example equities settlement kafka streams application

Run `./gradlew build` the first time to generate the model source classes before compiling or running tests in IDEA

Set MY_IP to your local LAN IP eg, 192.168.0.xx (note that localhost/127.0.0.1 won't work, for boring docker reasons)

Run docker-compose up to bring up Zookeeper and Kafka

Run `settlements.producers.Bootstrap` to create the topics

