<h1 align="center">Kafka-anomaly-detection</h1>
<p align="center">This project is a real-time data stream processing application, specifically focusing on anomaly detection. It uses Kafka for data streaming and Python for data generation and anomaly detection.</p>

## Screenshots
![kafka-anomaly](https://github.com/mikolaj-halemba/Kafka-anomaly-detection/blob/main/images/image_1.png)
![kafka-anomaly](https://github.com/mikolaj-halemba/Kafka-anomaly-detection/blob/main/images/image_2.png)

## Built With
- Kafka
- Python
- Docker-compose

### Requirements
- numpy
- confluent_kafka

<h2> Instructions: </h2>
- Navigate to the folder containing the Docker compose file: ** cd /path/to/docker-compose/directory ** <br/>
- Run Docker compose up: ** docker-compose up ** <br/>
- Create new Kafka topics for transactions and anomaly detection:<br/>
	** docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic transactions ** <br/>
	** docker exec broker kafka-topics --bootstrap-server broker:9092 --create --topic anomaly ** <br/>
- Run the Python Kafka producer script: ** python run_producer.py transactions ** <br/>
- Run the anomaly detection script: ** python anomaly_detection.py 3 transactions transactions anomaly ** <br/>
	
	
## Author

**Halemba Mikołaj**


- [Profile](https://github.com/mikolaj-halemba "Halemba Mikołaj")
- [Email](mailto:mikolaj.halemba96@gmail.com?subject=Hi "Hi!")


## 🤝 Support

Contributions, issues, and feature requests are welcome!

Give a ⭐️ if you like this project!	
