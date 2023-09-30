# E-commerce Cart Abandonment Consumer App

## Overview
This demo project showcases the process of analyzing user shopping carts in an e-commerce system using Amazon Kinesis and the Kinesis Client Library (KCL). It provides a complete example of how to consume, process, and analyze events from a Kinesis data stream.

## Features
- **Cart Abandonment Detection:** The application consumes information about user shopping carts, and detects when a cart is abandoned. It calculates the time of abandonment based on predefined threshold.
- **Notification to Users:** When a cart is detected as abandoned, the notification can be sent to the user as a reminder to complete the purchase, helping to recover potentially lost sales.
- **Average Potential Order Size:** The application analyzes the cart abandonment events over specified time intervals (e.g., 30-second windows) and calculates the average potential order size for the batch of abandoned carts. This insight can assist in understanding customer behavior and optimizing marketing strategies.
- **Scalability:** Built using the Kinesis Client Library, the application can easily scale to handle large volumes of cart abandonment events, making it suitable for use in real-world e-commerce platforms.
- **Logging:** The application utilizes log4j for robust logging, allowing you to monitor its performance and troubleshoot any issues effectively.
- **Configurability:** You can configure the application with various parameters, such as the Kinesis stream name, AWS region, and more, to adapt it to your specific environment and requirements.

## Prerequisites
Before running this demo project, make sure you have the following prerequisites:

* **Java Development Kit (JDK)**: Ensure you have Java 8 or later installed.
* **Amazon Web Services (AWS) Account**: You need an AWS account to create and manage Kinesis streams, DynamoDB tables, and other AWS resources.
* **AWS CLI**: Install and configure the AWS Command Line Interface (CLI) to set up your AWS credentials.

## Project Structure
The project includes the following classes and files:

* **CartAbandonmentEvent.java**: A POJO class representing a cart abandonment event.
* **CartItem.java**: A POJO class representing an item in a cart.
* **EventsManager.java**: Manages cart abandonment events, calculates average order sizes, and sends notifications.
* **EventRecordProcessor.java**: Processes Kinesis records, aggregates events, and performs analysis.
* **EventRecordProcessorFactory.java**: Provides instances of EventRecordProcessor for the KCL scheduler.
* **KclConsumerApp.java**: Main application class that sets up the KCL environment and runs the scheduler.
* **pom.xml** - The Maven project configuration file that includes project details, dependencies, and build settings.
* **log4j.properties** - Sets up logging for the application, including both console output and log file (`kinesis.log`).

## Getting Started

To run the application, follow these steps:
1. Clone this repository to your local machine.
2. Build the project using Maven:
   ```bash
   mvn clean package
   ```
3. Run the application with the required command-line arguments:
   ```bash
   java -jar target/aws-kcl-consumer-jar-with-dependencies.jar <appName> <streamName> <region>
   ```
   Replace `app_name`, `stream_name` and `region` with your specific application name, Kinesis stream name and AWS region. For example:
   ```bash
   java -jar target/cart-abandonment-demo.jar CartAbandonmentConsumerApp kinesis-stream us-east-1
   ```
4. The application will start consuming events from the specified Kinesis stream, process them, and log the results.

## Shutdown and Error Handling
The demo project includes graceful shutdown and error handling mechanisms. It ensures that when the application is terminated, it will checkpoint its progress and log any encountered errors.