# Kafka Error Handling Demo

This beginner-friendly Spring Boot application demonstrates JSON production and consumption, deserialization failures, business-processing failures, retries, and a dead-letter topic.

For the full component architecture and end-to-end functional flows, see [ARCHITECTURE_AND_FUNCTIONAL_FLOW.md](ARCHITECTURE_AND_FUNCTIONAL_FLOW.md).

## Prerequisites

- Java 21
- Maven
- A local Apache Kafka broker listening on `localhost:9092`

Docker is not required by this project.

Run the application with:

```bash
mvn spring-boot:run
```

Build and verify compilation with:

```bash
mvn clean compile
```

The application creates these one-partition topics if they do not already exist:

- `customer-topic`
- `customer-topic.DLT`

## Send a valid customer

```bash
curl -X POST http://localhost:8080/api/v1/kafka/publish-customer \
  -H "Content-Type: application/json" \
  -d '{"id":101,"name":"Customer One","email":"one@example.com"}'
```

The consumer prints `Customer processed successfully`.

Expected console output includes:

```text
Sending customer: Customer{id=101, name='Customer One', email='one@example.com'}
Received customer: Customer{id=101, name='Customer One', email='one@example.com'}
Customer processed successfully
```

## Trigger a business failure

This JSON is valid and deserializes into `Customer`, but the email fails business validation:

```bash
curl -X POST http://localhost:8080/api/v1/kafka/publish-customer \
  -H "Content-Type: application/json" \
  -d '{"id":102,"name":"Customer Two","email":"invalid-email"}'
```

The listener throws `RuntimeException("Invalid customer email")`. `DefaultErrorHandler` waits two seconds between three retries, then `DeadLetterPublishingRecoverer` publishes the record to `customer-topic.DLT`.

Retry sequence:

```text
Initial attempt -> failure
Wait 2 seconds -> retry 1 -> failure
Wait 2 seconds -> retry 2 -> failure
Wait 2 seconds -> retry 3 -> failure
Recovery -> customer-topic.DLT
```

## Trigger a deserialization failure

Publish malformed JSON directly with Kafka's console producer so the producer does not correct it:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic customer-topic
>{"id":103,"name":"Broken Customer","email":"broken@example.com"
```

The missing closing brace causes `JsonDeserializer` to fail before `JsonKafkaConsumer.consume` is called. `ErrorHandlingDeserializer` captures that exception in record headers, allowing the container error handler to recover the record to the DLT. The DLT consumer prints the topic, partition, offset, key, value, and relevant exception/original-record headers.

The malformed message must be sent directly to Kafka, not through the REST endpoint. Spring MVC may reject malformed HTTP JSON before it reaches Kafka.

The application also supports testing a raw value such as:

```text
This is not Customer JSON
```

## Why ErrorHandlingDeserializer is required

The listener method only receives records after deserialization. Therefore a `try/catch` inside `consume(Customer customer)` can handle business exceptions, but it can never catch malformed JSON. `ErrorHandlingDeserializer` wraps `JsonDeserializer`, converts deserialization failures into headers on the record, and lets Spring Kafka's `DefaultErrorHandler` apply retry and DLT recovery consistently.

The overall flow is:

```text
Kafka record -> ErrorHandlingDeserializer -> JsonDeserializer
                    | valid                    | invalid
                    v                          v
              Customer listener        DefaultErrorHandler
                    |                          |
              business failure         retries, then DLT
```

## Architecture summary

```text
REST client
    |
    v
CustomerController
    |
    v
JsonKafkaProducer -> JsonSerializer -> customer-topic
                                           |
                                           v
                              ErrorHandlingDeserializer
                                           |
                            +--------------+--------------+
                            |                             |
                       Invalid JSON                   Valid JSON
                            |                             |
                            v                             v
                     Error handler                 JsonDeserializer
                                                          |
                                                          v
                                                JsonKafkaConsumer
                                                          |
                                               Business validation
                                                   /           \
                                               Success       Failure
                                                   |             |
                                                   +------+------+
                                                          |
                                                          v
                                                DefaultErrorHandler
                                                          |
                                                   Retry / recovery
                                                          |
                                                          v
                                                customer-topic.DLT
                                                          |
                                                          v
                                                 DeadLetterConsumer
```

## Configuration summary

| Setting | Value |
|---|---|
| Broker | `localhost:9092` |
| Main topic | `customer-topic` |
| DLT | `customer-topic.DLT` |
| Consumer group | `customer-group` |
| DLT consumer group | `customer-dlt-group` |
| Retry delay | 2 seconds |
| Retry count | 3 |
| REST port | `8080` |

## Expected behavior

| Scenario | Deserialization | Business logic | Result |
|---|---|---|---|
| Valid customer | Succeeds | Succeeds | Offset committed |
| Invalid email | Succeeds | Fails | Retries, then DLT |
| Malformed JSON | Fails | Listener not called | Retries, then DLT |
| Valid record after failure | Succeeds | Succeeds | Consumer continues |

## Important classes

- `CustomerController`: accepts customer JSON over HTTP.
- `JsonKafkaProducer`: publishes `Customer` objects with `JsonSerializer`.
- `JsonKafkaConsumer`: validates and processes customers.
- `KafkaErrorHandlerConfig`: configures retries and DLT recovery.
- `DeadLetterConsumer`: observes records recovered to the DLT.
- `KafkaTopicConfig`: creates the main topic and DLT.
