# Kafka Error Handling Demo
## Architectural and Functional Flow Document

## 1. Purpose

This application demonstrates how Spring Kafka processes JSON messages and handles two different failure categories:

1. JSON deserialization failures caused by malformed input.
2. Business-processing failures caused by invalid customer data.

Both failure types are retried by Spring Kafka and eventually published to the dead-letter topic when processing remains unsuccessful.

## 2. Technology and Runtime Context

| Area | Technology |
|---|---|
| Language | Java 21 |
| Framework | Spring Boot 3.3.5 |
| Messaging | Apache Kafka |
| Broker | `localhost:9092` |
| Serialization | Spring Kafka `JsonSerializer` and `JsonDeserializer` |
| Error handling | `ErrorHandlingDeserializer`, `DefaultErrorHandler` |
| Recovery | `DeadLetterPublishingRecoverer` |
| Retry policy | Two-second fixed delay, three retries |
| Persistence | None |
| Database | None |

## 3. Logical Architecture

```text
                         HTTP Client / Postman
                                  |
                                  v
              POST /api/v1/kafka/publish-customer
                                  |
                                  v
                       CustomerController
                                  |
                                  v
                       JsonKafkaProducer
                                  |
                                  v
             KafkaTemplate<String, Customer>
                                  |
                         JsonSerializer
                                  |
                                  v
                         customer-topic
                                  |
                                  v
                    ErrorHandlingDeserializer
                                  |
                    +-------------+-------------+
                    |                           |
              Invalid JSON                  Valid JSON
                    |                           |
                    v                           v
           DeserializationException       JsonDeserializer
                    |                           |
                    |                           v
                    |                       Customer
                    |                           |
                    |                           v
                    |                   JsonKafkaConsumer
                    |                           |
                    |                    Business logic
                    |                       /       \
                    |                      /         \
                    |                 Success       Failure
                    |                    |              |
                    +--------------------+--------------+
                                         |
                                         v
                              DefaultErrorHandler
                                         |
                             Retry up to three times
                                         |
                                         v
                           DeadLetterPublishingRecoverer
                                         |
                                         v
                              customer-topic.DLT
                                         |
                                         v
                               DeadLetterConsumer
```

## 4. Application Components

### 4.1 `Customer`

The JSON payload model:

```text
Long id
String name
String email
```

The class contains a no-argument constructor, all-argument constructor, getters, setters, and `toString()`.

### 4.2 `CustomerController`

Exposes the REST endpoint:

```text
POST /api/v1/kafka/publish-customer
```

It accepts a JSON request body, maps it to `Customer`, and delegates publishing to `JsonKafkaProducer`.

### 4.3 `JsonKafkaProducer`

Uses constructor injection with:

```java
KafkaTemplate<String, Customer>
```

The customer is serialized to JSON by Spring Kafka's `JsonSerializer` and sent to `customer-topic`.

### 4.4 `JsonKafkaConsumer`

Listens to `customer-topic` using consumer group `customer-group`.

It performs business validation:

```java
if (customer == null
        || customer.getEmail() == null
        || !customer.getEmail().contains("@")) {
    throw new RuntimeException("Invalid customer email");
}
```

### 4.5 `DeadLetterConsumer`

Listens to `customer-topic.DLT` using consumer group `customer-dlt-group`.

It prints:

- Topic
- Partition
- Offset
- Key
- Value
- Relevant exception and original-record headers

The DLT consumer uses string deserialization so that malformed or unrecoverable records can still be observed without repeating the original customer deserialization failure.

## 5. Kafka Topics

### Main topic

```text
customer-topic
```

Receives customer messages from the REST API or another Kafka producer.

### Dead-letter topic

```text
customer-topic.DLT
```

Receives records that could not be successfully deserialized or processed after retries.

Both topics are configured with:

```text
Partitions: 1
Replicas: 1
```

## 6. Serialization and Deserialization Flow

### Producer side

```text
Customer Java object
        |
        v
JsonSerializer
        |
        v
JSON Kafka value with type metadata
        |
        v
customer-topic
```

### Consumer side

```text
Kafka record
    |
    v
ErrorHandlingDeserializer
    |
    v
JsonDeserializer
    |
    +-------------------+
    |                   |
 Valid JSON        Invalid JSON
    |                   |
    v                   v
Customer object   DeserializationException
    |                   |
    v                   v
Listener method    Error handler
```

## 7. Why `ErrorHandlingDeserializer` Is Required

Deserialization occurs before Spring Kafka invokes the listener method. Therefore, malformed JSON never reaches:

```java
public void consume(Customer customer)
```

A `try/catch` inside this method can catch business exceptions, but it cannot catch a JSON parsing exception that occurs before method invocation.

`ErrorHandlingDeserializer` wraps the delegate `JsonDeserializer`. When deserialization fails, it stores the failure information in Kafka record headers and allows the listener container's error handler to process the record.

This makes malformed JSON compatible with the same retry and DLT recovery mechanism used for business failures.

## 8. Functional Flow: Successful Message

Input:

```json
{
  "id": 101,
  "name": "Anand",
  "email": "anand@gmail.com"
}
```

Flow:

```text
1. Client sends HTTP POST request.
2. CustomerController maps the body to Customer.
3. JsonKafkaProducer sends the object to customer-topic.
4. JsonSerializer converts Customer to JSON.
5. ErrorHandlingDeserializer delegates to JsonDeserializer.
6. JsonDeserializer creates a Customer object.
7. JsonKafkaConsumer receives the Customer.
8. Email validation succeeds.
9. Listener completes successfully.
10. Kafka commits the record offset.
```

Expected output:

```text
Sending customer: Customer{id=101, name='Anand', email='anand@gmail.com'}
Received customer: Customer{id=101, name='Anand', email='anand@gmail.com'}
Customer processed successfully
```

## 9. Functional Flow: Business-Processing Failure

Input:

```json
{
  "id": 102,
  "name": "Customer Two",
  "email": "invalid-email"
}
```

Flow:

```text
1. JSON is produced successfully.
2. JsonDeserializer successfully creates Customer.
3. JsonKafkaConsumer receives Customer.
4. Business validation detects an invalid email.
5. Listener throws RuntimeException("Invalid customer email").
6. DefaultErrorHandler catches the exception.
7. The record is retried three times with a two-second delay.
8. Processing still fails.
9. DeadLetterPublishingRecoverer publishes the record to customer-topic.DLT.
10. DeadLetterConsumer receives and logs the failed record.
```

Retry timeline:

```text
Initial attempt  -> failure
Wait 2 seconds   -> retry 1 -> failure
Wait 2 seconds   -> retry 2 -> failure
Wait 2 seconds   -> retry 3 -> failure
Recovery         -> customer-topic.DLT
```

## 10. Functional Flow: Malformed JSON

Example raw Kafka value:

```text
This is not Customer JSON
```

Or:

```text
{"id":101,"name":"Anand"
```

Flow:

```text
1. Raw value is published directly to customer-topic.
2. ErrorHandlingDeserializer invokes JsonDeserializer.
3. JsonDeserializer fails to parse the value.
4. JsonKafkaConsumer is not invoked.
5. ErrorHandlingDeserializer records the exception in headers.
6. DefaultErrorHandler handles the failed record.
7. The record is retried three times with a two-second delay.
8. DeadLetterPublishingRecoverer publishes it to customer-topic.DLT.
9. DeadLetterConsumer logs the DLT record and error headers.
```

The malformed message should not be sent through the normal REST endpoint because Spring MVC may reject invalid HTTP JSON before it reaches Kafka.

Recommended direct test:

```bash
kafka-console-producer --bootstrap-server localhost:9092 --topic customer-topic
```

Then enter a malformed value.

## 11. Error Handler Configuration

The application uses:

```java
DeadLetterPublishingRecoverer recoverer =
        new DeadLetterPublishingRecoverer(kafkaTemplate);

FixedBackOff backOff = new FixedBackOff(2000L, 3L);

new DefaultErrorHandler(recoverer, backOff);
```

`DefaultErrorHandler` is responsible for retrying listener failures. `DeadLetterPublishingRecoverer` is responsible for publishing records that remain unsuccessful after the retry policy is exhausted.

By default, the recoverer maps a failed record from `customer-topic` to:

```text
customer-topic.DLT
```

## 12. DLT Record Information

The recovered record may contain original-record and exception-related Kafka headers, such as:

```text
kafka_dlt-exception-fqcn
kafka_dlt-exception-message
kafka_dlt-original-topic
kafka_dlt-original-partition
kafka_dlt-original-offset
```

The exact header set is supplied by the Spring Kafka version and recoverer configuration. The DLT consumer logs headers whose names contain `exception` or `original`.

## 13. Consumer Continuity

After a failed record is recovered to the DLT, the main consumer can continue processing later records on the topic. This prevents one unrecoverable record from permanently blocking all subsequent records.

```text
Record A: valid       -> processed successfully
Record B: invalid     -> retries -> DLT
Record C: valid       -> processed successfully
```

## 14. Test Matrix

| Test | Input | Expected result |
|---|---|---|
| Valid customer | Valid email | Processed successfully |
| Business failure | `invalid-email` | Three retries, then DLT |
| Missing email | `email: null` | Three retries, then DLT |
| Malformed JSON | Broken JSON syntax | Deserialization failure, retries, then DLT |
| Later valid record | Valid record after failure | Continues processing |

## 15. End-to-End Summary

```text
Producer
  -> JsonSerializer
  -> customer-topic
  -> ErrorHandlingDeserializer
  -> JsonDeserializer
  -> JsonKafkaConsumer
  -> business validation
  -> success OR exception
  -> DefaultErrorHandler
  -> FixedBackOff retries
  -> DeadLetterPublishingRecoverer
  -> customer-topic.DLT
  -> DeadLetterConsumer
```
