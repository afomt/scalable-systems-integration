# Scalable Systems Integration Platform

## What this project does

Collects customer data (REST + SOAP) and inventory data, streams them
via Kafka, processes them asynchronously, deduplicates using Redis, and
sends analytics events.

------------------------------------------------------------------------

## Prerequisites

-   Docker Desktop (or Docker Engine + Compose)
-   Ports free: **8080, 8011, 9000, 6379, 9092**

Verify:

    docker --version
    docker compose version

------------------------------------------------------------------------

## Start the whole stack

From project root:

    docker compose up --build

First startup takes a few minutes (Kafka + images).

When ready you should see services:

    producer-service
    consumer-service
    kafka
    zookeeper
    redis
    crm-rest-mock
    crm-soap-mock
    analytics-mock

------------------------------------------------------------------------

## Verify services are running

    docker compose ps

All should be **Up (healthy)**

------------------------------------------------------------------------

## Check producer logs (data ingestion)

    docker logs -f producer-service

Expected:

    SOAP CUSTOMER -> C003|Charlie Brown|charlie@example.com
    Publishing customer id=C001
    Finished source=crm topic=customer_data
    Finished source=inventory topic=inventory_data

This confirms: - REST integration works - SOAP integration works - Kafka
publishing works

------------------------------------------------------------------------

## Check consumer logs (processing)

    docker logs -f consumer-service

Expected:

    Customer received: C001 (Alice Smith)
    Inventory received: PROD-001 stock=50
    [Worker 0] Processing C001:PROD-001
    Sent analytics: {...}
    Duplicate ignored -> C001:PROD-001

This confirms: - Kafka consumption works - Parallel processing works -
Idempotency works

------------------------------------------------------------------------

## Check analytics output

    docker logs -f analytics-mock

Expected POST payloads:

    {'customerId': 'C001', 'products':[...], 'timestamp': ...}

------------------------------------------------------------------------

## Kafka inspection (optional)

Enter Kafka container:

    docker exec -it kafka bash

List topics:

    kafka-topics.sh --bootstrap-server localhost:9092 --list

Consume events:

    kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic customer_data --from-beginning

------------------------------------------------------------------------

## Reset environment (clean start)

    docker compose down -v
    docker compose up --build

Removes Redis + Kafka data (clears deduplication)

------------------------------------------------------------------------

## Run tests

### Java Producer tests

    docker compose run --rm producer-service mvn test

### Python Consumer tests

    docker compose run --rm consumer-service pytest

------------------------------------------------------------------------

## Performance expectations

  Requirement                Result
  -------------------------- ---------------------
  10,000 records/hour        Async worker pool
  Inventory export \<5 min   Parallel processing
  No duplicates              Redis idempotency

------------------------------------------------------------------------

## Troubleshooting

### Kafka connection errors

Wait \~30s after startup --- Kafka initializes slowly.

### Redis not ready

Check:

    docker logs redis

### No SOAP data

Verify:

    http://localhost:8011/?wsdl

------------------------------------------------------------------------

## Stop system

    docker compose down
