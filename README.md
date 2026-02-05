# Scalable Systems Integration Platform

## Overview

This project demonstrates an enterprise integration platform combining
**Spring Boot (Java producers)** and **Python async consumers** to
ingest, stream, process, deduplicate, and deliver analytics data.

Data Sources: - REST CRM - SOAP CRM - Inventory Service

Pipeline:

    Sources → Producer (Spring Boot) → Kafka → Consumer (Python Async) → Redis → Analytics API

------------------------------------------------------------------------

## Spring Boot + Python Integration Explanation

### Why Hybrid Architecture?

  Layer       Technology       Reason
  ----------- ---------------- ----------------------------------------
  Producers   Spring Boot      Enterprise integrations & SOAP support
  Streaming   Kafka            Decoupling & buffering
  Consumers   Python AsyncIO   High‑throughput event processing
  State       Redis            Idempotency & fast lookups

Spring Boot handles complex external integrations (REST/SOAP). Python
handles high‑volume asynchronous processing efficiently.

This simulates real companies where integration teams use JVM while data
pipelines use Python.

------------------------------------------------------------------------

## Message Flow Pipeline

                 +----------------+
                 | REST CRM Mock |
                 +--------+-------+
                          |
                 +--------v-------+
                 | SOAP CRM Mock |
                 +--------+-------+
                          |
                    Spring Boot
                    Producer Service
                          |
                          v
                       Kafka
             +------------+------------+
             |                         |
             v                         v
     Python Consumer           Redis Dedup Store
             |                         |
             +-----------+-------------+
                         v
                  Analytics Service

------------------------------------------------------------------------

## Scalability Strategies

1.  Kafka buffering prevents producer overload
2.  Async Python workers process in parallel
3.  Cached API responses reduce upstream load
4.  Stateless consumer instances allow horizontal scaling

System can scale by:

    docker compose up --scale consumer-service=5

------------------------------------------------------------------------

## Reliability Strategies

  Mechanism            Purpose
  -------------------- ------------------------------
  Redis Idempotency    Prevent duplicate analytics
  Retry loops          Handle service outages
  Kafka durability     No data loss
  Caching              Reduce external API pressure
  Health checks        Detect failures
  Prometheus metrics   Monitor throughput

------------------------------------------------------------------------

## Running the System

    docker compose up --build

Verify logs:

Producer:

    docker logs -f producer-service

Consumer:

    docker logs -f consumer-service

------------------------------------------------------------------------

## Sample Successful Output

Producer:

    SOAP CUSTOMER -> C003|Charlie Brown|charlie@example.com
    Publishing customer id=C001
    Finished source=crm topic=customer_data

Consumer:

    Customer received: C001 (Alice Smith)
    Inventory received: PROD-001 stock=50
    [Worker 0] Processing C001:PROD-001
    Sent analytics: {customerId:C001, product:PROD-001}
    Duplicate ignored -> C001:PROD-001

Analytics:

    POST /analytics/data
    {
      "customerId": "C001",
      "customerName": "Alice Smith",
      "products": [{ "id": "PROD-001", "name": "Wireless Mouse", "stock": 50 }]
    }

------------------------------------------------------------------------

## Monitoring

Prometheus: http://localhost:9090

Grafana: http://localhost:3000

Metrics include: - events/sec - duplicates skipped - analytics delivery
rate

------------------------------------------------------------------------

## Reset Environment

    docker compose down -v
    docker compose up --build

------------------------------------------------------------------------

## Technologies

Spring Boot, Kafka, Python AsyncIO, Redis, SOAP, REST, Docker Compose,
Prometheus, Grafana
