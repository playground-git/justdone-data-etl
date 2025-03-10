# Architecture Overview

## System Components

This project implements a modular ETL pipeline for currency exchange rates with these components:

1. **API Client** - Fetches exchange rates from external APIs
2. **Data Models** - Validates and structures exchange rate data
3. **Storage** - Stores data in BigQuery
4. **Airflow DAGs** - Orchestrates the ETL process

## Architecture Decisions

### Modular Design
I chose a modular approach with abstract base classes for the API client. This makes the system flexible - we can easily replace the API provider (for example, switch from ExchangeRatesData API to Open Exchange Rates) without changing DAG logic.

### Class-Based Components
The components are implemented as classes with clear responsibilities:
- `BaseExchangeRateClient` - Abstract base class for API clients
- `ExchangeRatesDataClient` - Concrete implementation for the ExchangeRatesData API
- `ExchangeRate` - Pydantic model for data validation
- `BigQueryStorage` - Handles interactions with BigQuery

### Storage Strategy
I used BigQuery for storing exchange rates because:
1. It's optimized for analytical queries
2. It supports time-based partitioning for efficient historical analysis
3. It integrates well with other GCP services
4. It can handle large volumes of data

### Error Handling
Each component has detailed error handling with retry logic in Airflow. Errors are logged and tasks can be retried automatically, making the pipeline more resilient.

### Airflow for Orchestration
I used Airflow for orchestration because:
1. It provides built-in scheduling
2. It has a UI for monitoring and triggering DAGs
3. It supports retries and error handling
4. It allows for task dependencies

### Separate DAGs for Different Purposes
I created separate DAGs for:
1. **Setup** - One-time infrastructure setup
2. **Daily Rates** - Daily collection of exchange rates
3. **Monthly Rates** - Monthly historical data collection. NOTE: Fraankly speaking I'm not sure if we need this DAG and/or how we need to run it. I added it just in case.

This separation allows for better management and scheduling of different data collection needs.

## Limitations and Future Improvements

Current implementation has some limitations:
- Instead of writing a separate class for BigQuery storage, we could use Airflow operators directly, but a custom class was useful for testing and reusability
- No monitoring or alerting system beyond what Airflow provides
- Limited error handling for API rate limits

For a production system, I would add:
- Better monitoring and alerting
- More comprehensive error handling
- Data quality checks
- Unit and integration tests
- Support for more currencies and API providers
- Better architecture =)

## Data Flow

1. Fetch exchange rates from the API
2. Validate and transform data using Pydantic models
3. Store data in BigQuery
4. Schedule regular updates using Airflow

The entire process is orchestrated by Airflow DAGs, which ensure that tasks run in the correct order and handle any errors that occur. Still this is not perfect solution, but just to show how I would do that in real case scenario and show that I know how to use airflow.
