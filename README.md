# Noora Health – WhatsApp-Based Intervention Data Pipeline

This project implements an end-to-end data pipeline for analyzing data for the WhatsApp-based intervention. The pipeline ingests raw message data from Google Sheets, transforms and validates it in BigQuery, and visualizes it in Looker Studio.

Architecture Overview

The pipeline consists of three main stages:
Data Ingestion – Google Sheets → BigQuery (via Airbyte)
Data Transformation & Validation – BigQuery Pipeline
Visualization & Reporting – Looker Studio

1. Data Ingestion

Raw WhatsApp messages and status data are assumed to be exported from the application into Google Sheets.
Data is loaded into BigQuery using Airbyte:
Source: Google Sheets
Destination: BigQuery dataset
Tool: Airbyte
Refresh Frequency: Every 24 hours
This results in two raw tables in BigQuery: messages and statuses

2. BigQuery Transformation & Validation Pipeline

A BigQuery pipeline combines the raw tables into a cleaned analytical dataset and performs multiple data quality checks.
<img width="1279" height="278" alt="image" src="https://github.com/user-attachments/assets/667b94df-fbba-4c88-8584-f6b9214b7703" />

2.1 Combined Dataset Creation (ChatHist)
A unified table ChatHist is created by:
Joining messages and statuses on message_id
Parsing datetime columns into proper TIMESTAMP format
Adding derived fields: has_failed: Boolean flag indicating failed messages and hr_to_read: Time difference (in hours) between sent status and read status for a message. These fields are required for visualizations.
This table serves as the analytical base for reporting and validation.

2.2 Data Validation Checks

The following data quality validations are executed as part of the pipeline:

1. check_duplicates
Identifies near-duplicate records where content values are identical and inserted_at timestamps are within 2 minutes of each other

2. check_datetime_format
Flags records where message_inserted_at is NULL after parsing to detect rows with improperly formatted datetime strings that failed timestamp conversion.

3. duplicate_status
Identifies messages that have the same status associated multiple times to ensure status transitions are unique and not duplicated.

4. missing_prev_status
Identifies messages with missing prerequisite statuses such as delivered without prior sent and read without prior delivered

5. wrong_order
Flags messages where status transitions occur in incorrect order such as sent after delivered and delivered after read

3. Visualization using Looker Studio

The cleaned ChatHist table is connected to a Looker Studio report for dashboarding and analysis. The looker report can be found [here](https://lookerstudio.google.com/u/0/reporting/21d3dd29-f8b9-4a36-8361-984111d8a324/page/2REoF/edit).

3.1 Users Overview (2023)
Number of total users (calculated by creating a new column that copies value from masked_addressee for outbound messages and from masked_from_addr for inbound messages and takes distinct count of values of this new column) and number of active users (calculated by taking distinct count of masked_from_addr for inbound messages) by month for year 2023 (since data for recent years is not available)

3.2 Read Rate of Outbound Messages

Fraction of non-failed outbound messages that were read:

{Distinct message_id where direction = outbound AND status = 'read' AND has_failed = FALSE} }{Distinct message_id where direction = outbound AND has_failed = FALSE}

3.3 Time to Read Distribution
A histogram is generated for hr_to_read with filter for direction = outbound

3.4 Outbound Messages by Status (Last Week)
A bar chart displays distinct count of message_id grouped by status with filters direction = outbound and has_failed = FALSE for date within the last 7 days (chose the first week of 2024 since latest data is not available)
