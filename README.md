# Multinational Retail Centralization

## Table of Contents

1. Description
2. Installation
3. Usage
4. File Structure
5. License

## Description

 The project aims to handle various data processing tasks, including extracting data from databases, cleaning and transforming data, and uploading it back to databases. It includes classes for database connectivity, data extraction, data cleaning, and data upload functionalities.

 In the SQL queries provided in the SQL file each query serves a specific purpose related to database management, data manipulation, or analysis. Below there are some examples of what is possible to achieve with theis code.

 > - Determining where the company operates and the number of stores in each country.
>- Identifying locations with the most stores.
>- Analyzing sales data to determine the month with the highest revenue.
>- Understanding online vs. offline sales distribution.
>- Calculating the percentage of sales from different store types.
>- Finding the months with the highest sales revenue each year.
>- Determining staff numbers in each country.
>- Identifying the store type generating the most sales in Germany.
>- Analyzing the average time taken between sales.

## Installation

To run these files is necessary to use pyton libraries as pandas and Numpy and Postgres for the SQL query


## Usage Instructions

**DatabaseConnector:** Use this class to connect to databases and perform operations like listing tables and uploading data.

 **DataExtractor:** This class provides methods to extract data from databases and other sources, such as S3 and JSON files.
 
 **DataCleaning:** Use this class to clean and transform data, including handling missing values, converting data types, and dropping unnecessary columns.

 **SQLqueries:** Each query serves a specific purpose related to database management, data manipulation, or analysis.

 **README:** This document provides guidance on using the project, including installation instructions, usage guidelines, and license information.

## File Structure

> - project_root
>
>>  - ├── data_cleaning.py
>>  - ├── data_extraction.py
>>  - ├── database_connector.py
>>  - ├── SQL_queries.sql
>>  - └── README.md