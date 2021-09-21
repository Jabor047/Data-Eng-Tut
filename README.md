# Data-Eng-Tut
Data warehouse tech stack with MySQL, DBT, Airflow, and Spark

## Business Need
You and your colleagues have joined to create an AI startup that deploys sensors to businesses, collects data from all activities in a business - from people’s interaction to the smart appliances installed in the company to reading environmental and other relevant information. Your startup is responsible to install all the required sensors, receive a stream of data from all sensors, and analyse the data to provide key insights to the business. The objective of your contract with the client is to reduce the cost of running the client facility as well as to increase the livability and productivity of workers. 
In this challenge you are tasked to create a scalable data warehouse tech-stack that will help you provide the AI service to the client.
By the end of this project, you should produce a tool that can be used as a basis for the data warehouse needs of your startup.

## Tasks
Complete the following tasks:
    - Create a DAG in Airflow that uses the bash/python operator to load the data files into your database. Think about a useful separation of Prod, Dev and Staging

    - Connect dbt with your DWH and write transformations codes for the data you can execute via the Bash or Python operator in Airflow. Write proper documentation for your data models and access the dbt docs UI for presentation. 

    -Check additional modules of dbt that can support you with data quality monitoring (e.g. great_expectations, dbt_expectations or re-data). 

    - Connect the reporting environment and create a dashboard out of this data

