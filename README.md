# weather-data-pipeline-ochestration
This project extracts data from a weather API, transforms the data and loads the data into PostgreSQL database.

STEPS INVOLVED INCLUDE:

IMPORTING NECESSARY LIBRARIES: In this step we import all needed libraries as seen in the python code, assuming they've been installed previously. If the haven't been installed, you can use %pip install package_name

DEFINE KEY PARAMETERS: Here we define key parameters to hold sensitive details. Note that a .env file has been created to hold the real values due to security concerns, and we are only referencing that file to get the real values. 

EXTRACTION: In this phase, we write a function to extract the data from the API, using also a for-loop, since we are getting the data for multiple locations. The data is returned in JSON format. 

TRANSFORM FUNCTION: In this phase we transform the data into the columns we want, making use of a for-loop and dictionary. The data is further transformed by modifying the data types of some of the columns. 

LOAD FUNCTION: Here, we define a function to load the transformed data into a PostgreSQL database

ORCHESTRATION: The pipeline orchestration was done with Apache airflow. In this phase, we first import the airflow DAG and other dependencies. Then we defined the default argument, and went further to write the DAG with all of its operators and callables. Screenshots of the successful run are also attached. 

This project was done using Python, Apache Airflow and PostgreSQL
