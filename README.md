
## Cricket end-to-end Data Pipeline on Snowflake

This project serves as a valuable addition to my portfolio, showcasing my proficiency in designing and implementing an end-to-end data pipeline on Snowflake, focusing on cricket sport data. The pipeline seamlessly loads data from various stages, ensuring a well-organized flow from the initial stage through to the consumption layer. The project demonstrates my foundational understanding of Snowflake ETL (Extract, Transform, Load) processes and pipeline automation.



## Features

### End-to-End Pipeline
The data pipeline is designed to cover the entire data processing lifecycle. It comprises multiple stages, including the following:

#### a. Stage Layer
The initial stage involves loading raw data from external sources into a designated stage area within Snowflake. This step sets the foundation for subsequent processing.

#### b. Raw Layer
In the raw layer, the data is ingested and stored in its original, unaltered format. This layer acts as a repository for the raw data, preserving its integrity for future reference and auditing. This stage also shows ways to fetch json data using lateral flatten functions

#### c. Clean Layer
The clean layer involves transforming the raw data into a structured and usable format. Data cleaning and normalization processes took place in this layer, ensuring high data quality and consistency.

#### d. Consumption Layer
The final stage of the pipeline is the consumption layer, where the processed data is made available for analytics, reporting, or any other relevant applications. This layer is optimized for easy access and analysis by end-users.
.....
