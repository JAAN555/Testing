# Cinema-Stocks---Data-Engineering
Data Engineering course's project

The datasets used: https://www.kaggle.com/datasets/disham993/9000-movies-dataset and https://www.kaggle.com/datasets/jacksoncrow/stock-market-dataset. Our project focuses on stocks and movies. We want to find out with our project how major movie release affect overall stock market performance of different sectors on or around the release date, how certain movie genres influence stock market performance in different sectors and how timing of a movie release affects stock market volatility across those sectors.  

We used Airflow for task orchestration. Our project has many tasks inside our DAG. You can trigger the DAG by doing "cd airflow", "docker-compose up -d", wait a little bit until the web server starts, log in with credentials (username is airflow and password is airflow), find the DAG called "download_stock_market_dataset" and trigger it. For all tasks to take effect it might take some time because the downloaded datasets are huge. 

At first, we properly ingests the data from Kaggle, clean it and then load cleaned data into DuckDB for data storage where we do some transformations and model the data by using star schema. 

File for our Python scripts, DAG definition and tasks definition can be found within "airflow/dags/tasks.py". You will also see other important folders such as "data" and "logs". It is possible to look at logs for each task within a DAG and within an execution. Usually all tasks should run without errors but sometimes there are anomalies which will result in some task to fail (mostly due to connection issues). To solve that, you just need to trigger the DAG once again. 

Ignore the files outside airflow and app folders.

If you have any technical issues with opening our project or any further questions then feel free to contact us by e-mail: jaan.otter@ut.ee, mols.ilmar@gmail.com, zhou.han@ut.ee.

We hope that you enjoy our project!








