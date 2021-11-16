# Data_Pipeline_NYC_TLC 

 

This repository provides the solution for following the problem statement. Please visit this project's wiki for solution and deployment, which explains the architecture and steps to deploy this pipeline. 

 

Problem Statement: 

The city of New York provides historical data of "The New York City Taxi and Limousine Commission" (TLC) (https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Your colleagues from the data science team want to create various evaluations and predictions based on this data. Depending on their different use cases, they need the output data in a row-oriented and column-oriented format. So, they approach you and ask for your help. Your colleagues only rely on a frequent output of the datasets in these two formats, so you have a free choice of your specific technologies. 

Your tasks: 

  1) Build a data pipeline that imports the TLC datasets for at least the last three years 
  2) Enhance your pipeline in a way that automatically imports future datasets 
  3) Convert the input datasets to a column-oriented (e.g., Parquet) and a row-oriented format (e.g., Avro) 
  4) Import or define a schema definition 
  5) Structure the data in that way so that the data science team can perform their predictions: 
      1. The input data is spread over several files, including separate files for “Yellow” and “Green” taxis. Does it make sense to merge those input files into one? 
      2. You will notice that the input data contains “date and time” columns. Your colleagues want to evaluate data also on hour-level and day of week-level. Does that affect your output structure in some way? 
  6) To determine the correctness of your output datasets, your colleagues want you to write the following queries: 
       1. The average distance is driven by yellow and green taxis per hour 
       2. Day of the week in 2019 and 2020 which has the lowest number of single rider trips 
       3. The top 3 of the busiest hours 

Addition: 

  1) Your data scientists want to make future predictions based on weather conditions. How would you expand your pipeline to help your colleagues with this task? 
  2) Another colleague approaches you. He is an Excel guru and makes all kinds of stuff using this tool forever. So, he needs all the available taxi trip records      in the XLSX format. Can you re-use your current pipeline? How does this output compare to your existing formats? Do you have performance concerns? 
