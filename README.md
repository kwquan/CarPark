# LTA CarPark Availability 

### Intro
![alt text](https://github.com/kwquan/CarPark/blob/main/process.png)
Tools: Airflow, Docker, DBT, GCP

This project is an end-to-end ELT pipeline that does the following:
1) Calls the LTA API to get no. of available lots for HDB, LTA and URA carparks[every 10 minutes]
2) Create GCS bucket if not exists
3) Push data[CSV] to bucket
4) Creates Bigquery external table[RefAvail] based on latest blob
5) Creates Bigquery external table[RawAvail] by collating all blobs in GCS bucket
6) Runs DBT to create CleanAvail, FinalAvail 
7) Checks for sufficient data
8) [IF INSUFFICIENT DATA]: print 'insufficient records!' message <br>
[BELOW STEPS IF SUFFICIENT DATA] <br>
9) [IF SUFFICIENT DATA]: <br>
  a) Get unique records[group by CarParkID, Area, Development, LotType, Agency, Date, IsWeekDay, Hour] <br>
  b) Create istrain column based on Date & Hour <br>
  c) Using training data[istrain=1], create label column[AvgAvailableLots][group by Area, Development, LotType, Agency, IsWeekDay, Hour] <br>
  d) Left join grouped results on dataframe[on Area, Development, LotType, Agency, IsWeekDay, Hour] <br>
  e) For prediction data[istrain=0], save a copy as CSV[temp.csv] <br>
  f) Create one-hot encoded columns <br>
  g) Separate & save them to training & testing dataframes[dataframe_train.csv,dataframe_pred.csv] <br>
  h) Train RandomForestRegressor using training dataframe  <br>
  i) Save trained model to pickle file[model_pkl] <br>
10) [IF ONE SUCCESS]: <br>
  a) Load trained model[model_pkl] <br>
  b) Read temp & pred dataframes[temp.csv,dataframe_pred.csv] <br>
  c) Predict model on pred dataframe <br>
  d) Round up results & save as predicted_avail column <br>
  e) Concatenate resulting dataframe[to_pred] & temp dataframe together <br>
  f) Filter for specific columns[CarParkID, Area, Development, LotType, Agency, Date, IsWeekDay, Hour,predicted_avail] <br>
  g) Save to CSV[pred_results.csv] <br>
  h) Create GCS bucket if not exist[for storing predicted results] <br>
  i) Push data to bucket as pred.csv <br>
11) [IF ONE SUCCESS]: <br>
  a) Create Bigquery external table[PredictAvail][using prediction results in GCS bucket] <br>
  b) DBT run in STEP 6 will do a left join of RefAvail on PredictAvail to create ResultsAvail table <br>
12) Visualize results using Looker

### Starting up Airflow

1) Ensure you have Airflow installed
![alt text](https://github.com/kwquan/CarPark/blob/main/models.png)
2) Place FinalAvail.sql & ResultsAvail.sql in dim folder[models > dim][See above]
3) Place CleanAvail.sql in models[outside of dim folder][See above]
![alt text](https://github.com/kwquan/CarPark/blob/main/credentials.png)
4) For connecting Airflow to GCP account, I created a gcp_config folder under dbt folder & pointed the keyfile to credentials folder[credentials/credentials.json][See above] 
