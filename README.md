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

### Setting up Airflow

1) Ensure you have Airflow installed <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/models.png) <br>
2) Place FinalAvail.sql & ResultsAvail.sql in dim folder[models > dim][See above] <br>
3) Place CleanAvail.sql in models[outside of dim folder][See above] <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/credentials.png) <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/credentials_2.png) <br>
4) For connecting Airflow to GCP account, I created a gcp_config folder under dbt folder & pointed the keyfile to credentials <br> folder[credentials/credentials.json][See above] <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/docker-compose.png) <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/docker-compose_2.png) <br>
5) Open up a terminal[I'm using VisualStudioCode], cd to folder containing dags[materials folder in above example] <br>
6) In terminal, run 'docker compose up -d' <br>
7) If successful, you should see the following blue messages[See above] <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/airflow.png) <br>
8) Login to Airflow on your localhost[mine is 8080] & you should see the following[See Above] <br>
9) Note that I named the dag as carpark.dag 

### Setting up carpark_dag.py
![alt text](https://github.com/kwquan/CarPark/blob/main/carpark_dag.png) <br>
1) Place carpark_dag.py under dags folder[See above]

### About the dag
Below is an explanation on how the dag works <br>
_get_request(ti): 
![alt text](https://github.com/kwquan/CarPark/blob/main/account_key.png) <br>
  a) Connects to gcp using AccountKey. Please change it to yours[See above] <br>
  b) Get current Date, Day & Time using datetime module <br>
  c) Call LTA API & save results as CSV[carpark_data.csv] <br>
  d) Creates GCS bucket if not exists <br>
  e) Get current time up to seconds & save as time variable <br>
  e) Push data to GCS bucket <br>
  f) Finally, push time as xcom value
 
 _create_ref_external_table(ti): <br>
  a) In Airflow, go to Admin > Connections
![alt text](https://github.com/kwquan/CarPark/blob/main/gcp_db.png) <br>
  b) Create connection to GCP[See above] <br>
  c) Please edit details as per your own <br>
  d) The above function will use Bigquery hook to create an external table[RefAvail] using the latest blob <br>
  e) Latest blob name is obtained by pulling xcom time variable from previous function <br>
  f) The external table is required to join back the other details of latest prediction data later on <br>
  
_create_external_table(): <br>
  a) Similar to above, this function uses the Bigquery hook to create an external table[RawAvail] by collating all blobs in the GCS bucket <br>
  b) Please change source_uris to your naming convention
  
run dbt: <br>  
This is where it gets a bit tricky. We need to create a virtual env in the Docker container & pip install all our dependencies[dbt-bigquery etc].
How I did it is as follows: <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/volume.png) <br>
  a) In docker-compose.yaml, I created another volume[before deployment] to store my virtual env[See above] <br>
![alt text](https://github.com/kwquan/CarPark/blob/main/run_dbt.png) <br>
  b) In the carpark_dag.py file, edit the bash command cd into the new volume, create virtual env[dbt-env], activate dbt-env & pip install dependencies <br>
  c) 1 instance can be "cd ${AIRFLOW_HOME}/envs && virtualenv dbt-env -p python && source dbt-env/bin/activate && pip install dbt-bigquery && pip install sklearn" <br>
  d) After installation, you can change the bash command to "cd ${AIRFLOW_HOME}/envs && source dbt-env/bin/activate && cd ${AIRFLOW_HOME}//dags/dbt/carpark && dbt run --profiles-dir ${AIRFLOW_HOME}/dags/gcp_profile" <br>
  e) The --profiles-dir is required to point to my gcp profile on Airflow volume instead of local machine[change accordingly] <br>
  f) Alternatively, use || to chain both bash command together. This way, bash will try to run the command in step e. If it fails, then it will do the installations in step c 
 
 _check_data(): <br>
  a) Branch operator that checks for sufficient records before starting model training <br>
  b) Here, I check for at least 500 records[grouped by IsWeekDay, Hour in FinalAvail table] <br>
  c) If yes, we return 'train_model', else return 'print_message' 
  
  _train_model(ti): <br>
    a) Start by importing sklearn. Note that we import in this function, NOT outside as virtual env needs to be activated first <br>
    b) Get current_date, current_hour & current_hour_type <br>
    c) Using Bigquery hook, get dataframe from FinalAvail table after removing duplicates[group by CarParkID, Area, Development, LotType, Agency, Date, IsWeekDay, Hour]. We could have used distinct[but currently not supported by BQ hook] <br>
    
  Flow Chart[_train_model] <br>
  ![alt text](https://github.com/kwquan/CarPark/blob/main/train_model.png) <br>
  
  _predict_model(ti): <br>
    a) Pull xcom key to_predict <br>
    b) If false, return 'No Prediction' <br>
    c) Else, do the following: <br>
      c1) Load model_pkl <br>
      c2) Read temp.csv & dataframe_pred.csv <br>
      c3) Run model predict on dataframe_pred <br>
      c4) Round off prediction as new column 'predicted_avail' <br>
      c5) Concat result with temp <br>
      c6) Filter for specific columns as save as pred_results.csv <br>
      c7) Create ANOTHER bucket if not exists[for storing predicted results] <br>
      c8) Push result to bucket as pred.csv <br>
      
  _create_external_result_table(): <br>
    a) Following the push of prediction results to the new bucket <br>
    b) Create ANOTHER external table[PredictAvail] using Bigquery hook <br>
    c) The run dbt step[See above] will join PredictAvail & RefAvail to create ResultsAvail[our FINAL table]
    ![alt text](https://github.com/kwquan/CarPark/blob/main/final_result.png) <br>
    d) In BigQuery, you should see the above[Note that there should only be 500 rows]
  
### DBT Process
![alt text](https://github.com/kwquan/CarPark/blob/main/dbt_process.png) <br>

### Looker
![alt text](https://github.com/kwquan/CarPark/blob/main/looker.png) <br>
Finally, visualize the results using Looker[See above] <br>
  
![alt text](https://github.com/kwquan/CarPark/blob/main/looker_2.png) <br>
One can examine the prediction_errors[See above] <br>
The above example shows that 'MARITIME SQUARED D OFF STREET' has the least error[for those Development displayed] 
