from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


from datetime import datetime
import datetime as dt
import requests
import pytz
import pandas as pd
import numpy as np
import pickle


def _get_request(ti):
    headers = {"AccountKey":"54UEKyywSWqaRTAqcoUqPQ==", "accept":"application/json"}
    response = requests.get("http://datamall2.mytransport.sg/ltaodataservice/CarParkAvailabilityv2", headers=headers)
    file = pd.json_normalize(response.json()['value'])
    file['Date'] = dt.datetime.now(pytz.timezone('Asia/Singapore')).strftime('%Y-%m-%d')
    file['Day'] = dt.datetime.now(pytz.timezone('Asia/Singapore')).weekday() 
    file['Time'] = dt.datetime.now(pytz.timezone('Asia/Singapore')).strftime('%H:%M')
    file.to_csv('carpark_data.csv',index=False)
    def upload_blob(bucket_name, data, destination_blob_name):
        gcs_hook = GCSHook(gcp_conn_id='gcp_db')
        try:
            gcs_hook.create_bucket(bucket_name=bucket_name, location='US')
        except:
            pass    
        gcs_hook.upload(bucket_name=bucket_name, object_name=destination_blob_name, filename=data)
    time = dt.datetime.now(pytz.timezone('Asia/Singapore')).strftime("%d:%m:%Y_%H:%M:%S")
    upload_blob('fine-balm-240505', 'carpark_data.csv', 'carpark_data_'+time+'.csv')
    ti.xcom_push(key='get_request', value=time) 

def _create_ref_external_table(ti):
    time = ti.xcom_pull(key='get_request') 
    bq_hook = BigQueryHook(gcp_conn_id='gcp_db')
    bq_hook.create_external_table(
        external_project_dataset_table='CarPark.RefAvail',
        source_uris="gs://fine-balm-240505/carpark_data_"+time+".csv",
        source_format='CSV',
        schema_fields=[
                    {"name": "CarParkID", "type": "STRING"},
                    {"name": "Area", "type": "STRING"},
                    {"name": "Development", "type": "STRING"},
                    {"name": "Location", "type": "STRING"},
                    {"name": "AvailableLots", "type": "INTEGER"},
                    {"name": "LotType", "type": "STRING"},
                    {"name": "Agency", "type": "STRING"},
                    {"name": "Date", "type": "STRING"},
                    {"name": "Day", "type": "INTEGER"},
                    {"name": "Time", "type": "STRING"}],
        skip_leading_rows=1)

def _create_external_table():
    bq_hook = BigQueryHook(gcp_conn_id='gcp_db')
    bq_hook.create_external_table(
        external_project_dataset_table='CarPark.RawAvail',
        source_uris=["gs://fine-balm-240505/*.csv"],
        source_format='CSV',
        schema_fields=[
                    {"name": "CarParkID", "type": "STRING"},
                    {"name": "Area", "type": "STRING"},
                    {"name": "Development", "type": "STRING"},
                    {"name": "Location", "type": "STRING"},
                    {"name": "AvailableLots", "type": "INTEGER"},
                    {"name": "LotType", "type": "STRING"},
                    {"name": "Agency", "type": "STRING"},
                    {"name": "Date", "type": "STRING"},
                    {"name": "Day", "type": "INTEGER"},
                    {"name": "Time", "type": "STRING"}],
        skip_leading_rows=1)

def _check_data():
    bq_hook = BigQueryHook(gcp_conn_id='gcp_db', location='US')
    check = bq_hook.get_records(sql="SELECT CASE WHEN MIN(records) >= 500 THEN TRUE ELSE FALSE END FROM (SELECT Count(*) records from CarPark.FinalAvail group by IsWeekDay, Hour)")
    if check[0][0] == True:
        return 'train_model'
    else:
        return 'print_message'
    
def get_hour(hour):
    if hour in [0,1]:
        return 'late'
    elif hour in range(2,7):
        return 'wee'
    elif hour in range(7,12):
        return 'morning'
    elif hour in [12,13]:
        return 'noon'
    elif hour in range(14,18):
        return 'afternoon'
    elif hour in range(18,22):
        return 'evening'
    else:
        return 'night'   

def _train_model(ti):
    from sklearn.ensemble import RandomForestRegressor
    current_date = dt.datetime.now(pytz.timezone('Asia/Singapore')).strftime('%Y-%m-%d')
    current_hour = int(dt.datetime.now(pytz.timezone('Asia/Singapore')).strftime('%H'))
    curent_hour_type = get_hour(current_hour)
    bq_hook = BigQueryHook(gcp_conn_id='gcp_db')
    dataframe = bq_hook.get_pandas_df(sql="SELECT CarParkID, Area, Development, LotType, Agency, Date, IsWeekDay, Hour, AVG(AvailableLots) DayAvgAvailableLots from CarPark.FinalAvail group by CarParkID, Area, Development, LotType, Agency, Date, IsWeekDay, Hour")
    cols = [col for col in dataframe.columns if col not in ['CarParkID','Date','DayAvgAvailableLots']]
    dataframe.loc[:,'istrain'] = 1
    dataframe.loc[(dataframe['Date'] == current_date) & (dataframe['Hour'] == curent_hour_type) ,'istrain'] = 0
    group = dataframe.loc[dataframe['istrain'] == 1,:].groupby(['Area', 'Development', 'LotType', 'Agency', 'IsWeekDay', 'Hour'])['DayAvgAvailableLots'].mean().reset_index().rename(columns={'DayAvgAvailableLots':'AvgAvailableLots'})
    dataframe = dataframe.merge(group, how='left', on=['Area', 'Development', 'LotType', 'Agency', 'IsWeekDay', 'Hour'])
    dataframe.loc[:,'AvgAvailableLots'] = np.ceil(dataframe.loc[:,'AvgAvailableLots'])
    temp = dataframe.loc[dataframe['istrain'] == 0,['Area', 'Development', 'LotType', 'Agency', 'IsWeekDay', 'Hour']]
    temp.to_csv('dags/models/to_predict/temp_pred.csv',index=False)

    one_hot_data = pd.get_dummies(data=dataframe,columns=cols,drop_first=True)
    dataframe_pred = one_hot_data.loc[one_hot_data['istrain'] == 0,:]
    dataframe_pred.to_csv('dags/models/to_predict/dataframe_pred.csv',index=False)
    print(temp.head())
    print(dataframe_pred.head())
    dataframe_train = one_hot_data.loc[one_hot_data['istrain'] != 0,:]
    dataframe_train.sort_values(by='Date',inplace=True)
    dataframe_train.to_csv('dags/models/to_predict/dataframe_train.csv',index=False)
    print(dataframe.shape, dataframe_train.shape, dataframe_pred.shape) 

    dataframe_train.drop(['CarParkID','Date'],axis=1,inplace=True)
    train_cols = [col for col in dataframe_train.columns if col not in ['AvgAvailableLots']]
    print(len(train_cols))
    X, y = dataframe_train[train_cols], dataframe_train[['AvgAvailableLots']].to_numpy().ravel()  
    RFR = RandomForestRegressor(random_state=0)
    model = RFR.fit(X, y)
    with open('dags/models/model_pkl', 'wb') as files:
        pickle.dump(model, files)
    ti.xcom_push(key='to_predict', value=True)   
       

def _predict_model(ti):
    response = ti.xcom_pull(key='to_predict')
    if not response:
        return 'No Prediction'
    model = pickle.load(open('dags/models/model_pkl', 'rb'))
    to_pred = pd.read_csv('dags/models/to_predict/dataframe_pred.csv')
    temp = pd.read_csv('dags/models/to_predict/temp_pred.csv')
    cols = [x for x in to_pred.columns if x not in ['CarParkID','Date','AvgAvailableLots']]
    print(len(to_pred[cols].columns))
    to_pred.loc[:,'predicted_avail'] = model.predict(to_pred[cols])
    to_pred.loc[:,'predicted_avail'] = np.ceil(to_pred.loc[:,'predicted_avail']).astype(np.int64)
    to_pred = pd.concat([to_pred, temp], axis=1)
    to_pred = to_pred.loc[:,['CarParkID', 'Area', 'Development', 'LotType', 'Agency', 'Date', 'IsWeekDay', 'Hour','predicted_avail']]
    to_pred.to_csv('dags/models/to_predict/pred_results.csv',index=False)
    gcs_hook = GCSHook(gcp_conn_id='gcp_db')
    try:
        gcs_hook.create_bucket(bucket_name='fine-balm-240505-pred', location='US')
    except:
        pass    
    print(to_pred.dtypes)
    gcs_hook.upload(bucket_name='fine-balm-240505-pred', object_name='pred.csv', filename='dags/models/to_predict/pred_results.csv')


def _print_message(ti):
    ti.xcom_push(key='to_predict', value=False)  
    print('insufficient records!')

def _create_external_result_table():
    bq_hook = BigQueryHook(gcp_conn_id='gcp_db')
    bq_hook.create_external_table(
        external_project_dataset_table='CarPark.PredictAvail',
        source_uris="gs://fine-balm-240505-pred/pred.csv",
        source_format='CSV',
        schema_fields=[
                    {"name": "CarParkID", "type": "STRING"},
                    {"name": "Area", "type": "STRING"},
                    {"name": "Development", "type": "STRING"},
                    {"name": "LotType", "type": "STRING"},
                    {"name": "Agency", "type": "STRING"},
                    {"name": "Date", "type": "STRING"},
                    {"name": "IsWeekDay", "type": "INTEGER"},
                    {"name": "Hour", "type": "STRING"},
                    {"name": "predicted_avail", "type": "INTEGER"}],
        skip_leading_rows=1)

    

with DAG('carpark_dag', start_date=datetime(2023,3,29,4,22,0,0), 
    schedule_interval='*/10 * * * *', catchup=False) as dag:
        
    get_request = PythonOperator(
        task_id='get_request',
        python_callable=_get_request
    )    

    create_table = PythonOperator(
        task_id='create_table',
        python_callable=_create_external_table
    )   

    run_dbt =  BashOperator(
        task_id='run_dbt',
        bash_command='cd ${AIRFLOW_HOME}/envs && source dbt-env/bin/activate && cd ${AIRFLOW_HOME}//dags/dbt/carpark && dbt run --profiles-dir ${AIRFLOW_HOME}/dags/gcp_profile'
    )  

    check_data = BranchPythonOperator(
    task_id='check_data',
    python_callable=_check_data
    )   

    get_dataframe = PythonOperator(
    task_id='train_model',
    python_callable = _train_model
    )

    print_message = PythonOperator(
        task_id='print_message',
        python_callable=_print_message
    )

    predict_dataframe = PythonOperator(
    task_id='predict_model',
    python_callable = _predict_model,
    trigger_rule=TriggerRule.ONE_SUCCESS
    )

    create_pred_table = PythonOperator(
    task_id='create_result_table',
    python_callable=_create_external_result_table,
    trigger_rule=TriggerRule.ONE_SUCCESS
    )   

    create_ref_table = PythonOperator(
    task_id='create_ref_table',
    python_callable=_create_ref_external_table   
    )



    get_request >> create_ref_table >> create_table >> run_dbt >> check_data >> [get_dataframe,print_message] >> predict_dataframe >> create_pred_table
    # ['CarParkID', 'Area', 'Development', 'AvailableLots', 'LotType',
    #    'Agency', 'Date', 'IsWeekDay', 'Hour', 'predicted_avail',
    #    'prediction_error']
    # >> check_data >> [get_dataframe,print_message]
    # python ${AIRFLOW_HOME}/envs/dbt-env/get-pip.py
    # pip install outside venv is in here:  /home/***/.local/lib/python3.7/site-packages
    # ./dbt-env/lib/python3.7/site-packages [modules installed here]
    # ['/home/***/.local/bin', '/usr/local/lib/python37.zip', '/usr/local/lib/python3.7', '/usr/local/lib/python3.7/lib-dynload', '/home/***/.local/lib/python3.7/site-packages', '/usr/local/lib/python3.7/site-packages', '/opt/***/dags', '/opt/***/config', '/opt/***/plugins'] modules imported from here
    # /opt/***/envs/dbt-env/bin/python [python executable used]
    # /opt/***/envs/dbt-env/bin/pip [pip used]
    #  /home/***/.local/lib/python3.7/site-packages/pandas [file modules imported from here]
    # AIRFLOW_HOME = /opt/***
    # cd ${AIRFLOW_HOME}/envs && virtualenv dbt-env -p python && source dbt-env/bin/activate && pip install dbt-bigquery && pip install sklearn
    # cd /opt/***/repo && source dbt-env/bin/activate && cd /opt/***/dags/dbt/carpark && dbt debug --config-dir 
    # cd ${AIRFLOW_HOME} && mkdir repo && cd repo && virtualenv dbt-env && pip install dbt-bigquery
    # cd /opt/***/repo && virtualenv dbt-env && source dbt-env/bin/activate
    # /tmp/***tmp1n2q78x4/dbt-env
    # /tmp/***tmp3dfnbi3c/dbt-env
    # 'source dbt-env/bin/activate || virtualenv dbt-env && pip install dbt-bigquery'