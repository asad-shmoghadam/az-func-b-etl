import logging
import os
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
import xlrd
import numpy as np
import re
import tempfile
from datetime import datetime


def upload_csv_blob(file_name, data_frame):
    # This is the connection to the blob storage, with the Azure Python SDK
    logging.info(f"File to upload: {file_name}")
    try:
        connection = os.getenv("ccpsattachmentsstorage_STORAGE")
        blob_service_client = BlobServiceClient.from_connection_string(connection)
        container_client = blob_service_client.get_container_client("sensorsdata")
    except Exception as e:
        logging.error(f"Connection to blob service was unsuccessful: {e}")

    logging.info(f"Connection successful")

    temp_path = tempfile.gettempdir()
    sensor_data_path = os.path.join(temp_path, file_name)
    logging.info(f"CSV file path: {sensor_data_path}")

    try:
        # sensor_data_csv = data_frame.to_csv(sensor_data_path, mode='a', index=False, header=False)
        # sensor_data_csv = data_frame.to_csv(mode='w', index=False, header=True)
        sensor_data_csv = data_frame.to_csv(index=False, header=True)
        print(f"Dataframe: {sensor_data_csv}")
        logging.info(f"Dataframe: {sensor_data_csv}")
    except Exception as e:
        logging.error(f"CSV convert failed: {e}")

    try:
        container_client.upload_blob(name=file_name,data=sensor_data_csv)
        # container_client.create_blob_from_path(container_name="",blob_name="",file_path="")
    except Exception as e:
        logging.error(f"Cannot upload to Blob Storage: {e}")

    # Here is the upload to the blob storage
    # tab1_csv=b.to_csv(header=False,index=False,mode='w')
    # name1=(os.path.splitext(text1)[0]) +'.csv'
    # container_client.upload_blob(name=name1,data=tab1_csv)


def main(sensorsExcelBlob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {sensorsExcelBlob.name}\n"
                 f"Blob Size: {sensorsExcelBlob.length} bytes")

    # file = myblob.read()
    try:
        sensors_excel = xlrd.open_workbook(file_contents=sensorsExcelBlob.read())
        sensors_dataframe_dirty = pd.read_excel(sensors_excel)
    except Exception as e:
        logging.error(f"Cannot read Excel file: {e}")

    N = 4
    sensors_dataframe_dirty.drop(index=sensors_dataframe_dirty.index[:N], inplace=True)

    timestamp_tmp = sensors_dataframe_dirty.iloc[:, 0]
    timestamp_tmp.dropna(inplace=True)
    timestamp_tmp = pd.DataFrame(timestamp_tmp.values, columns={'timestamp_org'})
    number_measures = timestamp_tmp.shape[0]

    sensors_dataframe_dirty.drop(sensors_dataframe_dirty.columns[0], axis=1, inplace=True)
    sensors_dataframe_dirty.reset_index(drop=True, inplace=True)

    for key, value in sensors_dataframe_dirty.iteritems():
        sensor_info_tmp = value.iloc[0]

        try:
            site_tmp = re.findall('//(.*)/\d', str(sensor_info_tmp))
            site_tmp = ''.join(site_tmp)
            sensor_info_tmp_list = sensor_info_tmp.split(' - ')[1].split()
            unit_of_measurement_tmp = sensor_info_tmp_list[-1]
            sensor_name_tmp_list = sensor_info_tmp_list[0].split('_')
            sensor_id_tmp = sensor_name_tmp_list[0]
            room_no_tmp = sensor_name_tmp_list[2]
            sensor_name_tmp = '_'.join(sensor_name_tmp_list[1:])
        except Exception as e:
            logging.error(f"Cannot process the sensors data, ERROR: {e}")

        sensor_measure_tmp = value.iloc[1:]

        try:
            sensor_measure_tmp = sensor_measure_tmp.apply(lambda x: str(x).encode('ascii', 'ignore').decode().replace(',', '.')).astype(float)
        except Exception as e:
            logging.error(f"Sensors value, ERROR: {e}")

        data_tmp = {
            'sensor_id': np.repeat(sensor_id_tmp, int(number_measures)),
            'sensor_measure': sensor_measure_tmp.values,
            'sensor_name':  np.repeat(sensor_name_tmp, int(number_measures)),
            'unit_of_measurement': np.repeat(unit_of_measurement_tmp, int(number_measures)),
            'room_no': np.repeat(room_no_tmp, int(number_measures)),
            'site': np.repeat(site_tmp, int(number_measures))
        }

        sensor_dataframe_clean = pd.DataFrame(data_tmp)

        sensor_dataframe_clean = pd.concat([sensor_dataframe_clean, timestamp_tmp], axis=1)

        date = datetime.now().strftime("%Y_%m_%d-%I:%M:%S_%p")

        sensor_filename_tmp = sensor_id_tmp + '_' + date + '.csv'


        upload_csv_blob(sensor_filename_tmp, sensor_dataframe_clean)
