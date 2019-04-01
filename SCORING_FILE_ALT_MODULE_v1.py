

import pandas as pd
import numpy as np
import json
from pprint import pprint
import sys, getopt
import csv
import json
import gzip
import os
import time
import datetime


indir = 'Z:/onsitereportservice/mom'

time_now_actual = (pd.to_datetime('now') + pd.to_timedelta(5, unit='h') + pd.to_timedelta(30, unit='m')  ).to_datetime() 

time_now = (pd.to_datetime('now') ).to_datetime()
time_l12 = (pd.to_datetime('now') -  pd.to_timedelta(12, unit='h') ).to_datetime()



if time_now_actual.month < 10:
    month_now_actual = "0" + str(time_now_actual.month) 
else:
    month_now_actual = str(time_now_actual.month)
    
if time_now_actual.day < 10:
    day_now_actual = "0" + str(time_now_actual.day) 
else:
    day_now_actual = str(time_now_actual.day)     
    
if time_now_actual.hour < 10:
    hour_now_actual = "0" + str(time_now_actual.hour) 
else:
    hour_now_actual = str(time_now_actual.hour)     



time_now_l1 = time_now_actual - pd.to_timedelta(1, unit='h')
time_now_l2 = time_now_actual - pd.to_timedelta(2, unit='h')
time_now_l3 = time_now_actual - pd.to_timedelta(3, unit='h')



if time_now_l1.month < 10:
    time_now_l1 = "0" + str(time_now_l1.month) 
else:
    month_now_l1 = str(time_now_l1.month)
    
if time_now_l1.day < 10:
    day_now_l1 = "0" + str(time_now_l1.day) 
else:
    day_now_l1 = str(time_now_l1.day)     
    
if time_now_l1.hour < 10:
    hour_now_l1 = "0" + str(time_now_l1.hour) 
else:
    hour_now_l1 = str(time_now_l1.hour)     



if time_now_l2.month < 10:
    time_now_l2 = "0" + str(time_now_l2.month) 
else:
    month_now_l2 = str(time_now_l2.month)
    
if time_now_l2.day < 10:
    day_now_l2 = "0" + str(time_now_l2.day) 
else:
    day_now_l2 = str(time_now_l2.day)     
    
if time_now_l2.hour < 10:
    hour_now_l2 = "0" + str(time_now_l2.hour) 
else:
    hour_now_l2 = str(time_now_l2.hour)     



datefield_l1 = str(time_now_l1.year) + "_" + str(month_now_l1) + str(day_now_l1) + "_" + str(hour_now_l1) 
datefield_l2 = str(time_now_l2.year) + "_" + str(month_now_l2) + str(day_now_l2) + "_" + str(hour_now_l2) 



if time_now.month < 10:
    month_now = "0" + str(time_now.month) 
else:
    month_now = str(time_now.month)
    
if time_now.day < 10:
    day_now = "0" + str(time_now.day) 
else:
    day_now = str(time_now.day)     
    
if time_now.hour < 10:
    hour_now = "0" + str(time_now.hour) 
else:
    hour_now = str(time_now.hour)     



file_name = "C:/Users/hotsdt/Desktop/SCORING_CODE/RAW_DATA/" + str(time_now_actual.year) + "_" + str(month_now_actual) + str(day_now_actual) + "_" + str(hour_now_actual) + 'rolling3_hrs.csv'



datefield = str(time_now.year) + "_" + str(month_now) + str(day_now) + "_" + str(hour_now) 


datefield_actual = str(time_now_actual.year) + "_" + str(month_now_actual) + str(day_now_actual) + "_" + str(hour_now_actual) 



if time_l12.month < 10:
    month_l12 = "0" + str(time_l12.month) 
else:
    month_l12 = str(time_l12.month)
    
if time_l12.day < 10:
    day_l12 = "0" + str(time_l12.day) 
else:
    day_l12 = str(time_l12.day)     


indir2 = indir  + "/" + str(time.strftime("%Y") ) + month_l12 + "/" + day_l12



indir3 = indir  + "/" + str(time.strftime("%Y") ) + month_now + "/" + day_now 




content1_write = open(file_name, 'w', newline= '')
csvwriter = csv.writer(content1_write)

cntfile = 0



# running the loop below to iterate over each folder and append the data in a csv file:
for root, dirs, filenames in os.walk(indir3):
    count = 0

    for f in filenames:
        cntfile+=1
        print(cntfile, end = "\r")
        with gzip.open(os.path.join(root, f), 'r') as f2:
            file_content_new = f2.read()
            file_content_new2 = pd.io.json.loads(file_content_new)
            content_file1 = file_content_new2['Measurements'].copy()
            for i in range(len(content_file1)):
                for j in content_file1[i]['Samples']:
                    j['SignalName'] = content_file1[i]['SignalName']
                    j['TimeStamp'] = j['TimeStamp'].replace("T", " ")
                    j['Device'] = file_content_new2['Device']
                    if count == 0:
                        header = j.keys()
                        csvwriter.writerow(header) 
                        count += 1
                    csvwriter.writerow(j.values())


if indir3!= indir2:
    for root, dirs, filenames in os.walk(indir2):

        for f in filenames:
            cntfile+=1
            print(cntfile, end = "\r")
            with gzip.open(os.path.join(root, f), 'r') as f2:
                file_content_new = f2.read()
                file_content_new2 = pd.io.json.loads(file_content_new)
                content_file1 = file_content_new2['Measurements'].copy()
                for i in range(len(content_file1)):
                    for j in content_file1[i]['Samples']:
                        #if file_content_new2['Device']
                        j['SignalName'] = content_file1[i]['SignalName']
                        j['TimeStamp'] = j['TimeStamp'].replace("T", " ")
                        j['Device'] = file_content_new2['Device']
                        if count == 0:
                            header = j.keys()
                            csvwriter.writerow(header) 
                            count += 1
                        csvwriter.writerow(j.values())


content1_write.close()


df_all = pd.read_csv(file_name)



f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('292')

f.close()

indir = 'Z:/onsitereportservice/mom'



df_all = df_all.drop_duplicates()





df_all['Device'].value_counts()



sandvik_trucks = ['T663D110','T663D113','T663D118','T663D120','T663D143','T763D173','T763D174','T763D131','T763D151']



df_all = df_all[df_all['Device'].isin(sandvik_trucks) ]




df_all['TimeStamp'] = pd.to_datetime(df_all['TimeStamp'], errors = 'coerce' )
df_all['TimeStamp_n'] = pd.to_datetime(df_all['TimeStamp'].apply(lambda x: x.replace(second=0) )  )

df_all['TimeRank'] = df_all.groupby(['Device','SignalName','TimeStamp_n'])['TimeStamp'].rank(ascending = False)

df_all_filtered = df_all[df_all['TimeRank']==1]
del df_all



df_all_filtered = df_all_filtered[(df_all_filtered['TimeStamp']>= time_l12) & (df_all_filtered['TimeStamp']<= time_now)]   



df_all_filtered['TimeStamp'] = pd.to_datetime(df_all_filtered['TimeStamp'] + pd.to_timedelta(5, unit='h') + pd.to_timedelta(30, unit='m') )


df_all_filtered = df_all_filtered[df_all_filtered['Device'].isin(sandvik_trucks) ]



df_all_filtered['Value'] =  df_all_filtered['Value'].astype(float)

df_all_filtered2 = df_all_filtered.pivot_table(index = ['TimeStamp_n','Device' ], columns = 'SignalName', values = 'Value')

df_all_filtered2.reset_index(level=0, inplace=True)

df_all_filtered2['Device'] = df_all_filtered2.index 

df_all_filtered3 = df_all_filtered2[df_all_filtered2['Engine RPM']>=1500] 

df_all_filtered3['TimeStamp_h'] = pd.to_datetime(df_all_filtered3['TimeStamp_n'].apply(lambda x: x.replace(minute=0) )  )


df_all_filtered3 = df_all_filtered3.sort_values(['Device', 'TimeStamp_n'], ascending = [1,1] )



df_all_new_oct_hr7 = df_all_filtered3.copy()


df_all_new_oct_hr7['final_chunk2'] =  df_all_new_oct_hr7['Device']



df_all_new_oct_hr7 = df_all_new_oct_hr7.sort_values(['Device', 'TimeStamp_n'], ascending = [1,1] )


df_oct_groupby = pd.DataFrame(df_all_new_oct_hr7.groupby('final_chunk2')['TimeStamp_n'].count())


df_oct_groupby.columns = ['TimeStamp_count']


df_oct_groupby['final_chunk2'] = df_oct_groupby.index



df_oct2 = pd.merge(df_all_new_oct_hr7[df_all_new_oct_hr7['Engine RPM']>=1500] , df_oct_groupby[df_oct_groupby['TimeStamp_count']>=61], how = 'inner', on = 'final_chunk2' )



df_oct2['TimeStamp_n'] = pd.to_datetime(df_oct2['TimeStamp_n'])



df_oct4 = df_oct2.melt(id_vars = ['Device', 'final_chunk2','TimeStamp_n'] )



df_oct4 = df_oct4.sort_values(['Device', 'final_chunk2', 'variable', 'TimeStamp_n'], ascending = [1,1,1,1] )



df_oct4['TimeStamp_n'] = pd.to_datetime(df_oct4['TimeStamp_n'])


df_oct4['TimeRank'] = df_oct4.groupby(['Device','final_chunk2','variable' ])['TimeStamp_n'].rank(ascending = True)


df_oct5 = df_oct4.groupby(['final_chunk2','variable'], as_index = False ).fillna(method='ffill')


df_oct5 = df_oct5.sort_values(['Device', 'final_chunk2', 'variable', 'TimeStamp_n'], ascending = [1,1,1,1] )


df_oct6 = df_oct5.copy()


df_oct6['rolling_15_val_mean'] = df_oct6.groupby(['final_chunk2','variable'])['value'].apply(lambda x:x.rolling(center=False,window=15).mean())



def rollupflag(df):
    if df['TimeStamp_n'] <= time_l12 + pd.to_timedelta(3, unit='h') :
        return "1"
    elif df['TimeStamp_n'] <= time_l12 + pd.to_timedelta(6, unit='h') :
        return "2"
    elif df['TimeStamp_n'] <= time_l12 + pd.to_timedelta(9, unit='h') :
        return "3"
    else:
        return "4"



df_oct6['RollupFlag'] = df_oct6.apply(rollupflag, axis = 1)



df_oct6['ColumnNames'] = df_oct6['variable'].astype(str) + df_oct6['RollupFlag'].astype(str)



imp_var = [ 'Ambient Temperature',	'Box and Steering Pump Pressure',	'Brake Circuit Charging Pressure',	'Brake Hydraulic Oil Temperature',	'Converter Lock Up',	'Dropbox Oil Temperature',	'Engine Coolant Temperature',	'Engine Cooler Pump Pressure',	'Engine Fuel Rate',	'Engine Intake Manifold Pressure',	'Engine Intake Manifold Temperature',	'Engine Load',	'Engine Oil Pressure',	'Engine Oil Temperature',	'Engine RPM',	'Engine Torque',	'Front Axle Brake Pressure',	'Hydraulic Oil Temperature',	'Machine Speed',	'Rear Axle Brake Pressure',	'Throttle Request',	'Transmission Oil Pressure',	'Transmission Oil Temperature',	'Transmission Retarder Active',	'Transmission Retarder Control',	'Transmission Retarder Output Oil Temperature','Upbox Oil Temperature'    ]



df_oct6_impvar = df_oct6[df_oct6['variable'].isin(imp_var)]



df_oct6_impvar['rolling_15_val_mean'] = df_oct6_impvar['rolling_15_val_mean'].astype(float) 



df_oct6_impvar2 = df_oct6_impvar.pivot_table(index = ['Device', 'final_chunk2'],  columns = 'ColumnNames', values = 'rolling_15_val_mean', aggfunc=np.mean )



df_oct6_impvar2.reset_index(level=0, inplace=True)



df_oct6_impvar2['Device2'] = df_oct6_impvar2['Device']


del df_oct6_impvar2['Device'] 



df_oct6_impvar2['Avg_Ambient_Temperature1'] =  					df_oct6_impvar2.iloc[:, 0:4].mean(axis=1)
df_oct6_impvar2['Avg_Ambient_Temperature2'] =  					df_oct6_impvar2.iloc[:, 1:4].mean(axis=1)
df_oct6_impvar2['Avg_Ambient_Temperature3'] =  					df_oct6_impvar2.iloc[:, 2:4].mean(axis=1)
df_oct6_impvar2['Avg_Ambient_Temperature4'] =  					df_oct6_impvar2.iloc[:, 3:4].mean(axis=1)
df_oct6_impvar2['Avg_Box_and_Steering_Pump_Pressure1'] =  					df_oct6_impvar2.iloc[:, 4:8].mean(axis=1)
df_oct6_impvar2['Avg_Box_and_Steering_Pump_Pressure2'] =  					df_oct6_impvar2.iloc[:, 5:8].mean(axis=1)
df_oct6_impvar2['Avg_Box_and_Steering_Pump_Pressure3'] =  					df_oct6_impvar2.iloc[:, 6:8].mean(axis=1)
df_oct6_impvar2['Avg_Box_and_Steering_Pump_Pressure4'] =  					df_oct6_impvar2.iloc[:, 7:8].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Circuit_Charging_Pressure1'] =  					df_oct6_impvar2.iloc[:, 8:12].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Circuit_Charging_Pressure2'] =  					df_oct6_impvar2.iloc[:, 9:12].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Circuit_Charging_Pressure3'] =  					df_oct6_impvar2.iloc[:, 10:12].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Circuit_Charging_Pressure4'] =  					df_oct6_impvar2.iloc[:, 11:12].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Hydraulic_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 12:16].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Hydraulic_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 13:16].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Hydraulic_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 14:16].mean(axis=1)
df_oct6_impvar2['Avg_Brake_Hydraulic_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 15:16].mean(axis=1)
df_oct6_impvar2['Avg_Converter_Lock_Up1'] =  					df_oct6_impvar2.iloc[:, 16:20].mean(axis=1)
df_oct6_impvar2['Avg_Converter_Lock_Up2'] =  					df_oct6_impvar2.iloc[:, 17:20].mean(axis=1)
df_oct6_impvar2['Avg_Converter_Lock_Up3'] =  					df_oct6_impvar2.iloc[:, 18:20].mean(axis=1)
df_oct6_impvar2['Avg_Converter_Lock_Up4'] =  					df_oct6_impvar2.iloc[:, 19:20].mean(axis=1)
df_oct6_impvar2['Avg_Dropbox_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 20:24].mean(axis=1)
df_oct6_impvar2['Avg_Dropbox_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 21:24].mean(axis=1)
df_oct6_impvar2['Avg_Dropbox_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 22:24].mean(axis=1)
df_oct6_impvar2['Avg_Dropbox_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 23:24].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Coolant_Temperature1'] =  					df_oct6_impvar2.iloc[:, 24:28].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Coolant_Temperature2'] =  					df_oct6_impvar2.iloc[:, 25:28].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Coolant_Temperature3'] =  					df_oct6_impvar2.iloc[:, 26:28].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Coolant_Temperature4'] =  					df_oct6_impvar2.iloc[:, 27:28].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Cooler_Pump_Pressure1'] =  					df_oct6_impvar2.iloc[:, 28:32].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Cooler_Pump_Pressure2'] =  					df_oct6_impvar2.iloc[:, 29:32].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Cooler_Pump_Pressure3'] =  					df_oct6_impvar2.iloc[:, 30:32].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Cooler_Pump_Pressure4'] =  					df_oct6_impvar2.iloc[:, 31:32].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Fuel_Rate1'] =  					df_oct6_impvar2.iloc[:, 32:36].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Fuel_Rate2'] =  					df_oct6_impvar2.iloc[:, 33:36].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Fuel_Rate3'] =  					df_oct6_impvar2.iloc[:, 34:36].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Fuel_Rate4'] =  					df_oct6_impvar2.iloc[:, 35:36].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Pressure1'] =  					df_oct6_impvar2.iloc[:, 36:40].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Pressure2'] =  					df_oct6_impvar2.iloc[:, 37:40].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Pressure3'] =  					df_oct6_impvar2.iloc[:, 38:40].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Pressure4'] =  					df_oct6_impvar2.iloc[:, 39:40].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Temperature1'] =  					df_oct6_impvar2.iloc[:, 40:44].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Temperature2'] =  					df_oct6_impvar2.iloc[:, 41:44].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Temperature3'] =  					df_oct6_impvar2.iloc[:, 42:44].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Intake_Manifold_Temperature4'] =  					df_oct6_impvar2.iloc[:, 43:44].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Load1'] =  					df_oct6_impvar2.iloc[:, 44:48].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Load2'] =  					df_oct6_impvar2.iloc[:, 45:48].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Load3'] =  					df_oct6_impvar2.iloc[:, 46:48].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Load4'] =  					df_oct6_impvar2.iloc[:, 47:48].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Pressure1'] =  					df_oct6_impvar2.iloc[:, 48:52].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Pressure2'] =  					df_oct6_impvar2.iloc[:, 49:52].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Pressure3'] =  					df_oct6_impvar2.iloc[:, 50:52].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Pressure4'] =  					df_oct6_impvar2.iloc[:, 51:52].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 52:56].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 53:56].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 54:56].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 55:56].mean(axis=1)
df_oct6_impvar2['Avg_Engine_RPM1'] =  					df_oct6_impvar2.iloc[:, 56:60].mean(axis=1)
df_oct6_impvar2['Avg_Engine_RPM2'] =  					df_oct6_impvar2.iloc[:, 57:60].mean(axis=1)
df_oct6_impvar2['Avg_Engine_RPM3'] =  					df_oct6_impvar2.iloc[:, 58:60].mean(axis=1)
df_oct6_impvar2['Avg_Engine_RPM4'] =  					df_oct6_impvar2.iloc[:, 59:60].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Torque1'] =  					df_oct6_impvar2.iloc[:, 60:64].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Torque2'] =  					df_oct6_impvar2.iloc[:, 61:64].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Torque3'] =  					df_oct6_impvar2.iloc[:, 62:64].mean(axis=1)
df_oct6_impvar2['Avg_Engine_Torque4'] =  					df_oct6_impvar2.iloc[:, 63:64].mean(axis=1)
df_oct6_impvar2['Avg_Front_Axle_Brake_Pressure1'] =  					df_oct6_impvar2.iloc[:, 64:68].mean(axis=1)
df_oct6_impvar2['Avg_Front_Axle_Brake_Pressure2'] =  					df_oct6_impvar2.iloc[:, 65:68].mean(axis=1)
df_oct6_impvar2['Avg_Front_Axle_Brake_Pressure3'] =  					df_oct6_impvar2.iloc[:, 66:68].mean(axis=1)
df_oct6_impvar2['Avg_Front_Axle_Brake_Pressure4'] =  					df_oct6_impvar2.iloc[:, 67:68].mean(axis=1)
df_oct6_impvar2['Avg_Hydraulic_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 68:72].mean(axis=1)
df_oct6_impvar2['Avg_Hydraulic_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 69:72].mean(axis=1)
df_oct6_impvar2['Avg_Hydraulic_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 70:72].mean(axis=1)
df_oct6_impvar2['Avg_Hydraulic_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 71:72].mean(axis=1)
df_oct6_impvar2['Avg_Machine_Speed1'] =  					df_oct6_impvar2.iloc[:, 72:76].mean(axis=1)
df_oct6_impvar2['Avg_Machine_Speed2'] =  					df_oct6_impvar2.iloc[:, 73:76].mean(axis=1)
df_oct6_impvar2['Avg_Machine_Speed3'] =  					df_oct6_impvar2.iloc[:, 74:76].mean(axis=1)
df_oct6_impvar2['Avg_Machine_Speed4'] =  					df_oct6_impvar2.iloc[:, 75:76].mean(axis=1)
df_oct6_impvar2['Avg_Rear_Axle_Brake_Pressure1'] =  					df_oct6_impvar2.iloc[:, 76:80].mean(axis=1)
df_oct6_impvar2['Avg_Rear_Axle_Brake_Pressure2'] =  					df_oct6_impvar2.iloc[:, 77:80].mean(axis=1)
df_oct6_impvar2['Avg_Rear_Axle_Brake_Pressure3'] =  					df_oct6_impvar2.iloc[:, 78:80].mean(axis=1)
df_oct6_impvar2['Avg_Rear_Axle_Brake_Pressure4'] =  					df_oct6_impvar2.iloc[:, 79:80].mean(axis=1)
df_oct6_impvar2['Avg_Throttle_Request1'] =  					df_oct6_impvar2.iloc[:, 80:84].mean(axis=1)
df_oct6_impvar2['Avg_Throttle_Request2'] =  					df_oct6_impvar2.iloc[:, 81:84].mean(axis=1)
df_oct6_impvar2['Avg_Throttle_Request3'] =  					df_oct6_impvar2.iloc[:, 82:84].mean(axis=1)
df_oct6_impvar2['Avg_Throttle_Request4'] =  					df_oct6_impvar2.iloc[:, 83:84].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Pressure1'] =  					df_oct6_impvar2.iloc[:, 84:88].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Pressure2'] =  					df_oct6_impvar2.iloc[:, 85:88].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Pressure3'] =  					df_oct6_impvar2.iloc[:, 86:88].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Pressure4'] =  					df_oct6_impvar2.iloc[:, 87:88].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 88:92].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 89:92].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 90:92].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 91:92].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Active1'] =  					df_oct6_impvar2.iloc[:, 92:96].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Active2'] =  					df_oct6_impvar2.iloc[:, 93:96].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Active3'] =  					df_oct6_impvar2.iloc[:, 94:96].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Active4'] =  					df_oct6_impvar2.iloc[:, 95:96].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Control1'] =  					df_oct6_impvar2.iloc[:, 96:100].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Control2'] =  					df_oct6_impvar2.iloc[:, 97:100].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Control3'] =  					df_oct6_impvar2.iloc[:, 98:100].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Control4'] =  					df_oct6_impvar2.iloc[:, 99:100].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Output_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 100:104].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Output_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 101:104].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Output_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 102:104].mean(axis=1)
df_oct6_impvar2['Avg_Transmission_Retarder_Output_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 103:104].mean(axis=1)

df_oct6_impvar2['Avg_Upbox_Oil_Temperature1'] =  					df_oct6_impvar2.iloc[:, 104:108].mean(axis=1)
df_oct6_impvar2['Avg_Upbox_Oil_Temperature2'] =  					df_oct6_impvar2.iloc[:, 105:108].mean(axis=1)
df_oct6_impvar2['Avg_Upbox_Oil_Temperature3'] =  					df_oct6_impvar2.iloc[:, 106:108].mean(axis=1)
df_oct6_impvar2['Avg_Upbox_Oil_Temperature4'] =  					df_oct6_impvar2.iloc[:, 107:108].mean(axis=1)




df_oct6_impvar2.to_csv('C:/Users/hotsdt/Desktop/SCORING_CODE/INPUT_DATA_ALL/df_input_' +  datefield_actual + 'rolling_3hrs.csv')



list_event_history_signals = ['AlarmActiveTime',	'AlarmCount',	'AlarmInstanceCount',	'AlarmLimit',	'AlarmOperatorName',	'AlarmStartTime',	'Description',	'IsHighLimit',	'LimitValueState',	'TimeStamp',	'Title',	'WarningActiveTime',	'WarningCount',	'WarningInstanceCount',	'WarningLimit',	'WarningOperatorName',	'WarningStartTime']



list_event_history_signals2 = ['Device', 'Title',	'AlarmInstanceCount',	'TimeStamp']




file_alerts = "C:/Users/hotsdt/Desktop/SCORING_CODE/RAW_DATA/Raw_Alerts_" + datefield_actual  + "rolling_3hrs.csv" 


# /NEW DATA UTC TIMZONE/Alerts Data/

# Appending all the data for the month of August
content1_write = open(file_alerts, 'w', newline= '')  
csvwriter = csv.writer(content1_write)

cntfile = 0

list_all_alert_col = list_event_history_signals2

list_all_alert_col.append('Device')



#indir = 'C:/Users/ranglani hardev/Desktop/HZL/DATA/Raw Data/COPY AGAIN/BCG/201710/25/T663D118'
# location of the path where the raw json files are to be kept

count = 0


list_timestamp_var = ['WarningStartTime', 'TimeStamp', 'AlarmStartTime' ]

# running the loop below to iterate over each folder and append the data in a csv file:
for root, dirs, filenames in os.walk(indir2):
    for f in filenames:
        cntfile+=1
        print(cntfile, end = "\r")
        with gzip.open(os.path.join(root, f), 'r') as f2:
            file_content_new = f2.read()
            file_content_new2 = pd.io.json.loads(file_content_new)
            content_file1 = file_content_new2['AlertHistory'].copy()
            deviceid = file_content_new2['Device']
            dict1 = {}
            for i in range(len(content_file1)):
                if len(content_file1) > 0 :
                    
                    dict1 = {}

                    for j in list_all_alert_col:
                        if j in  content_file1[i] :
                            if j in list_timestamp_var:
                                dict1[j] = content_file1[i][j].replace("T", " ")
                            else :
                                dict1[j] = content_file1[i][j]
                        elif j == 'Device':
                            dict1[j] = deviceid
                        elif j == 'Filename':
                            dict1[j] = f

                        else :
                            dict1[j] = ""
    #                    dict['Device'].append(deviceid)


                if count == 0:
                    header = dict1.keys()
                    csvwriter.writerow(list_all_alert_col) 
                    count += 1
                csvwriter.writerow(dict1.values())

                
for root, dirs, filenames in os.walk(indir3):
    for f in filenames:
        cntfile+=1
        print(cntfile, end = "\r")
        with gzip.open(os.path.join(root, f), 'r') as f2:
            file_content_new = f2.read()
            file_content_new2 = pd.io.json.loads(file_content_new)
            content_file1 = file_content_new2['AlertHistory'].copy()
            deviceid = file_content_new2['Device']
            dict1 = {}
            for i in range(len(content_file1)):
                if len(content_file1) > 0 :
                    
                    dict1 = {}

                    for j in list_all_alert_col:
                        if j in  content_file1[i] :
                            if j in list_timestamp_var:
                                dict1[j] = content_file1[i][j].replace("T", " ")
                            else :
                                dict1[j] = content_file1[i][j]
                        elif j == 'Device':
                            dict1[j] = deviceid
                        elif j == 'Filename':
                            dict1[j] = f

                        else :
                            dict1[j] = ""
    #                    dict['Device'].append(deviceid)


                if count == 0:
                    header = dict1.keys()
                    csvwriter.writerow(list_all_alert_col) 
                    count += 1
                csvwriter.writerow(dict1.values())
                
                
                
content1_write.close()




df_alerts = pd.read_csv(file_alerts) 




df_alerts['TimeStamp'] = pd.to_datetime(df_alerts['TimeStamp'])



df_alerts = df_alerts[(df_alerts['TimeStamp']<=pd.to_datetime(time_now))&(df_alerts['TimeStamp']>=pd.to_datetime(time_now)- pd.to_timedelta(6, unit='h') )]




df_alerts = df_alerts.drop_duplicates()



df_alerts = df_alerts[df_alerts['Device'].isin(sandvik_trucks) ]




df_alarmtype = pd.read_excel('C:/Users/hotsdt/Desktop/SCORING_CODE/AlarmType_Categorization.xlsx')




df_alerts2 = pd.merge(df_alerts, df_alarmtype, how = 'left', on = 'Title' )



df_alerts3 = df_alerts2.groupby(['Device', 'AlarmType'] , as_index=False).agg({'AlarmInstanceCount': 'sum'} )



df_alerts4 = df_alerts3.pivot_table(index = ['Device'], columns = 'AlarmType', values = 'AlarmInstanceCount')



df_alerts4['Device'] = df_alerts4.index 


df_alerts4 = df_alerts4.reset_index(drop=True)


list_all_alert_col = ['Brake',	'Electrical',	'Engine_Others',	'Engine',	'Pressure',	'Sensor',	'Temperature',	'TM',	'TM_Others',	'Weighing',	'Others']



for i in list_all_alert_col:
    if i not in df_alerts4.columns.tolist():
        df_alerts4[i] = 0.0




df_alerts4 = df_alerts4.fillna(0)



df_alerts4['Brake_bin'] = df_alerts4['Brake'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Electrical_bin'] = df_alerts4['Electrical'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Engine_bin'] = df_alerts4['Engine'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Engine_Others_bin'] = df_alerts4['Engine_Others'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Others_bin'] = df_alerts4['Others'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Pressure_bin'] = df_alerts4['Pressure'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Sensor_bin'] = df_alerts4['Sensor'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Temperature_bin'] = df_alerts4['Temperature'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['TM_bin'] = df_alerts4['TM'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['TM_Others_bin'] = df_alerts4['TM_Others'].apply(lambda x: 1 if x>0 else 0 )
df_alerts4['Weighing_bin'] = df_alerts4['Weighing'].apply(lambda x: 1 if x>0 else 0 )


# In[742]:

# In[743]:


df_oct6_impvar2['Device'] = df_oct6_impvar2.index


# In[744]:


df_oct6_impvar3 = pd.merge(df_oct6_impvar2, df_alerts4, how = 'left', on = 'Device')


# In[745]:


df_oct6_impvar3.to_csv('C:/Users/hotsdt/Desktop/SCORING_CODE/INPUT_DATA_ALL/df_input_' +  datefield_actual + 'rolling_3hrs.csv')

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('984')

f.close()




import os
os.environ['R_HOME'] = 'C:\\Users\\hotsdt\\Documents\\R\\R-3.4.2\\'
os.environ['R_USER'] = 'C:\\Users\hotsdt\Anaconda\Lib\site-packages\rpy2'


# In[748]:

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1004')

f.close()

import rpy2

import scipy as sp
from pandas import *
from rpy2.robjects.packages import importr
import rpy2.robjects as ro
import math, datetime

import rpy2.robjects as ro
from rpy2.robjects.packages import importr

import rpy2.robjects as robjects
from rpy2.robjects import pandas2ri


# In[749]:


from rpy2.robjects.packages import importr
utils = importr('utils')


# In[750]:


svm =  importr('e1071')  


# In[751]:


nnet = importr('nnet')


# In[752]:


from rpy2.robjects.packages import importr
utils = importr('utils')




randomForest=importr('randomForest')
# nnnet = importr('nnet')


# In[757]:


df_oct6_impvar3 = df_oct6_impvar3.fillna(0)


# In[758]:


df_oct6_impvar3.to_csv('C:/Users/hotsdt/Desktop/SCORING_CODE/WORKING_FOLDER/df_scoring_data_temp_rolling_3hrs.csv')

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1087')

f.close()

# In[759]:


robjects.r('''
                       
            TM_nnet = readRDS("C:/Users/hotsdt/Desktop/SCORING_CODE/FINAL_MODEL_FILES/Transmission_RF_15Nov.rds")
            
            ENG_rf = readRDS("C:/Users/hotsdt/Desktop/SCORING_CODE/FINAL_MODEL_FILES/Engine_RF_v6_NoFlags1.rds")
                        
            pred_var_engine =c( 'Avg_Box_and_Steering_Pump_Pressure1','Avg_Box_and_Steering_Pump_Pressure2',	                   'Avg_Brake_Circuit_Charging_Pressure1',	                   'Avg_Brake_Circuit_Charging_Pressure2',	                   'Avg_Brake_Hydraulic_Oil_Temperature1',	                   'Avg_Brake_Hydraulic_Oil_Temperature2',	                   'Avg_Converter_Lock_Up1',	                   'Avg_Converter_Lock_Up2',	                   'Avg_Dropbox_Oil_Temperature1',	                   'Avg_Dropbox_Oil_Temperature2',	                   'Avg_Engine_Cooler_Pump_Pressure1',	                   'Avg_Engine_Cooler_Pump_Pressure2',	                   'Avg_Engine_Intake_Manifold_Pressure1',	                   'Avg_Engine_Intake_Manifold_Pressure2',	                   'Avg_Engine_Intake_Manifold_Temperature1',	                   'Avg_Engine_Intake_Manifold_Temperature2',	                   'Avg_Engine_Oil_Pressure1',	                   'Avg_Engine_Oil_Pressure2',	                   'Avg_Engine_Oil_Temperature1',	                   'Avg_Engine_Oil_Temperature2',	                   'Avg_Front_Axle_Brake_Pressure1',	                   'Avg_Front_Axle_Brake_Pressure2',	                   'Avg_Hydraulic_Oil_Temperature1',	                   'Avg_Hydraulic_Oil_Temperature2',	                   'Avg_Rear_Axle_Brake_Pressure1',	                   'Avg_Rear_Axle_Brake_Pressure2',	                   'Avg_Transmission_Oil_Pressure1',	                   'Avg_Transmission_Oil_Pressure2',	                   'Avg_Transmission_Retarder_Active1',	                   'Avg_Transmission_Retarder_Active2',	                   'Avg_Transmission_Retarder_Control1',	                   'Avg_Transmission_Retarder_Control2',	                   'Avg_Transmission_Retarder_Output_Oil_Temperature1',	                   'Avg_Transmission_Retarder_Output_Oil_Temperature2',	                   'Avg_Upbox_Oil_Temperature1',	                   'Avg_Upbox_Oil_Temperature2',	                   'Brake',	                   'Electrical',	                   'Engine',	                   'Engine_Others',	                   'Others',	                   'Pressure',	                   'Sensor',	                   'Temperature',	                   'TM',	                   'TM_Others',	                   'Weighing')
            
            pred_var_trans = pred_var_trans = c('Avg_Box_and_Steering_Pump_Pressure1',   'Avg_Box_and_Steering_Pump_Pressure2',                    'Avg_Brake_Circuit_Charging_Pressure1',                    'Avg_Brake_Circuit_Charging_Pressure2',                    'Avg_Brake_Hydraulic_Oil_Temperature1',                    'Avg_Brake_Hydraulic_Oil_Temperature2',                    'Avg_Converter_Lock_Up1',                    'Avg_Converter_Lock_Up2',                    'Avg_Dropbox_Oil_Temperature1',                    'Avg_Dropbox_Oil_Temperature2',                    'Avg_Engine_Cooler_Pump_Pressure1',                    'Avg_Engine_Cooler_Pump_Pressure2',                    'Avg_Engine_Intake_Manifold_Pressure1',                    'Avg_Engine_Intake_Manifold_Pressure2',                    'Avg_Engine_Intake_Manifold_Temperature1',                    'Avg_Engine_Intake_Manifold_Temperature2',                    'Avg_Engine_Oil_Pressure1',                    'Avg_Engine_Oil_Pressure2',                    'Avg_Engine_Oil_Temperature1',                    'Avg_Engine_Oil_Temperature2',                    'Avg_Front_Axle_Brake_Pressure1',                    'Avg_Front_Axle_Brake_Pressure2',                    'Avg_Hydraulic_Oil_Temperature1',                    'Avg_Hydraulic_Oil_Temperature2',                    'Avg_Rear_Axle_Brake_Pressure1',                    'Avg_Rear_Axle_Brake_Pressure2',                    'Avg_Transmission_Oil_Pressure1',                    'Avg_Transmission_Oil_Pressure2',                    'Avg_Transmission_Retarder_Active1',                    'Avg_Transmission_Retarder_Active2',                    'Avg_Transmission_Retarder_Control1',                    'Avg_Transmission_Retarder_Control2',                    'Avg_Transmission_Retarder_Output_Oil_Temperature1',                    'Avg_Transmission_Retarder_Output_Oil_Temperature2',                    'Avg_Upbox_Oil_Temperature1',                    'Avg_Upbox_Oil_Temperature2',                    'Brake',                    'Brake_bin',                    'Electrical',                    'Electrical_bin',                    'Engine',                    'Engine_bin',                    'Engine_Others',                    'Engine_Others_bin',                    'Others',                    'Others_bin',                    'Pressure',                    'Pressure_bin',                    'Sensor',                    'Sensor_bin',                    'Temperature',                    'Temperature_bin',                    'TM',                    'TM_bin',                    'TM_Others',                    'TM_Others_bin',                    'Weighing',                    'Weighing_bin')


            scoring_data <- read.csv(file="C:/Users/hotsdt/Desktop/SCORING_CODE/WORKING_FOLDER/df_scoring_data_temp_rolling_3hrs.csv",header = T)


           scoring_data$TM_BD_Prob <- predict(TM_nnet, newdata = scoring_data, type = "prob" )[,2]
            
           scoring_data$Engine_BD_Prob <- predict(ENG_rf, newdata = scoring_data, type = "prob" )[,2]
            
            write.csv(scoring_data, file = "C:/Users/hotsdt/Desktop/SCORING_CODE/WORKING_FOLDER/df_scoring_data_temp_rolling_3hrs.csv")
           ''')




f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1122')

f.close()

# In[760]:


df_scored = pd.read_csv('C:/Users/hotsdt/Desktop/SCORING_CODE/WORKING_FOLDER/df_scoring_data_temp_rolling_3hrs.csv')

# df_scoring_data_temp_rolling_3hrs

# df_scoring_data_temp_output_rolling_3hrs

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1135')

f.close()



# In[761]:


df_scored['Datefield'] = datefield_actual


# In[762]:


df_scored


# In[763]:


df_scored_email = df_scored[['Device','TM_BD_Prob', 'Engine_BD_Prob']]




# In[767]:


import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


import smtplib, os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import COMMASPACE, formatdate
from email import encoders


# In[768]:


msg = MIMEMultipart()
msg['Date'] = formatdate(localtime = True)
msg['Subject'] = "subject"
msg.attach(MIMEText("text"))


# In[769]:


df_scored_email = df_scored[['Device','TM_BD_Prob', 'Engine_BD_Prob']]


# In[770]:


df_scored_email


# In[771]:

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1228')

f.close()


df_scored_email['TM_BD_Prob'] = df_scored_email['TM_BD_Prob'].apply(lambda x : 0.8 if x > 0.8 else x )
df_scored_email['Engine_BD_Prob'] = df_scored_email['Engine_BD_Prob'].apply(lambda x : 0.8 if x > 0.8 else x )


# In[772]:


df_scored_email


# In[773]:


from IPython.display import HTML

# In[632]:


df_scored_email['TM_BD_PROB'] = np.round((df_scored_email['TM_BD_Prob']*100),0 )
df_scored_email['TM_BD_PROB'] = df_scored_email['TM_BD_PROB'].astype(int)
df_scored_email['TM_BD_PROB'] = df_scored_email['TM_BD_PROB'].astype(str) + "%"


# In[774]:


df_scored_email['ENGINE_BD_PROB'] = np.round((df_scored_email['Engine_BD_Prob']*100),0 )
df_scored_email['ENGINE_BD_PROB'] = df_scored_email['ENGINE_BD_PROB'].astype(int)
df_scored_email['ENGINE_BD_PROB'] = df_scored_email['ENGINE_BD_PROB'].astype(str) + "%"


# In[775]:


df_scored_email


# In[776]:


df_scored_email.rename(columns = {'Device':'DEVICE'}, inplace = True)


# In[777]:


device_list = ['T663D143', 'T663D110','T663D113','T663D118','T663D120','T763D173','T763D174','T763D131','T763D151' ]


# In[778]:


device_data = pd.DataFrame(data = device_list, columns = ['DEVICE'])


# In[779]:


email_data1 = pd.merge(device_data,df_scored_email[['DEVICE','TM_BD_PROB', 'ENGINE_BD_PROB']] , how = 'left', on = 'DEVICE' )


# In[780]:


email_data1['TM_BD_PROB'] = email_data1['TM_BD_PROB'].fillna('Current Data Insufficient')
email_data1['ENGINE_BD_PROB'] = email_data1['ENGINE_BD_PROB'].fillna('Current Data Insufficient')


# In[781]:


email_data1['DateField'] = datefield_actual


# In[782]:


email_data1

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1314')

f.close()


# In[783]:


with open('C:/Users/hotsdt/Desktop/SCORING_CODE/OUTPUT_DATA_ALL/FINAL_ALL_SCORES_NEW.csv', 'a') as f:
    email_data1.to_csv(f, header = False, index = False)


f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1322')

f.close()

    
# In[784]:


# In[788]:


df_output_all = pd.read_csv('C:/Users/hotsdt/Desktop/SCORING_CODE/OUTPUT_DATA_ALL/FINAL_ALL_SCORES_NEW.csv')    

df_output_all_l1 = df_output_all[df_output_all['Datefield_actual']==datefield_l1] 
df_output_all_l2 = df_output_all[df_output_all['Datefield_actual']==datefield_l2]     


# In[789]:

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1352')

f.close()


# In[791]:


df_scored_to_store = email_data1.copy()


# In[794]:


df_scored_to_store['DateField'] = datefield_actual


# In[795]:


T663D110_score_TM = email_data1.iloc[0,1]
T663D113_score_TM = email_data1.iloc[1,1]
T663D118_score_TM = email_data1.iloc[2,1]
T663D120_score_TM = email_data1.iloc[3,1]
T663D143_score_TM = email_data1.iloc[4,1]
T763D173_score_TM = email_data1.iloc[5,1]
T763D174_score_TM = email_data1.iloc[6,1]
T763D131_score_TM = email_data1.iloc[7,1]
T763D151_score_TM = email_data1.iloc[8,1]


# In[796]:


T663D110_score_ENG = email_data1.iloc[0,2]
T663D113_score_ENG = email_data1.iloc[1,2]
T663D118_score_ENG = email_data1.iloc[2,2]
T663D120_score_ENG = email_data1.iloc[3,2]
T663D143_score_ENG = email_data1.iloc[4,2]
T763D173_score_ENG = email_data1.iloc[5,1]
T763D174_score_ENG = email_data1.iloc[6,1]
T763D131_score_ENG = email_data1.iloc[7,1]
T763D151_score_ENG = email_data1.iloc[8,1]


# In[797]:


T663D110_score_TM_l2 = df_output_all_l2.iloc[0,1]
T663D113_score_TM_l2 = df_output_all_l2.iloc[1,1]
T663D118_score_TM_l2 = df_output_all_l2.iloc[2,1]
T663D120_score_TM_l2 = df_output_all_l2.iloc[3,1]
T663D143_score_TM_l2 = df_output_all_l2.iloc[4,1]
T763D173_score_TM_l2 = df_output_all_l2.iloc[5,1]
T763D174_score_TM_l2 = df_output_all_l2.iloc[6,1]
T763D131_score_TM_l2 = df_output_all_l2.iloc[7,1]
T763D151_score_TM_l2 = df_output_all_l2.iloc[8,1]


# In[798]:


T663D110_score_ENG_l2 = df_output_all_l2.iloc[0,2]
T663D113_score_ENG_l2 = df_output_all_l2.iloc[1,2]
T663D118_score_ENG_l2 = df_output_all_l2.iloc[2,2]
T663D120_score_ENG_l2 = df_output_all_l2.iloc[3,2]
T663D143_score_ENG_l2 = df_output_all_l2.iloc[4,2]
T763D173_score_ENG_l2 = df_output_all_l2.iloc[5,2]
T763D174_score_ENG_l2 = df_output_all_l2.iloc[6,2]
T763D131_score_ENG_l2 = df_output_all_l2.iloc[7,2]
T763D151_score_ENG_l2 = df_output_all_l2.iloc[8,2]


# In[799]:


T663D110_score_TM_l1 = df_output_all_l1.iloc[0,1]
T663D113_score_TM_l1 = df_output_all_l1.iloc[1,1]
T663D118_score_TM_l1 = df_output_all_l1.iloc[2,1]
T663D120_score_TM_l1 = df_output_all_l1.iloc[3,1]
T663D143_score_TM_l1 = df_output_all_l1.iloc[4,1]
T763D173_score_TM_l1 = df_output_all_l1.iloc[5,1]
T763D174_score_TM_l1 = df_output_all_l1.iloc[6,1]
T763D131_score_TM_l1 = df_output_all_l1.iloc[7,1]
T763D151_score_TM_l1 = df_output_all_l1.iloc[8,1]


# In[800]:


T663D110_score_ENG_l1 = df_output_all_l1.iloc[0,2]
T663D113_score_ENG_l1 = df_output_all_l1.iloc[1,2]
T663D118_score_ENG_l1 = df_output_all_l1.iloc[2,2]
T663D120_score_ENG_l1 = df_output_all_l1.iloc[3,2]
T663D143_score_ENG_l1 = df_output_all_l1.iloc[4,2]
T763D173_score_ENG_l1 = df_output_all_l1.iloc[5,2]
T763D174_score_ENG_l1 = df_output_all_l1.iloc[6,2]
T763D131_score_ENG_l1 = df_output_all_l1.iloc[7,2]
T763D151_score_ENG_l1 = df_output_all_l1.iloc[8,2]


# In[801]:


time_now_n2 = time_now_actual + pd.to_timedelta(2, unit='h')
time_now_n3 = time_now_actual + pd.to_timedelta(3, unit='h')
time_now_n4 = time_now_actual + pd.to_timedelta(4, unit='h')

if time_now_n2.hour < 10:
    hour_now_n2 = "0" + str(time_now_n2.hour) 
else:
    hour_now_n2 = str(time_now_n2.hour)    


if time_now_n3.hour < 10:
    hour_now_n3 = "0" + str(time_now_n3.hour) 
else:
    hour_now_n3 = str(time_now_n3.hour)    


if time_now_n4.hour < 10:
    hour_now_n4 = "0" + str(time_now_n4.hour) 
else:
    hour_now_n4 = str(time_now_n4.hour)    



    
f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1497')

f.close()


# In[844]:




HTML(""" Engine Breakdown Probability Report:  
<style type="text/css">
	table.tableizer-table {
		font-size: 12px;
		text-align:center;
		border: 1px solid #CCC; 
		text-align:center;
		font-family: Arial, Helvetica, sans-serif;
	} 
	.tableizer-table td {
		padding: 4px;
		text-align:center;
		text-align:center;
		margin: 3px;
		border: 1px solid #CCC;
	}
	.tableizer-table th {
		background-color: #104E8B; 
		text-align:center;
		color: #FFF;
		font-weight: bold;
	}
</style>
<table class="tableizer-table">
<thead><tr class="tableizer-firstrow"> <th>Truck #</th><th>""" + hour_now_l2 + """00 hrs - """ + hour_now_n2 +"""00 hrs</th><th>""" +
     hour_now_l1 + """00 hrs - """ + hour_now_n3 + """00 hrs</th><th>""" + hour_now_actual + """00 hrs - """ + 
     hour_now_n4 + """00 hrs</th></tr></thead><tbody>
 <tr><td>LPDT 601 - T663D143</td><td>""" + T663D143_score_ENG_l2 +  """</td><td>""" + T663D143_score_ENG_l1 
     + """</td><td>""" + T663D143_score_ENG +"""</td></tr>
 <tr><td>LPDT 602 - T663D110</td><td>""" + T663D110_score_ENG_l2 + """</td><td>""" + T663D110_score_ENG_l1 
     + """</td><td>""" + T663D110_score_ENG +"""</td></tr>
 <tr><td>LPDT 603 - T663D113</td><td>""" + T663D113_score_ENG_l2 + """</td><td>""" + T663D113_score_ENG_l1 
     + """</td><td>""" + T663D113_score_ENG +"""</td></tr>
 <tr><td>LPDT 604 - T663D118</td><td>""" + T663D118_score_ENG_l2 + """</td><td>""" + T663D118_score_ENG_l1 
     + """</td><td>""" + T663D118_score_ENG +"""</td></tr>
 <tr><td>LPDT 605 - T663D120</td><td>""" + T663D120_score_ENG_l2 + """</td><td>""" + T663D120_score_ENG_l1 
     + """</td><td>""" + T663D120_score_ENG +"""</td></tr>
 <tr><td>LPDT 606 - T763D173</td><td>""" + T763D173_score_ENG_l2 + """</td><td>""" + T663D110_score_ENG_l1 
     + """</td><td>""" + T763D173_score_ENG  +"""</td></tr>
 <tr><td>LPDT 607 - T763D174</td><td>""" + T763D174_score_ENG_l2 + """</td><td>""" + T763D174_score_ENG_l1 
     + """</td><td>""" + T763D174_score_ENG +"""</td></tr>
 <tr><td>LPDT 608 - T763D131</td><td>""" + T763D131_score_ENG_l2 + """</td><td>""" + T763D131_score_ENG_l1 
     + """</td><td>""" + T763D131_score_ENG +"""</td></tr>
 <tr><td>LPDT 609 - T763D151</td><td>""" + T763D151_score_ENG_l2 + """</td><td>""" + T763D151_score_ENG_l1 
     + """</td><td>""" + T763D151_score_ENG +"""</td></tr>
</tbody></table>


""")


# In[812]:



# In[841]:


eng_table = """ Engine Breakdown Probability Report:  <style type="text/css">table.tableizer-table {font-size: 12px;text-align:center;border: 1px solid #CCC; text-align:center;font-family: Arial, Helvetica, sans-serif;} .tableizer-table td {padding: 4px;text-align:center;text-align:center;margin: 3px;border: 1px solid #CCC;}.tableizer-table th {background-color: #104E8B; text-align:center;color: #FFF;font-weight: bold;}</style><table class="tableizer-table"><thead><tr class="tableizer-firstrow"> <th>Truck #</th><th>""" + hour_now_l2 + """00 hrs - """ + hour_now_n2 +"""00 hrs</th><th>""" +hour_now_l1 + """00 hrs - """ + hour_now_n3 + """00 hrs</th><th>""" + hour_now_actual + """00 hrs - """ + hour_now_n4 + """00 hrs</th></tr></thead><tbody><tr><td>LPDT 601 - T663D143</td><td>""" + T663D143_score_ENG_l2 +  """</td><td>""" + T663D143_score_ENG_l1 + """</td><td>""" + T663D143_score_ENG +"""</td></tr><tr><td>LPDT 602 - T663D110</td><td>""" + T663D110_score_ENG_l2 + """</td><td>""" + T663D110_score_ENG_l1 + """</td><td>""" + T663D110_score_ENG +"""</td></tr><tr><td>LPDT 603 - T663D113</td><td>""" + T663D113_score_ENG_l2 + """</td><td>""" + T663D113_score_ENG_l1 + """</td><td>""" + T663D113_score_ENG +"""</td></tr><tr><td>LPDT 604 - T663D118</td><td>""" + T663D118_score_ENG_l2 + """</td><td>""" + T663D118_score_ENG_l1 + """</td><td>""" + T663D118_score_ENG +"""</td></tr><tr><td>LPDT 605 - T663D120</td><td>""" + T663D120_score_ENG_l2 + """</td><td>""" + T663D120_score_ENG_l1 + """</td><td>""" + T663D120_score_ENG +"""</td></tr><tr><td>LPDT 606 - T763D173</td><td>""" + T763D173_score_ENG_l2 + """</td><td>""" + T663D110_score_ENG_l1 + """</td><td>""" + T763D173_score_ENG  +"""</td></tr><tr><td>LPDT 607 - T763D174</td><td>""" + T763D174_score_ENG_l2 + """</td><td>""" + T763D174_score_ENG_l1 + """</td><td>""" + T763D174_score_ENG +"""</td></tr><tr><td>LPDT 608 - T763D131</td><td>""" + T763D131_score_ENG_l2 + """</td><td>""" + T763D131_score_ENG_l1 + """</td><td>""" + T763D131_score_ENG +"""</td></tr><tr><td>LPDT 609 - T763D151</td><td>""" + T763D151_score_ENG_l2 + """</td><td>""" + T763D151_score_ENG_l1 + """</td><td>""" + T763D151_score_ENG +"""</td></tr></tbody></table>"""


# In[842]:


tm_table = """ Transmission Breakdown Probability Report:  <style type="text/css">table.tableizer-table {font-size: 12px;text-align:center;border: 1px solid #CCC; text-align:center;font-family: Arial, Helvetica, sans-serif;} .tableizer-table td {padding: 4px;text-align:center;text-align:center;margin: 3px;border: 1px solid #CCC;}.tableizer-table th {background-color: #104E8B; text-align:center;color: #FFF;font-weight: bold;}</style><table class="tableizer-table"><thead><tr class="tableizer-firstrow"> <th>Truck #</th><th>""" + hour_now_l2 + """00 hrs - """ + hour_now_n2 +"""00 hrs</th><th>""" +hour_now_l1 + """00 hrs - """ + hour_now_n3 + """00 hrs</th><th>""" + hour_now_actual + """00 hrs - """ + hour_now_n4 + """00 hrs</th></tr></thead><tbody><tr><td>LPDT 601 - T663D143</td><td>""" + T663D143_score_TM_l2 +  """</td><td>""" + T663D143_score_TM_l1 + """</td><td>""" + T663D143_score_TM +"""</td></tr><tr><td>LPDT 602 - T663D110</td><td>""" + T663D110_score_TM_l2 + """</td><td>""" + T663D110_score_TM_l1 + """</td><td>""" + T663D110_score_TM +"""</td></tr><tr><td>LPDT 603 - T663D113</td><td>""" + T663D113_score_TM_l2 + """</td><td>""" + T663D113_score_TM_l1 + """</td><td>""" + T663D113_score_TM +"""</td></tr><tr><td>LPDT 604 - T663D118</td><td>""" + T663D118_score_TM_l2 + """</td><td>""" + T663D118_score_TM_l1 + """</td><td>""" + T663D118_score_TM +"""</td></tr><tr><td>LPDT 605 - T663D120</td><td>""" + T663D120_score_TM_l2 + """</td><td>""" + T663D120_score_TM_l1 + """</td><td>""" + T663D120_score_TM +"""</td></tr><tr><td>LPDT 606 - T763D173</td><td>""" + T763D173_score_TM_l2 + """</td><td>""" + T663D110_score_TM_l1 + """</td><td>""" + T763D173_score_TM  +"""</td></tr><tr><td>LPDT 607 - T763D174</td><td>""" + T763D174_score_TM_l2 + """</td><td>""" + T763D174_score_TM_l1 + """</td><td>""" + T763D174_score_TM +"""</td></tr><tr><td>LPDT 608 - T763D131</td><td>""" + T763D131_score_TM_l2 + """</td><td>""" + T763D131_score_TM_l1 + """</td><td>""" + T763D131_score_TM +"""</td></tr><tr><td>LPDT 609 - T763D151</td><td>""" + T763D151_score_TM_l2 + """</td><td>""" + T763D151_score_TM_l1 + """</td><td>""" + T763D151_score_TM +"""</td></tr></tbody></table><br><br>"""


# In[815]:


import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate


# In[816]:


import smtplib, os
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.utils import COMMASPACE, formatdate
from email import encoders


# In[817]:


msg = MIMEMultipart()
msg['Date'] = formatdate(localtime = True)
msg['Subject'] = "subject"
msg.attach(MIMEText("text"))


# In[818]:


from IPython.display import HTML


# In[819]:


text1_1 = """<html><head></head><body><p>Hi,<br> <br>Please find below the breakdown probability for Transmission and Engine failures of below trucks for <br>"""


# In[820]:


text1_2 = "<br></p></body></html><br><br> "


# In[821]:


text2 = """<html>
  <head></head>
  <body>
    <p><br> Kindly take the appropriate action<br> <br>
       Thanks and Regards. <br>
       </p>
  </body>
</html>
"""


# In[822]:


subject1 = "Truck Breakdown Prediction Report | " + str(time_now_actual.day) + "-" + str(time_now_actual.month) + "-" + str(time_now_actual.year) 


# In[823]:


subject2 = " | " + str(hour_now_actual) + "00 hrs to next 4 hrs" 


# In[824]:


text_field = str(time_now_actual.day) + "-" + str(time_now_actual.month) + "-" + str(time_now_actual.year) + " : "  + str(hour_now_actual) + "00 hrs to next 4 hrs" 


# In[826]:


text_1_all = text1_1 + text_field + text1_2 


# In[829]:


final_text = text_1_all  + tm_table +  eng_table  + text2


# In[830]:


HTML(final_text)


# In[831]:


f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1679')

f.close()


subject = subject1 + subject2

msg = MIMEMultipart()
msg['From'] = "it.admin@vedanta.co.in"


f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1692')

f.close()



# to = "Rajesh.Choudhary2@vedanta.co.in,s.bhattacharjee@vedanta.co.in,Yuvraj.Rajput@vedanta.co.in,Simranjeet.Singh@vedanta.co.in,Devendra.Yadav@vedanta.co.in,Atul.Sharma2@vedanta.co.in,Priya.Purohit@vedanta.co.in,pradeep.mahajan@vedanta.co.in,Devendra.Yadav@vedanta.co.in,gunjan.kothari@vedanta.co.in"

# msg["To"] = to

# cc = "ranglani.hardev@bcg.com,Trehan.Deepak@bcg.com,Jhavery.Ankur@bcg.com,Bhatia.Abhishek@bcg.com"

# msg["cc"] = cc

# rcpt = cc.split(",") + [to]

msg['Date'] = formatdate(localtime = True)
msg['Subject'] = subject


f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1714')

f.close()


# In[834]:


part2 = MIMEText(final_text, 'html')

msg.attach(part2)


f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1729')

f.close()


# In[835]:



s = smtplib.SMTP('10.101.37.99:25')


# In[836]:


# Simranjeet.Singh@vedanta.co.in,narayan.choudhary@sandvik.com,ankit.r.mathur@sandvik.com,ravindra_kumar.shrivastava@sandvik.com,Atul.Sharma2@vedanta.co.in,kumarmanohar199@gmail.com,Priya.Purohit@vedanta.co.in


# In[837]:


# Devendra.Yadav@vedanta.co.in,


# In[838]:

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1757')

f.close()


cc_1 = "ranglani.hardev@bcg.com,jhavery.ankur@bcg.com,trehan.deepak@bcg.com,bhatia.abhishek@bcg.com"


to_1 = "Rajesh.Choudhary2@vedanta.co.in,s.bhattacharjee@vedanta.co.in,Yuvraj.Rajput@vedanta.co.in,Simranjeet.Singh@vedanta.co.in,Atul.Sharma2@vedanta.co.in,Priya.Purohit@vedanta.co.in,Devendra.Yadav@vedanta.co.in,Gunjan.Kothari@vedanta.co.in,pradeep.mahajan@vedanta.co.in"

#rcpt_1 = cc_1 

rcpt_1 = cc_1.split(",") + [to_1]


msg["To"] = to_1
msg["cc"] = cc_1


# In[839]:

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('1773')

f.close()


s.sendmail(msg["From"], rcpt_1 ,  msg.as_string())


# In[840]:


s.quit()

f = open('C:/Users/hotsdt/Desktop/SCORING_CODE/status_check_rolling_3hrs.txt','w')

f.write('finish')

f.close()


