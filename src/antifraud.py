from __future__ import print_function, unicode_literals, division
import pandas as pd
import numpy as np
import pickle
engine = 'python'

# ! mv /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv_orig
# ! cut -d, -f1-4 /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv_orig > /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv
batchData = pd.read_csv("../paymo_input/batch_payment.csv", sep=', ')   # Note that these *.csv files were too large to upload to the github repository
batchData.time = pd.to_datetime(batchData.time,format="%Y-%m-%d %H:%M:%S")



# Some of the code here is just for testing purposes
batchData.head()

batchData.dtypes

print(batchData.shape)
print(batchData.isnull().sum())
print(batchData.isnull().any())
print(batchData.count())
print(batchData.size)
print(batchData.info())



(numRows,numColumns) = batchData.shape
print("Number of rows in batch_payment.csv: ", numRows)
print("Number of columns in batch_payment.csv: ", numColumns)
print("Number of users making payments in batch_payment.csv", batchData.id1.unique().shape)
print("Number of users receiving payments in batch_payment.csv", batchData.id2.unique().shape)
print("Number of unique times in the batch_payment.csv", batchData.time.unique().shape)




# At the Unix command prompt, I did:
# ! mv /Users/markyashar/digital-wallet/paymo_input/stream_payment.csv /Users/markyashar/digital-wallet/paymo_input/stream_payment.csv_orig                                                # ! cut -d, -f1-4 /Users/markyashar/digital-wallet/paymo_input/stream_payment.csv_orig > /Users/markyashar/digital-wallet/paymo_input/stream_payment.csv
streamData = pd.read_csv("../paymo_input/stream_payment.csv", sep=', ')  # Note that these *.csv files were too large to upload to the github repository
streamData.time = pd.to_datetime(streamData.time,format="%Y-%m-%d %H:%M:%S")
streamData.dtypes
streamData.head()




print(streamData.shape)
print(streamData.isnull().sum())
print(streamData.isnull().any())
print(streamData.count())
print(streamData.size)
print(streamData.info())




(numRows,numColumns) = streamData.shape
print("Number of rows in stream_payment.csv: ", numRows)
print("Number of columns in stream_payment.csv: ", numColumns)
print("Number of users making payments in stream_payment.csv", streamData.id1.unique().shape)
print("Number of users receiving payments in stream_payment.csv", streamData.id2.unique().shape)
print("Number of unique times in the stream_payment.csv", streamData.time.unique().shape)




streamData[streamData.duplicated(['id1','id2'], keep=False)].groupby(('id1','id2')).min().reset_index()




batch_streamData = batchData.merge(streamData,left_index=True,right_index=True)
batch_streamData.head(25)




feature1_trusted = batch_streamData[(batch_streamData.id1_x==batch_streamData.id1_y) & (batch_streamData.id2_x==batch_streamData.id2_y)]
print(len(feature1_trusted))
trusted='trusted'
feature1_trusted=feature1_trusted.assign(trusted=trusted)
feature1_trusted.columns = ['' if x=='trusted' else x for x in feature1_trusted.columns]
feature1_trusted



feature1_unverified = batch_streamData[(batch_streamData.id1_x != batch_streamData.id1_y) & (batch_streamData.id2_x != batch_streamData.id2_y)]
print(len(feature1_unverified))
unverified='unverified'
feature1_unverified=feature1_unverified.assign(unverified=unverified)
feature1_unverified.columns = ['' if x=='unverified' else x for x in feature1_unverified.columns]
feature1_unverified


result_feature1 = feature1_unverified.append(feature1_trusted)
result_feature1.sort_index(inplace=True)
result_feature1


result_feature1.loc[2566018]   # this is just a test




result_feature1.columns = ['status' if x=='' else x for x in result_feature1.columns]
results_feature1 = result_feature1.status.to_string(index=False)

import os, csv
output1 = '/Users/markyashar/digital-wallet/paymo_output/output1.txt'

results_feature1.encode('unicode-escape')

with open(output, 'w') as f:
    for s in results_feature1:
        f.write(s)

feature2_trusted = batch_streamData[(batch_streamData.id1_x == batch_streamData.id2_y) 
                                    & (batch_streamData.id2_x != batch_streamData.id2_y)] 
                                    # & (batch_streamData.id2_x == batch_streamData.id1_y)] # & 
                                     #      (batch_streamData.id1_x != batch_streamData.id2_y)]
                                   #  or ((batch_streamData.id1_x == batch_streamData.id2_y) & 
                                   #     (batch_streamData.id2_x == batch_streamData.id2_y))]
print(len(feature2_trusted))
trusted='trusted'
feature2_trusted=feature2_trusted.assign(trusted=trusted)
feature2_trusted.columns = ['' if x=='trusted' else x for x in feature2_trusted.columns]
feature2_trusted.head()

feature2_unverified = batch_streamData[(batch_streamData.id1_x != batch_streamData.id1_y) & (batch_streamData.id2_x != batch_streamData.id2_y)]
print(len(feature2_unverified))
unverified='unverified'
feature2_unverified=feature2_unverified.assign(unverified=unverified)
feature2_unverified.columns = ['' if x=='unverified' else x for x in feature2_unverified.columns]
feature2_unverified.head()
result_feature2 = feature2_unverified.append(feature2_trusted)
result_feature2.sort_index(inplace=True)
result_feature2.head()

result_feature2.columns = ['status' if x=='' else x for x in result_feature2.columns]
results_feature2 = result_feature2.status.to_string(index=False)
results_feature2.encode('unicode-escape')
output2 = '/Users/markyashar/digital-wallet/paymo_output/output2.txt'
with open(output2, 'w') as f:
    for s in results_feature2:
        f.write(s)
