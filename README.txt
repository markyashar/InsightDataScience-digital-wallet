The Python code that I wrote and used for this challenge is "antifraud.py" in the following directory in the github repository:

https://github.com/markyashar/InsightDataScience-digital-wallet/src/

I used the python pandas and numpy modules/libraries as well.

The python notebook antifraud.ipynb in the same directory was written and used for testing purposes.

The first thing I did was to remove the 'message' column from the downloaded "batch_payment.csv" and
"stream_payment.csv" files as a way to get rid of the unicode characters. I did this by using unix commands
such as the following:

mv /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv_orig
# ! cut -d, -f1-4 /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv_orig > /Users/markyashar/digital-wallet/paymo_input/batch_payment.csv

Note that the resulting modified "batch_payment.csv" and "stream_payment.csv" files were too large to upload to the github
repository. Therefore, they will NOT be found in https://github.com/markyashar/InsightDataScience-digital-wallet/paymo_input/
  
