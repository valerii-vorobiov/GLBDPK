### How this works:

1. clone this repo into server where kafka installed
2. `cd GLBDPK/lab_kafka`
3. `bash create_topic.sh btc_transactions`
4. import nifi_lab_kafka.xml to your nifi and start workflow
5. run consumer

### How to run consumer:


1. `python3 -m venv venv`
2. activate your virtual environment
3. `pip  install -r requirements.txt`
4. `python ./consumer.py`
