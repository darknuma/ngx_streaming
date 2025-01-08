# NIGERIA STOCK EXCHANGE STREAMING

The goal was to stream the market data statistics in Nigeria Stock Exchange market, this is a batch run for 30 minutes, followin each weekday the stock market is running

## PROJECT SETUP
- `src`
- `prefect`
- `mage_ai`

## PREQUISITES
I used the WSL:Ubuntu as my development environment (local)
- Have kafka installed 
- Check out the snowflake config text
- To use Snowflake with Kafka, follow this tutorial [here](https://sandundayananda.medium.com/using-snowflake-connector-for-kafka-with-snowpipe-streaming-5510ec092895) and this [video](https://www.youtube.com/watch?app=desktop&v=w813ttJ5Bps)



## HOW TO RUN

- **Run local job**
   - Start zookeeper and kafka, create your topic
```
pip install -r requirement.txt
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic ngx-market-data 
cd src
python3 scrape_stream.py
```
- **Run with Prefect** 
   - Start the server
   - create your workpool
   - deploy your code
   - you can set the schedule on the UI, which I did with `*/30 9-13 * * 1-5`
```
prefect start server
prefect work-pool create --type process [workpool_name]
python create_deployment.py
```

- **Run with MageAI**: 
  - most of the work done, is in the UI (basically create a streaming pipeline)
    - set configs in `mage_ai/ngx/io_config.yaml`
    - dataloader: `mage_ai/ngx/data_loaders/kafka_run.yaml`, edit the configs
    - transformer: `mage_ai/ngx/transformers/transform.py`
    - data_exporter: `mage_ai/ngx/data_exporters/export_s3.yml`s3 sink (make sure you pass your env vars)
```
mage start [project_name]
```