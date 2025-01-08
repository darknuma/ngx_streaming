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

- Run local job
```
pip install -r requirement.txt
cd src
python3 scrape_stream.py (to run loc)
```
- Run with Prefect 
   - Start the server
   - create your workpool
   - deploy your code
```
prefect start server
prefect workpool - [workpoolname]
python 

```

- Run with MAgeAI: most of the work done, is in the UI (basically create a streaming pipeline)
    - dataloader: is kafka, edit the configs
    - transformer
    - data_exporter: s3 sink (make sure you pass your env vars)
```
mage start 

```