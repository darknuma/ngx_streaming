# write prefect workflow here, so we can load to snowflake and push to grafana 
# write a mageAI so we can do some analytics
from prefect import task, flow

@task
async def run():
    await task 


