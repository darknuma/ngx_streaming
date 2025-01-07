from prefect import flow
from pathlib import Path
from scrape_deploy import market_data_stream

if __name__ == "__main__":
    market_data_stream.from_source(
        source=str(Path(__file__).parent),  # code stored in local directory
        entrypoint="scrape_deploy.py:market_data_stream",
    ).deploy(
        name="local-process-deploy-local-code",
        work_pool_name="ngx-managed-pool",
    )