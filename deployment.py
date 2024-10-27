from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from fpl_etl import main_flow

'''
Before creating a deployment, create a work pool
first if one do not exist

CLI: prefect work-pool create "my-work-pool" --type process
you can also create a work-pool with a python script 
'''

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/bonsuot/fantasy-premier-league"

if __name__ == "__main__":
    main_flow.from_source(
        source=SOURCE_REPO,
        entrypoint="fpl_etl.py:main_flow" # Specific flow to run
    ).deploy(
        name="fpl-deployment",
        work_pool_name="fpl-pool",
        cron="0 0,12 * * *", # Cron schedule (runs daily, every 12hrs)
    )



''' 
Now start a worker with: prefect worker start -p 'fpl-pool' 

run deployment via CLI
prefect deployment run 'main-flow/fpl_etl'
'''

"""
creating a work pool with python

from prefect.workers.pools import WorkPool
try:
    work_pool = WorkPool(
        name="my-work-pool",
        type="process",
        base_job_template={},
    )
    work_pool.save()
except Exception as e:
    print(f"Work pool might already exist: {e}")
"""