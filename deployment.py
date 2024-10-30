from prefect import flow

# # Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/bonsuot/fantasy-premier-league.git"


if __name__ == "__main__":
    flow.from_source(
        source=SOURCE_REPO,
        entrypoint="fpl_etl.py:main_flow",
    ).deploy(
        name="fpl-deployment",
        work_pool_name="my-work-pool",
        cron="0 1 * * *", # Cron schedule (1am every day)
    )

