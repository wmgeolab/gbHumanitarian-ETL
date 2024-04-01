from prefect import flow

@flow(flow_run_name="Trial",log_prints=True)
def trial():
    print("prefect is working")

trial()