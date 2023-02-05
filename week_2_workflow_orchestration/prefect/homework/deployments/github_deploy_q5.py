from prefect.deployments import Deployment
from etl_web_to_gcs_04_2019_green import etl_web_to_gcs_q5
from prefect.filesystems import GitHub

github_block = GitHub.load("github-connector")

github_deploy = Deployment.build_from_flow(
    flow=etl_web_to_gcs,
    name="github-flow",
    storage=github_block,
    entrypoint="/homework/deployments/etl_web_to_gcs_04_2019_green.py:etl_web_to_gcs_q5"
)

if __name__ == "__main__":
    github_deploy.apply()