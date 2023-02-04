from prefect.deployments import Deployment
from etl_gcs_to_bq import etl_parent_flow
from prefect.filesystems import GitHub

github_block = GitHub.load("github-connector")

github_deploy = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="github-flow",
    infrastructure=docker_block,
)


if __name__ == "__main__":
    github_deploy.apply()