from datetime import timedelta

import httpx

from prefect import flow, task # Prefect flow and task decorators
from prefect.cache_policies import INPUTS
from prefect.concurrency.asyncio import rate_limit


@flow(log_prints=True)
def show_stars(github_repos: list[str]):
    """Flow: Show the number of stars that GitHub repos have"""

    # Call Task 1
    repo_stats = fetch_stats.map(github_repos)

    # Call Task 2
    stars = get_stars.map(repo_stats).result()

    # Print the result
    for repo, star_count in zip(github_repos, stars):
        print(f"{repo}: round 2 - {star_count} stars")


@task(
    retries=3,
    cache_policy=INPUTS, cache_expiration=timedelta(days=1)
)
def fetch_stats(github_repo: str):
    """Task 1: Fetch the statistics for a GitHub repo"""
    rate_limit('github-api')
    return httpx.get(f"https://api.github.com/repos/{github_repo}").json()


@task
def get_stars(repo_stats: dict):
    """Task 2: Get the number of stars from GitHub repo statistics"""

    return repo_stats['stargazers_count']


# Run the flow
if __name__ == "__main__":
    show_stars([
        "PrefectHQ/prefect",
        "pydantic/pydantic",
        "huggingface/transformers"
    ])
