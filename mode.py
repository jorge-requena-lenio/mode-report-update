import requests
from requests.auth import HTTPBasicAuth
import os

host = "https://app.mode.com"
account = os.getenv("MODE_ACCOUNT")
username = os.getenv("MODE_KEY")
password = os.getenv("MODE_SECRET")


def get_report_info(report: str) -> object:
    print(f"Getting report info for {report}...")
    url = f"{host}/api/{account}/reports/{report}"
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    return response.json()


def clone_report(report: str, run: str) -> object:
    print(f"Cloning report {report}/{run}...")
    url = f"{host}/api/{account}/reports/{report}/runs/{run}/clone"
    response = requests.post(url, auth=HTTPBasicAuth(username, password))
    return response.json()


def get_queries(report: str) -> list:
    print(f"Getting queries for {report}...")
    url = f"{host}/api/{account}/reports/{report}/queries"
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    return response.json()["_embedded"]["queries"]


def update_query(report: str, query: str, sql: str) -> None:
    print(f"Updating query {report}/{query}...")
    url = f"{host}/api/{account}/reports/{report}/queries/{query}"
    payload = {
        "query": {
            "raw_query": sql,
        }
    }
    response = requests.patch(url, auth=HTTPBasicAuth(
        username, password), json=payload)
    if response.status_code != 200:
        raise Exception(response.text)


def run_report(report: str) -> None:
    print(f"Running report {report}...")
    url = f"{host}/api/{account}/reports/{report}/runs"
    response = requests.post(url, auth=HTTPBasicAuth(username, password))
    if response.status_code != 202:
        raise Exception(response.text)


def get_report_runs(report: str) -> list:
    print(f"Getting report runs for {report}...")
    url = f"{host}/api/{account}/reports/{report}/runs"
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    data = response.json()
    if data['pagination']['total_pages'] > 1:
        raise Exception('More than one page of results')
    return data['_embedded']['report_runs']


def get_query_runs(report, query):
    print(f"Getting  runs for {report}...")
    url = f'{host}/api/{account}/reports/{report}/queries/{query}/runs'
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    data = response.json()
    if data['pagination']['total_pages'] > 1:
        raise Exception('More than one page of results')
    return data['_embedded']['query_runs']

def get_query_runs_by_run(report: str, run: str) -> list:
    url = f"{host}/api/{account}/reports/{report}/runs/{run}/query_runs"
    response = requests.get(url, auth=HTTPBasicAuth(username, password))
    data = response.json()
    return data["_embedded"]["query_runs"]


def delete_report(report):
    print(f"Deleting report {report}...")
    url = f"{host}/api/{account}/reports/{report}"
    response = requests.delete(url, auth=HTTPBasicAuth(username, password))
    if response.status_code != 200:
        raise Exception(response.text)


def restore_queries(report):
    queries = get_queries(report)
    for query in queries:
        token = query['token']
        query_runs = get_query_runs(report, token)
        newest = query_runs[0]
        oldest = query_runs[-1]
        if newest['raw_source'] != oldest['raw_source']:
            update_query(report, token, oldest['raw_source'])
