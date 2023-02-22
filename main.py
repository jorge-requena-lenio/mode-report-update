import mode
from repository import Repository
import sql_utils
import os
import pandas as pd
import json


repository = Repository()


def get_target_reports():
    input_df = pd.read_csv("original-reports.csv")
    keywords = ["opportunity1", "opportunity2",
                "opportunity3", "opportunity_field_history", "salesforce_all"]

    results = {}
    for index, row in input_df.iterrows():
        report = row["Snowflake links"]
        print(f"Report {index+1}/{len(input_df.index)}: {report}")
        queries = mode.get_queries(report)

        for query in queries:
            name = query["name"]
            token = query["token"]
            sql = query["raw_query"]
            updated_at = query["updated_at"] if query["updated_at"] else query["created_at"]

            if any(word in sql.lower() for word in keywords):
                print(f"Found keyword in {report}/{name}")

                if report not in results:
                    results[report] = []

                results[report].append(
                    dict(token=token, name=name, updated_at=updated_at))

                path = f'output/{report}/'
                os.makedirs(path, exist_ok=True)

                with open(f'{path}/{token}.sql', 'w') as f:
                    f.write(sql)
            else:
                print(
                    f"Skipping {name} because it doesn't contain any of the keywords")

    json.dump(results, open(
        "target-reports.json", "w"), indent=4)


def secure_process_report(report):
    cloned_report = repository.get_cloned_report(report)

    if not cloned_report:
        run = mode.get_report_runs(report)[0]['token']
        cloned_report_result = mode.clone_report(report, run)
        cloned_report = cloned_report_result["token"]
        repository.save_cloned_report(
            report, run, cloned_report)

    queries = mode.get_queries(cloned_report)

    for query in queries:
        name = query["name"]
        token = query["token"]
        sql = query["raw_query"]

        if "salesforce_all" not in sql:
            print(f'Query "{name}" does not need to be updated')
            continue

        print(f'Processing query "{name}"')
        updated_sql = sql_utils.update_sql(sql)

        formatted_sql = sql_utils.format_sql(sql)
        formatted_sql = sql_utils.convert_to_single_line(formatted_sql)
        formatted_sql = sql_utils.format_sql(formatted_sql)

        path = f'output/{cloned_report}/{token}'
        if os.path.exists(path):
            os.system(f'rm -rf {path}')
        os.makedirs(path, exist_ok=True)

        with open(f'{path}/input.sql', 'w') as f:
            f.write(formatted_sql)

        with open(f'{path}/output.sql', 'w') as f:
            f.write(updated_sql)

        mode.update_query(cloned_report, token, updated_sql)


def dangerous_process_report(report):
    queries = mode.get_queries(report)

    for query in queries:
        name = query["name"]
        token = query["token"]
        sql = query["raw_query"]

        if "salesforce_all" not in sql:
            print(f'Query "{name}" does not need to be updated')
            continue

        print(f'Processing query "{name}"')
        updated_sql = sql_utils.update_sql(sql)

        formatted_sql = sql_utils.format_sql(sql)
        formatted_sql = sql_utils.convert_to_single_line(formatted_sql)
        formatted_sql = sql_utils.format_sql(formatted_sql)

        path = f'prod-output/{report}/{token}'
        if os.path.exists(path):
            os.system(f'rm -rf {path}')
        os.makedirs(path, exist_ok=True)

        with open(f'{path}/input.sql', 'w') as f:
            f.write(formatted_sql)

        with open(f'{path}/output.sql', 'w') as f:
            f.write(updated_sql)

        mode.update_query(report, token, updated_sql)


def delete_cloned_report(report):
    cloned_report = repository.get_cloned_report(report)
    if not cloned_report:
        raise Exception(f'No cloned report found for {report}')
    print(f'Deleting cloned report original:{report} cloned:{cloned_report}')
    mode.delete_report(cloned_report)
    repository.delete_report(report)


def run_cloned_report(report):
    cloned_report = repository.get_cloned_report(report)
    if not cloned_report:
        raise Exception(f'No cloned report found for {report}')
    print(f'Running cloned report original:{report} cloned:{cloned_report}')
    mode.run_report(cloned_report)


if __name__ == "__main__":
    reports = [
        "1f3831f7b418",
        "676bc3781f0b",
        "8d62662ab146",
        "c1fd6b4dfefb",
        "160dc51b55a9",
        "167b6a52998b",
        "ddf2bb94b4af",
        "57a3f066b0fb",
        "ec82c2c1f225",
        "595c90382f38",
    ]

    for report in reports:
        secure_process_report(report)
