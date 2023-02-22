import json
from datetime import datetime


class Repository:
    def __init__(self, path=None):
        self.path = path if path else "repository.json"

    def load_data(self):
        with open(self.path, "r") as f:
            return json.load(f)

    def save_data(self, data):
        with open(self.path, "w") as f:
            json.dump(data, f, indent=4)

    def get_cloned_reports(self) -> dict:
        data = self.load_data()
        return data["cloned_reports"]

    def save_cloned_report(self, report, run, cloned_report):
        data = self.load_data()
        data['cloned_reports'][report] = {
            'run': run,
            'cloned_report': cloned_report,
            'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        self.save_data(data)

    def get_cloned_report(self, report: str):
        data = self.load_data()
        cloned_report_data = data["cloned_reports"].get(report, None)
        if not cloned_report_data:
            return None
        return cloned_report_data['cloned_report']

    def delete_report(self, report):
        data = self.load_data()
        del data['cloned_reports'][report]
        self.save_data(data)
