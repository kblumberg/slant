from typing import Dict, Any

class Analysis:
    def __init__(self, metric: str, project: str, activity: str, start_time: int, end_time: int):
        self.metric = metric
        self.project = project
        self.activity = activity
        self.start_time = start_time
        self.end_time = end_time

    def to_dict(self) -> Dict[str, Any]:
        return {
            'metric': self.metric,
            'project': self.project,
            'activity': self.activity,
            'start_time': self.start_time,
            'end_time': self.end_time
        }

    @classmethod
    def from_project(cls, project: Dict[str, Any]) -> "Analysis":
        return cls(
            metric=project['metric'],
            project=project['project'],
            activity=project['activity'],
            start_time=project['start_time'],
            end_time=project['end_time']
        )

    def to_string(self) -> str:
        return f"""
        Metric: {self.metric}
        Project: {self.project}
        Activity: {self.activity}
        Start Time: {self.start_time}
        End Time: {self.end_time}
        """

    def __str__(self):
        return self.to_string()
