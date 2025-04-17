from typing import List, Dict, Any
from datetime import datetime

class StateSnapshot:
    def __init__(self, id: str, user_message_id: str, analyses: List[Analysis], tools: List[str], highcharts_config: dict, flipside_example_queries: pd.DataFrame):
        self.id = id
        self.user_message_id = user_message_id
        self.analyses = analyses
        self.tools = tools
        self.highcharts_config = highcharts_config
        self.flipside_example_queries = flipside_example_queries

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'user_message_id': self.user_message_id,
            'analyses': self.analyses,
            'tools': self.tools,
            'highcharts_config': self.highcharts_config,
            'flipside_example_queries': self.flipside_example_queries
        }

    @classmethod
    def from_state_snapshot(cls, state_snapshot: Dict[str, Any]) -> "StateSnapshot":
        return cls(
            id=state_snapshot['id'],
            user_message_id=state_snapshot['user_message_id'],
            analyses=state_snapshot['analyses'],
            tools=state_snapshot['tools'],
            highcharts_config=state_snapshot['highcharts_config'],
            flipside_example_queries=state_snapshot['flipside_example_queries']
        )

    def to_string(self) -> str:
        return f"""
        ID: {self.id}
        User Message ID: {self.user_message_id}
        Analyses: {self.analyses}
        Tools: {self.tools}
        Highcharts Config: {self.highcharts_config}
        Flipside Example Queries: {self.flipside_example_queries}
        """

    def __str__(self):
        return self.to_string()
