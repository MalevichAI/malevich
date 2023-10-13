from abc import ABC, abstractmethod


class Anomaly(ABC):
    @abstractmethod
    def __str__(self) -> str:
        pass

class MultipleLeaves(Anomaly):
    def __str__(self) -> str:
        return "multiple leaves are not supported"


class DetectedAnomalyError(Exception):
    """Exception raised when an anomaly is detected"""

    def __init__(self, anomaly: Anomaly) -> None:
        self.anomaly = anomaly
        super().__init__(f"During the execution of the workflow, an anomaly was detected: {anomaly}")  # noqa: E501
