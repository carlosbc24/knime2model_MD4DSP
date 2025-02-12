from DataProcessing import DataProcessing


class Workflow:
    """
    Represents a workflow with a name and a list of data processing units.

    Attributes:
        name (str): The name of the workflow.
        data_processing_units (list): A list of data processing units in the workflow.
    """

    def __init__(self, name: str):
        """
        Initializes a Workflow instance.

        Args:
            name (str): The name of the workflow.
        """
        self.name = name
        self.data_processing_units = []

    def add_data_processing(self, data_processing: DataProcessing):
        """
        Adds a data processing unit to the workflow.

        Args:
            data_processing (DataProcessing): The data processing unit to add.
        """
        self.data_processing_units.append(data_processing)

    def remove_data_processing(self, data_processing: DataProcessing):
        """
        Removes a data processing unit from the workflow.

        Args:
            data_processing (DataProcessing): The data processing unit to remove.
        """
        self.data_processing_units.remove(data_processing)
