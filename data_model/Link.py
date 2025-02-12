from data_model.DataProcessing import DataProcessing


class Link:
    """
    Represents a link between two data processing units.

    Attributes:
        name (str): The name of the data processing unit.
        incoming: The incoming data processing.
        outgoing: The outgoing data processing.
    """
    def __init__(self, name: str, incoming: DataProcessing, outgoing: DataProcessing):
        """
        Initializes a DataDictionary instance.

        Args:
            name (str): The name of the data dictionary.
            incoming (DataProcessing): The incoming data processing.
            outgoing (DataProcessing): The outgoing data processing.
        """
        self.name = name
        self.incoming = incoming
        self.outgoing = outgoing

    def __str__(self) -> str:
        """
        Returns a string representation of the link instance.

        Returns:
            str: A string representation of the link instance.
        """
        return "Name: " + self.name + ", Incoming: " + self.incoming.name + ", Outgoing: " + self.outgoing.name
