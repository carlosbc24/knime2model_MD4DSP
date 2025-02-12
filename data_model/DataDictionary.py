from enum import Enum


class PortType(Enum):
    INPUT = 0
    OUTPUT = 1


class DataDictionary:
    """
    Represents a data dictionary with a name, port type, and file path.

    Attributes:
        name (str): The name of the data dictionary.
        port_type (PortType): The type of the port (INPUT or OUTPUT).
        file_path (str): The file path associated with the data dictionary.
    """

    def __init__(self, name: str, port_type: PortType, file_path: str):
        """
        Initializes a DataDictionary instance.

        Args:
            name (str): The name of the data dictionary.
            port_type (PortType): The type of the port (INPUT or OUTPUT).
            file_path (str): The file path associated with the data dictionary.
        """
        self.name = name
        self.port_type = port_type
        self.file_path = file_path

    def __str__(self) -> str:
        """
        Returns a string representation of the DataDictionary instance.

        Returns:
            str: A string representation of the DataDictionary instance.
        """
        return "Name: " + self.name + ", Port Type: " + self.port_type.name + ", File Path: " + self.file_path
