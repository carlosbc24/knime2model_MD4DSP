from DataDictionary import DataDictionary

class DataProcessing:
    """
    Represents a data processing unit with a name, input ports, and output ports.

    Attributes:
        name (str): The name of the data processing unit.
        input_ports (list): A list of input ports.
        output_ports (list): A list of output ports.
    """

    def __init__(self, name: str):
        """
        Initializes a DataProcessing instance.

        Args:
            name (str): The name of the data processing unit.
        """
        self.name = name
        self.input_ports = []
        self.output_ports = []

    def add_input_port(self, input_port: DataDictionary):
        """
        Adds an input port to the data processing unit.

        Args:
            input_port (DataDictionary): The input port to add.
        """
        self.input_ports.append(input_port)

    def add_output_port(self, output_port: DataDictionary):
        """
        Adds an output port to the data processing unit.

        Args:
            output_port (DataDictionary): The output port to add.
        """
        self.output_ports.append(output_port)

    def remove_input_port(self, input_port: DataDictionary):
        """
        Removes an input port from the data processing unit.

        Args:
            input_port (DataDictionary): The input port to remove.
        """
        self.input_ports.remove(input_port)

    def remove_output_port(self, output_port: DataDictionary):
        """
        Removes an output port from the data processing unit.

        Args:
            output_port (DataDictionary): The output port to remove.
        """
        self.output_ports.remove(output_port)
