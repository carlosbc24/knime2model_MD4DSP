class Workflow:
    def __init__(self, name):
        self.name = name
        self.data_processing_units = []

    def add_data_processing(self, data_processing):
        self.data_processing_units.append(data_processing)

    def remove_data_processing(self, data_processing):
        self.data_processing_units.remove(data_processing)


class DataProcessing:
    def __init__(self, name):
        self.name = name
        self.input_ports = []
        self.output_ports = []

    def add_input_port(self, input_port):
        self.input_ports.append(input_port)

    def add_output_port(self, output_port):
        self.output_ports.append(output_port)

    def remove_input_port(self, input_port):
        self.input_ports.remove(input_port)

    def remove_output_port(self, output_port):
        self.output_ports.remove(output_port)


class InputPort:
    def __init__(self, name, file_path):
        self.name = name
        self.file_path = file_path


class OutputPort:
    def __init__(self, name, file_path):
        self.name = name
        self.file_path = file_path
