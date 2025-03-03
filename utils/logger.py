
# Importing libraries
import logging
import os


def set_logger(logger_name: str = "test"):
    """
    Create the logger to save the logs of the execution in the corresponding path

    Parameters:
    logger_name (str): name of the type of logger. Example: "test"
    """
    log_path = os.path.join('logs', logger_name)
    if not os.path.exists(log_path):
        os.makedirs(log_path)

    existing_logs = [f for f in os.listdir(log_path) if f.startswith(f'{logger_name}_log_')]
    log_numbers = [int(os.path.splitext(f)[0].split('_')[-1]) for f in existing_logs] if existing_logs else [0]
    next_log_number = max(log_numbers) + 1

    log_filename = os.path.abspath(
        os.path.join(log_path, f'{logger_name}_log_{next_log_number}.log'))
    # print(f'Log file: {log_filename}')

    try:
        logging.basicConfig(filename=log_filename, level=logging.DEBUG,
                            format="%(asctime)-15s %(levelname)8s %(name)s %(message)s")
        logging.getLogger("matplotlib.font_manager").disabled = True
        logging.getLogger("matplotlib.colorbar").disabled = True
        logging.getLogger("matplotlib.pyplot").disabled = True
    except Exception as e:
        print(f"Error al configurar el registro: {e}")


def print_and_log(message, level=logging.INFO):
    """
    Print a message on screen and save it in the log

    Parameters:
    message (str): message to print and save in the log
    level (int): level of the message
    """
    # print(message)
    logging.log(level, message)


def print_and_log_dict(dictionary, level=logging.INFO):
    """
    Print a dictionary on screen and save it in the log

    Parameters:
    dictionary (dict): dictionary to print and save in the log
    level (int): level of the message
    """
    print_and_log("Mapped data dictionary:", level=level)
    for key, value in dictionary.items():
        print_and_log(f"{key}: {value}", level=level)
    print_and_log("", level=level)
    print_and_log("", level=level)
