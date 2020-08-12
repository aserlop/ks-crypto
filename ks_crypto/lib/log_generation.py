import logging
from datetime import datetime


def __setup_logger(logger_name, log_file, level=logging.INFO):
    """Crea un fichero de log con nombre 'log_file'.
        Args:
            logger_name (str): identificador del logger
            log_file (str): nombre del fichero de log
            level (str): nivel por defecto al escribir en el log

        Returns:
            logging.Logger: logging creado
    """
    log_setup = logging.getLogger(logger_name)
    formatter = logging.Formatter('%(levelname)s: %(asctime)s %(message)s',
                                  datefmt='%m/%d/%Y %I:%M:%S %p')
    file_handler = logging.FileHandler(log_file, mode='a')
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log_setup.setLevel(level)
    log_setup.addHandler(file_handler)
    log_setup.addHandler(stream_handler)
    return log_setup


def get_logs(script_name):
    """Crea dos ficheros de log, uno de ejecución y otro de detalle, que empiezan por el nombre 'script_name'
        Args:
            script_name (str): nombre por el que empezarán los ficheros

        Returns:
            (logging.Logger, logging.Logger): Devuelve dos loggings. El primero es el de ejecución y el segundo es el
            de detalle.
        """
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_ejecucion = "{script_name}_ejecucion_{timestamp}.log".format(script_name=script_name,
                                                                          timestamp=timestamp_str)
    log_file_detalle = "{script_name}_detalle_{timestamp}.log".format(script_name=script_name,
                                                                      timestamp=timestamp_str)
    log_ejecucion = __setup_logger('log_one', log_file_ejecucion)
    log_detalle = __setup_logger('log_two', log_file_detalle)
    return log_ejecucion, log_detalle
