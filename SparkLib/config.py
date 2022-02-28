import configparser as cparser
from codecs import open as codecs_open
from enum import Enum


__all__ = ['parse_config']


def parse_config(filename_or_template):
    """ Returns config dictionary for SparkContext initialization
    based on the provided template of config file.

    Parameters
    ----------
    filename_or_template : str or Enum with str values
        Full name of the config file.

    Returns
    -------
    dict
        Dictionary of config parameters.

    """

    filename = filename_or_template.value if isinstance(filename_or_template, Enum) else filename_or_template

    config = cparser.ConfigParser()

    config.read_file(codecs_open(filename, 'r', 'utf8'))

    options_dict = {}
    for section in config.sections():
        for option in config.options(section):
            options_dict[option] = config.get(section, option)

    return options_dict
