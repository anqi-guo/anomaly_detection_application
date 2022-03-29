import configparser
import os
from enum import Enum
import utils.file_util as file_util
from utils.get_logger import Logger


class Conf:
    def __init__(self, name):
        file_path = file_util.get_file_path(name, 'resource')
        self.name = file_path
        self.cp = configparser.RawConfigParser()
        log = Logger().logger
        if not os.path.exists(file_path):
            print('当前文件不存在：', file_path)
            log.error('当前文件不存在：' + file_path)
        self.cp.read(file_path, encoding='utf-8-sig')

    def get_sections(self):
        return self.cp.sections()

    def get_options(self, section):
        if isinstance(section, Enum):
            section = section.name
        if self.cp.has_section(section):
            return self.cp.options(section)

    def get_items(self, section):
        if isinstance(section, Enum):
            section = section.name
        if self.cp.has_section(section):
            return self.cp.items(section)

    def get_dict(self, section):
        if isinstance(section, Enum):
            section = section.name
        if self.cp.has_section(section):
            items = self.cp.items(section)
            return dict(items)

    def get_value(self, section, option):
        if isinstance(section, Enum):
            section = section.name
        if isinstance(option, Enum):
            option = option.name
        if self.cp.has_option(section, option):
            return self.cp.get(section, option)

    def set_section(self, section):
        if isinstance(section, Enum):
            section = section.name
        if not self.cp.has_section(section):
            self.cp.add_section(section)
            self.cp.write(open(self.name, 'w'))

    def set_value(self, section, option, value):
        if isinstance(section, Enum):
            section = section.name
        if isinstance(option, Enum):
            option = option.name
        if isinstance(value, Enum):
            value = value.name
        if not self.cp.has_option(section, option):
            self.cp.set(section, option, value)
            self.cp.write(open(self.name, 'w'))

    def del_section(self, section):
        if isinstance(section, Enum):
            section = section.name
        if self.cp.has_section(section):
            self.cp.remove_section(section)
            self.cp.write(open(self.name, 'w'))

    def del_option(self, section, option):
        if isinstance(section, Enum):
            section = section.name
        if isinstance(option, Enum):
            option = option.name
        if self.cp.has_option(section, option):
            self.cp.remove_option(section, option)
            self.cp.write(open(self.name, 'w'))

    def update_value(self, section, option, value):
        if isinstance(section, Enum):
            section = section.name
        if isinstance(option, Enum):
            option = option.name
        if isinstance(value, Enum):
            value = value.name
        self.cp.set(section, option, value)
        self.cp.write(open(self.name, 'w'))