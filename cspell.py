import hashlib
import logging
import os
import pickle
import re
import signal
import string
import sys
from datetime import datetime
from threading import Timer

import CPlusSpell as cp
import pandas as pd

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s][%(levelname)s]: %(message)s')

sys.setrecursionlimit(10000)


class LogParser:

    def __init__(self, in_dir='./', out_dir='./result/', log_format=None, tau=0.5, keep_para=True, text_max_length=4096,
                 log_main=None):
        """
        Attributes in priority order (from most necessary to optional)
        """
        self.tau = tau
        self.log_format = log_format
        self.trie_root = None
        self.log_cluster_lines = None
        self.save_path = out_dir
        self.last_line_id = 0
        self.df_log = None
        self.keep_para = keep_para
        self.text_max_length = text_max_length
        self.path = in_dir
        self.log_name = None
        self.main_log_name = log_main
        self.df_event = None

        self.parser = None

        self.headers, self.regex = generate_log_format_regex(self.log_format)

        root_node_path = os.path.join(self.save_path, 'rootNode.pkl')
        log_clu_l_path = os.path.join(self.save_path, 'logCluL.pkl')

        if os.path.exists(root_node_path) and os.path.exists(log_clu_l_path):
            with open(root_node_path, 'rb') as f:
                self.trie_root = pickle.load(f)
            with open(log_clu_l_path, 'rb') as f:
                self.log_cluster_lines = pickle.load(f)
            self.set_last_line_id()
            self.parser = cp.Parser(self.log_cluster_lines, self.trie_root,
                                    self.tau)
            logging.info(f'Load objects done, last_line_id: {self.last_line_id}')
        else:
            self.parser = cp.Parser(self.tau)

    def set_last_line_id(self):
        for logClust in self.log_cluster_lines:
            max_line = max(logClust.logIDL)
            if max_line > self.last_line_id:
                self.last_line_id = max_line

    def log_to_dataframe(self, log):
        """ Function to create initial dataframe
        """
        log_messages = []
        line_count = 0
        total_line = len(log)

        for line in log:
            if len(line) > self.text_max_length:
                logging.error('Length of log string is too long')
                logging.error(line)
                continue
            t = self.start_timer(self._log_to_dataframe_handler)
            line = re.sub(r'[^\x00-\x7F]+', '<NASCII>', line)
            try:
                match = self.regex.search(line.strip())
                message = [match.group(header) for header in self.headers]
                log_messages.append(message)
                line_count += 1
                if line_count % 10000 == 0 or line_count == total_line:
                    logging.info('Loaded {0:.1f}% of log lines.'.format(line_count * 100 / total_line))
            except Exception as e:
                _ = e
                pass
            self.stop_timer(t)

        df_log = pd.DataFrame(log_messages, columns=self.headers)
        df_log.insert(0, 'LineId', None)
        df_log['LineId'] = [i + 1 for i in range(line_count)]
        return df_log

    def parse_file(self, file, persistence=True):
        start_time = datetime.now()
        filepath = os.path.join(self.path, file)
        logging.info('Parsing file: ' + filepath)
        self.log_name = file
        with open(filepath, 'r') as f:
            self.df_log = self.log_to_dataframe(f.readlines())
        logging.info('Pre-processing done. [Time taken: {!s}]'.format(datetime.now() - start_time))
        return self.parse(persistence)

    def parse_lines(self, lines, persistence=True):
        self.log_name = self.main_log_name
        start_time = datetime.now()
        logging.info(f'Parsing {len(lines)} lines')
        self.df_log = self.log_to_dataframe(lines)
        logging.info('Pre-processing done. [Time taken: {!s}]'.format(datetime.now() - start_time))
        return self.parse(persistence)

    def parse(self, persistence=True):
        """
        Main function responsible for parsing the log lines
        contained inside self.df_log["Content"]
        :param persistence: if you want to save the result to file
        :return: dataframe with the log lines split as the regex plus the columns:
            - EventId: hashed value from EventTemplate
            - EventTemplate: template representing the log.
            - ParameterList: if self.keep_para set to True. List of tokens substituted by <*>.
        """
        t0 = datetime.now()
        self.df_log['LineId'] = self.df_log['LineId'].apply(lambda x: x + self.last_line_id)
        self.log_cluster_lines = self.parser.parse(self.df_log["Content"], self.last_line_id)
        t1 = datetime.now()

        logging.info('Parsing done. [Time taken: {!s}]'.format(t1 - t0))

        self.cluster_to_df()

        self.trie_root = self.parser.trieRoot

        if not os.path.exists(self.save_path):
            os.makedirs(self.save_path)
        if persistence:
            self.output_result()

        # Update last_line for next execution if called in batch
        self.set_last_line_id()

        root_node_path = os.path.join(self.save_path, 'rootNode.pkl')
        log_clu_l_path = os.path.join(self.save_path, 'logCluL.pkl')
        logging.info(f'rootNodePath: {root_node_path}')
        with open(root_node_path, 'wb') as output:
            pickle.dump(self.trie_root, output, pickle.HIGHEST_PROTOCOL)
        logging.info(f'logCluLPath: {log_clu_l_path}')
        with open(log_clu_l_path, 'wb') as output:
            pickle.dump(self.log_cluster_lines, output, pickle.HIGHEST_PROTOCOL)

        logging.info('Saving done (persistence={!s}). [Time taken: {!s}]'.format(persistence, datetime.now() - t1))

        return self.df_log

    def cluster_to_df(self):
        if self.df_log.shape[0] == 0:
            return

        templates = [0] * self.df_log.shape[0]
        ids = [0] * self.df_log.shape[0]
        self.df_event = []

        for logClust in self.log_cluster_lines:
            template_str = ' '.join(logClust.logTemplate)
            eid = hashlib.md5(template_str.encode('utf-8')).hexdigest()[0:8]
            for logId in logClust.logIDL:
                if logId <= self.last_line_id:
                    continue
                templates[logId - self.last_line_id - 1] = template_str
                ids[logId - self.last_line_id - 1] = eid
            self.df_event.append([eid, template_str, len(logClust.logIDL)])

        self.df_event = pd.DataFrame(self.df_event, columns=['EventId', 'EventTemplate', 'Occurrences'])

        self.df_log['EventId'] = ids
        self.df_log['EventTemplate'] = templates
        if self.keep_para:
            self.df_log["ParameterList"] = self.df_log.apply(self.get_parameter_list, axis=1)
        logging.info('Output parse file')

    def get_parameter_list(self, row):
        event_template = str(row["EventTemplate"])
        template_regex = re.sub(r"\s<.{1,5}>\s", "<*>", event_template)
        if "<*>" not in template_regex:
            return []
        template_regex = re.sub(r'([^A-Za-z0-9])', r'\\\1', template_regex)
        template_regex = re.sub(r'\\ +', r'[^A-Za-z0-9]+', template_regex)
        template_regex = "^" + template_regex.replace("\<\*\>", "(.*?)") + "$"
        t = self.start_timer(self._parameter_handler)
        try:
            parameter_list = self._get_parameter_list(row, template_regex)
        except Exception as e:
            logging.error(e)
            parameter_list = ["TIMEOUT"]
        self.stop_timer(t)
        return parameter_list

    @staticmethod
    def _get_parameter_list(row, template_regex):
        parameter_list = re.findall(template_regex, row["Content"])
        parameter_list = parameter_list[0] if parameter_list else ()
        parameter_list = list(parameter_list) if isinstance(parameter_list, tuple) else [parameter_list]
        parameter_list = [para.strip(string.punctuation).strip(' ') for para in parameter_list]
        return parameter_list

    def output_result(self):

        if self.main_log_name:
            main_structured_path = os.path.join(self.save_path, self.main_log_name + '_main_structured.csv')
            if os.path.isfile(main_structured_path):
                df_log_main_structured = pd.read_csv(main_structured_path)
                last_main_line_id = df_log_main_structured['LineId'].max()
                # logging.info(f'last_main_line_id: {last_main_line_id}')
                trimmed = self.df_log[self.df_log['LineId'] > last_main_line_id]
                # df_log_main_structured = pd.concat([df_log_main_structured, trimmed])

                trimmed.to_csv(os.path.join(self.save_path, self.main_log_name + '_main_structured.csv'),
                                              header=False, index=False, mode='a')
                self.df_event.to_csv(os.path.join(self.save_path, self.main_log_name + '_main_templates.csv'),
                                     index=False)
            else:
                self.df_log.to_csv(os.path.join(self.save_path, self.main_log_name + '_main_structured.csv'), index=False)
                self.df_event.to_csv(os.path.join(self.save_path, self.main_log_name + '_main_templates.csv'), index=False)

        self.df_log.to_csv(os.path.join(self.save_path, self.log_name + '_structured.csv'), index=False)
        self.df_event.to_csv(os.path.join(self.save_path, self.log_name + '_templates.csv'), index=False)

    @staticmethod
    def start_timer(handler):
        if os.name != 'nt':
            signal.signal(signal.SIGALRM, handler)
            signal.alarm(1)
            return None
        else:
            t = Timer(1.0, handler)
            t.start()
            return t

    @staticmethod
    def stop_timer(timer: Timer):
        if os.name != 'nt':
            signal.alarm(0)
        else:
            timer.cancel()

    @staticmethod
    def _parameter_handler(signum, frame):
        logging.error("_get_parameter_list function is hangs!")
        raise Exception("TIME OUT!")

    @staticmethod
    def _parameter_handler():
        logging.error("_get_parameter_list function is hangs!")
        raise Exception("TIME OUT!")

    @staticmethod
    def _log_to_dataframe_handler():
        logging.error('log_to_dataframe function is hangs')
        raise Exception("TIME OUT!")

    @staticmethod
    def _log_to_dataframe_handler(signum, frame):
        logging.error('log_to_dataframe function is hangs')
        raise Exception("TIME OUT!")


def generate_log_format_regex(log_format):
    """
    Function to generate regular expression to split log messages
    """
    headers = []
    splitters = re.split(r'(<[^<>]+>)', log_format)
    regex = ''
    for k in range(len(splitters)):
        if k % 2 == 0:
            splitter = re.sub(r'\\ +', r' ', splitters[k])
            regex += splitter
        else:
            header = splitters[k].strip('<').strip('>')
            regex += f'(?P<{header}>.*?)'
            headers.append(header)
    regex = re.compile('^' + regex + '$')
    return headers, regex
