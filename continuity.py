from cspell import LogParser

from glob import glob
import os
import pandas as pd

from string import ascii_lowercase as abc

log = 'HDFS'
# base_dir = 'C:/Users/Antonio/Desktop/Tesi Magistrale/Codice/Resources/'
base_dir = '/mnt/c/Users/Antonio/Desktop/Tesi Magistrale/Codice/Resources/'
# input_dir = '.'
input_dir = base_dir + f'{log}Split/'
output_dir = f'./{log}_result/'
if log == 'BGL':
    log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
    tau = 0.75
elif log == 'HDFS':
    log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'
    tau = 0.7

parser = LogParser(in_dir=input_dir, out_dir=output_dir, log_format=log_format, tau=tau, log_main=log)

file = f'{log}part' + abc[0 // 26] + abc[0 % 26]
parser.parse_file(file)
parser = LogParser(in_dir=input_dir, out_dir=output_dir, log_format=log_format, tau=tau, log_main=log)
file = f'{log}part' + abc[1 // 26] + abc[1 % 26]
parser.parse_file(file)

