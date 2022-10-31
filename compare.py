import pandas as pd

import spell
# import cspell as cp
import os

# with open("HDFS_2k", 'r') as f:
#     lines = f.readlines()

test = 'py'

log = 'HDFS'
base_dir = './'
# input_dir = '.'
input_dir = f'{log}Split/'
output_dir = f'{log}_result/'
if log == 'BGL':
    log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
    tau = 0.75
elif log == 'HDFS':
    log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'
    tau = 0.7

if not os.path.exists("./DFOutput"):
    os.makedirs("DFOutput")

if test == 'py':
    parser = spell.LogParser(indir=base_dir, outdir="HDFSpy",
                             log_format=log_format, tau=tau, logmain=log)

    df = parser.parseFile("HDFS_2k")
    df.to_csv("DFOutput/spellpy_output.csv", index=False)
else:
    parser = cp.LogParser(in_dir=base_dir, out_dir="HDFSc",
                 log_format=log_format, tau=tau, log_main=log)
    df = parser.parse_file("HDFS_2k")
    df.to_csv("DFOutput/cspell_output.csv", index=False)