import pandas as pd
import spell

with open("HDFS_2k", 'r') as f:
    lines = f.readlines()

log = 'HDFS'
base_dir = './'
# input_dir = '.'
input_dir = f'{log}Split/'
output_dir = base_dir+f'{log}_result/'
if log == 'BGL':
    log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
    tau = 0.75
elif log == 'HDFS':
    log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'
    tau = 0.7

parser = spell.LogParser(indir=base_dir+input_dir, outdir=output_dir,
                         log_format=log_format, tau=tau, logmain=log)

# parser.df_log = pd.DataFrame(lines, columns=["Content"])
df = parser.parseLines(lines)

df.to_csv("spellpy_output.csv", index=False)
df = df[["Content", "EventId","EventTemplate","ParameterList"]]
df.to_csv("trimmed.csv", index = False)
# df['Content'].to_csv("HDFS_2k_Content", index=False)
