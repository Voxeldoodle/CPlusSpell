# import CSpell
# import pandas as pd

# df = pd.read_csv("HDFS_2k.csv")
# # CSpell.parse(df['Content'].tolist())
# CSpell.parse(["Abc","def"])
# obj = CSpell.TemplateCluster(["AC", "jis"], [1,2,3,4])
# CSpell.testCluster(obj)
#
# node = CSpell.TrieNode("piippo", 5)
# print(node)

import CPlusSpell as cp
import pandas as pd
import spell
import numpy as np

trie = cp.TrieNode("",0)
parser = cp.Parser([], trie, .7)

with open("HDFS_2k.txt", 'r') as f:
    lines = f.readlines()

out = parser.parse(lines)


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

spellp = spell.LogParser(indir=base_dir+input_dir, outdir=output_dir,
                   log_format=log_format, tau=tau, logmain=log)

spellp.df_log = pd.DataFrame(np.random.randint(0,100,size=(2000, 1)), columns=list('A'))
spellp.logname = "HDFS"
spellp.outputResult(out)
