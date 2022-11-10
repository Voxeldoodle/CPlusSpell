import CPlusSpell as cp
import pandas as pd
import spell
import time

trie = cp.TrieNode()
parser = cp.Parser([], trie, .7)

with open("../Resources/HDFS_2k_Content", 'r') as f:
    lines = f.readlines()

out = parser.parse(lines)


log = 'HDFS'
base_dir = '../'
# input_dir = '.'
input_dir = f'{log}Split/'
output_dir = base_dir+f'{log}_result/'
if log == 'BGL':
    log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
    tau = 0.75
elif log == 'HDFS':
    log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'
    tau = 0.7

spellp = spell.LogParser(indir=base_dir + input_dir, outdir=output_dir,
                         log_format=log_format, tau=tau, logmain=log)

lines = [l.strip() for l in lines]
spellp.df_log = pd.DataFrame(lines, columns=["Content"])
spellp.logname = "HDFS"
spellp.lastestLineId = -1
# web_pdb.set_trace()
t0 = time.time()
spellp.outputResult(out)
t1 = time.time()
spellp.df_log.to_csv("cspell_output.csv", index=False)
print(f"Time taken: {t1 - t0}")
