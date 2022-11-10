exec(open('spell.py').read())

log = 'HDFS'
base_dir = '/content/'
# input_dir = '.'
input_dir = f'{log}Split/'
output_dir = base_dir+f'{log}_result/'
if log == 'BGL':
    log_format = '<Label> <Timestamp> <Date> <Node> <Time> <NodeRepeat> <Type> <Component> <Level> <Content>'
    tau = 0.75
elif log == 'HDFS':
    log_format = '<Date> <Time> <Pid> <Level> <Component>: <Content>'
    tau = 0.7

parser = LogParser(indir=base_dir+input_dir, outdir=output_dir,
                   log_format=log_format, tau=tau, logmain=log)

df = parser.log_to_dataframe("HDFS_2k.txt")
df.to_csv("HDFS_2k.csv", index= False)

