import unittest
import re
import os
import pandas as pd
from pandas.testing import assert_frame_equal
from spell import LogParser
import CPlusSpell as cp

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FORMAT = '<Date> <Time> <Pid> <Level> <Component>: <Content>'

mock = {
    'LineId': [1, 2, 3],
    'Date': ['081109', '081109', '081109'],
    'Time': ['203518', '203518', '203519'],
    'Pid': ['143', '35', '143'],
    'Level': ['INFO', 'INFO', 'INFO'],
    'Component': ['dfs.DataNode$DataXceiver', 'dfs.FSNamesystem', 'dfs.DataNode$DataXceiver'],
    'Content': [
        'Receiving block blk_-1608999687919862906 src: /10.250.19.102:54106 dest: /10.250.19.102:50010',
        'BLOCK* NameSystem.allocateBlock: /mnt/hadoop/mapred/system/job_200811092030_0001/job.jar. blk_-1608999687919862906',
        'Receiving block blk_-1608999687919862906 src: /10.250.10.6:40524 dest: /10.250.10.6:50010'
    ],
}
DF_MOCK = pd.DataFrame(mock)


class TestLogParser(unittest.TestCase):
    def setUp(self):
        self.cpParser = cp.Parser()

    def test_addSeqToPrefixTree(self):
        logmessageL = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '/10.250.19.102', '54106', 'dest', '/10.250.19.102', '50010']
        logID = 0

        rootNode = cp.TrieNode()
        newCluster = cp.TemplateCluster(logmessageL, [logID])

        self.cpParser.addSeqToPrefixTree(rootNode, newCluster)
        res = helper(rootNode)
        self.assertEqual(res, logmessageL)

    def test_LCS(self):
        seq1 = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '/10.250.10.6', '40524', 'dest', '/10.250.10.6', '50010']
        seq2 = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '/10.250.19.102', '54106', 'dest', '/10.250.19.102', '50010']
        expected_lcs = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', 'dest', '50010']

        lcs = self.cpParser.LCS(seq1, seq2)
        self.assertListEqual(lcs, expected_lcs)

    def test_LCSMatch(self):
        seq1 = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '/10.250.10.6', '40524', 'dest', '/10.250.10.6', '50010']
        seq2 = ['Just', 'A', 'Test']
        logmessageL = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '/10.250.19.102', '54106', 'dest', '/10.250.19.102', '50010']
        logID = 0
        newCluster = cp.TemplateCluster(logmessageL, [logID])

        retLogClust = self.cpParser.LCSMatch([newCluster], seq1)
        self.assertListEqual(retLogClust.logTemplate, newCluster.logTemplate)

        ret = self.cpParser.LCSMatch([newCluster], seq2)
        self.assertEqual(ret, None)

    def test_getTemplate(self):
        lcs = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', 'dest', '50010']
        seq = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '/10.250.19.102', '54106', 'dest', '/10.250.19.102', '50010']
        expected_template = ['Receiving', 'block', 'blk_-1608999687919862906', 'src', '<*>', '<*>', 'dest', '<*>', '50010']

        new_template = self.parser.getTemplate(lcs, seq)
        self.assertListEqual(new_template, expected_template)


def helper(rootNode):
    if rootNode.child == dict():
        return []
    res = []
    for k in rootNode.child.keys():
        res.append(k)
        res += helper(rootNode.child[k])
    return res


if __name__ == '__main__':
    unittest.main()
