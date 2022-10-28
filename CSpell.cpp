#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <map>
#include <optional>
#include <set>
#include <cassert>
#include <chrono>
#include <ctime>
#include <fstream>

using namespace std;

vector<string> split(string s, string delimiter){
    vector<string> res;

    regex rgx(delimiter);
    sregex_token_iterator iter(s.begin(),s.end(),
                               rgx,-1);
    sregex_token_iterator end;
    for ( ; iter != end; ++iter){
        res.push_back(*iter);
    }

    return res;
}

class TemplateCluster {
public:
    vector<string> logTemplate;
    vector<int> logIds;
    TemplateCluster(){}
    TemplateCluster(vector<string> tmp, vector<int> ids)
            : logTemplate(tmp), logIds(ids){}
};

class TrieNode {
public:
    optional<TemplateCluster> cluster;
    string token;
    int templateNo;
    map<string , TrieNode> child;

    TrieNode(){}
    TrieNode(string token, int templateNo)
            : token(token), templateNo(templateNo){}
};

class Parser {
public:
    vector<TemplateCluster> logClust;
    TrieNode trieRoot;
    const float tau;
    int lastLine = 0;

    Parser() : tau(.5) {}
    Parser(float tau)
            : tau(tau){}
    Parser(vector<TemplateCluster> logClust, TrieNode trieRoot, float tau, int lastLine)
            : logClust(logClust), trieRoot(trieRoot), tau(tau), lastLine(lastLine){}

    vector<string> getTemplate(vector<string> lcs, vector<string> seq) {
//        cout << "getTemplate START" << endl;

        vector<string> res;
        if (lcs.empty())
            return res;

        reverse(lcs.begin(), lcs.end());
        int i = 0;
        for (const string& tok : seq) {
            i++;
            if (tok == lcs[lcs.size() - 1]){
                res.push_back(tok);
                lcs.erase(lcs.begin() + lcs.size() - 1);
            }else
                res.emplace_back("<*>");
            if (lcs.empty())
                break;
        }
        if (i < seq.size())
            res.emplace_back("<*>");
        return res;
    }

    void removeSeqFromPrefixTree(TrieNode prefixTreeRoot, TemplateCluster newCluster) {
        auto parentn = prefixTreeRoot;
        vector<string> seq;
        copy_if (newCluster.logTemplate.begin(), newCluster.logTemplate.end(),
                 back_inserter(seq),
                 [](const string& s){return s != "<*>";});
        for (const string& tok : seq) {
            if (parentn.child.count(tok)){
                auto matched = parentn.child[tok];
                if (matched.templateNo == 1){
                    parentn.child.erase(tok);
                    break;
                }else {
                    matched.templateNo--;
                    parentn = matched;
                }
            }
        }
    }

    void addSeqToPrefixTree(TrieNode& prefixTreeRoot, TemplateCluster newCluster) {
//        cout << "addSeqToPrefixTree START" << endl;

        auto parentn = &prefixTreeRoot;
        vector<string> seq;
        copy_if (newCluster.logTemplate.begin(), newCluster.logTemplate.end(),
                 back_inserter(seq),
                 [](const string& s){return s != "<*>";});
        for (const string& tok : seq) {
            if (parentn->child.count(tok))
                parentn->child[tok].templateNo++;
            else
                parentn->child.insert({tok,TrieNode(tok, 1)});
//                parentn->child[tok] = TrieNode(tok, 1);
            parentn = &parentn->child[tok];
        }
        if (!parentn->cluster.has_value())
            parentn->cluster = newCluster;
    }

    vector<string> LCS(vector<string> seq1, vector<string> seq2) {

        vector<vector<int>> lengths(seq1.size()+1,vector<int>(seq2.size()+1));
        for (int i = 0; i < seq1.size() ; i++){
            for (int j = 0; j < seq2.size(); j++) {
//                printf("i: %d j:%d\n", i, j);
                if (seq1[i] == seq2[j])
                    lengths[i+1][j+1] = lengths[i][j]+1;
                else
                    lengths[i+1][j+1] = max(lengths[i+1][j], lengths[i][j+1]);
            }
        }
        vector<string> result;
        auto lenOfSeq1= seq1.size();
        auto lenOfSeq2 = seq2.size();
        while (lenOfSeq1 != 0 && lenOfSeq2 != 0){
            if (lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1-1][lenOfSeq2])
                lenOfSeq1--;
            else if (lengths[lenOfSeq1][lenOfSeq2] == lengths[lenOfSeq1][lenOfSeq2-1])
                lenOfSeq2--;
            else{
                assert(seq1[lenOfSeq1-1] == seq2[lenOfSeq2-1] && "Error in LCS");
                result.insert(result.begin(), seq1[lenOfSeq1-1]);
                lenOfSeq1--;
                lenOfSeq2--;
            }
        }
        return result;
    }

    [[nodiscard]] optional<TemplateCluster> LCSMatch(vector<TemplateCluster> cluster, vector<string> logMsg) {
//        cout << "LCSMatch START" << endl;

        optional<TemplateCluster> res = nullopt;
        set<string> msgSet;
        for (const string& w : logMsg) {
            msgSet.insert(w);
        }
        auto msgLen = logMsg.size();
        int maxLen = -1;
        optional<TemplateCluster> maxLCS = nullopt;

        for (TemplateCluster templateCluster : cluster) {
            set<string> tempSet;
            for (auto w : templateCluster.logTemplate) {
                tempSet.insert(w);
            }
            set<string> intersect;
            set_intersection(msgSet.begin(), msgSet.end(), tempSet.begin(), tempSet.end(),
                             inserter(intersect, intersect.begin()));
            if (intersect.size() < .5 * msgLen)
                continue;
            auto lcs = LCS(logMsg, templateCluster.logTemplate);
            int lenLcs = lcs.size();
            if (lenLcs > maxLen ||
                (lenLcs == maxLen &&
                 templateCluster.logTemplate.size() < maxLCS.value().logTemplate.size())){
                maxLen = lenLcs;
                maxLCS = templateCluster;
            }
        }

        if (maxLen >= tau * msgLen)
            res = maxLCS;

//        cout << "LCSMatch END" << endl;

        return res;
    }

    optional<TemplateCluster> simpleLoopMatch(vector<TemplateCluster> cluster, vector<string> constLogMsg) {
//        cout << "simpleLoopMatch START" << endl;

        for (TemplateCluster templateCluster : cluster) {
            if (templateCluster.logTemplate.size() < .5 * constLogMsg.size())
                continue;
            set<string> tokenSet;
            for (string w : constLogMsg) {
                tokenSet.insert(w);
            }
            if (all_of(templateCluster.logTemplate.cbegin(), templateCluster.logTemplate.cend(),
                       [&tokenSet](const string& tok) { return tok == "<*>" || tokenSet.count(tok); }))
                return templateCluster;
        }
        return nullopt;
    }

    optional<TemplateCluster> prefixTreeMatch(TrieNode prefixTree, vector<string> constLogMsg, int start) {
//        cout << "prefixTreeMatch START" << endl;
        for (int i = start; i < constLogMsg.size(); i++) {
            if (prefixTree.child.count(constLogMsg[i])){
                TrieNode child = prefixTree.child.at(constLogMsg[i]);
                if (child.cluster.has_value()){
                    vector<string> tmp =  child.cluster.value().logTemplate;
                    vector<string> constLM;
                    copy_if (tmp.begin(), tmp.end(),
                             back_inserter(constLogMsg),
                             [](string s){return s != "<*>";});
                    if (constLM.size() >= tau * (constLogMsg[i]).size())
                        return child.cluster;
                }else
                    prefixTreeMatch(child, constLogMsg, i+1);
            }
        }
        return nullopt;
    }

    vector<TemplateCluster> parse(const vector<string> content){
//        cout << "parse START" << endl;
        int i = 0;
        for (const string& logMsg : content){
//            cout << "Loop: " << i << " Msg: "<< logMsg << endl;

            int logID = i + lastLine;
            vector<string> tokMsg = split(logMsg, "[\\s=:,]");
            vector<string> constLogMsg;
            copy_if (tokMsg.begin(), tokMsg.end(),
                     back_inserter(constLogMsg),
                     [](string s){return s != "<*>";});

            optional<TemplateCluster> matchCluster = prefixTreeMatch(trieRoot, constLogMsg, 0);
            if (!matchCluster.has_value()){
                matchCluster = simpleLoopMatch(logClust, constLogMsg);
                if (!matchCluster.has_value()){
                    matchCluster = LCSMatch(logClust, tokMsg);
                    if (!matchCluster.has_value()){
//                        cout << "Inner FALSE" << endl;

                        vector<int> ids = {logID};
                        auto newCluster = TemplateCluster(tokMsg, ids);
                        logClust.push_back(newCluster);
                        addSeqToPrefixTree(trieRoot, newCluster);
                    }else{
//                        cout << "Inner TRUE" << endl;
                        std::string s;
                        for (const auto &piece : matchCluster.value().logTemplate) s += piece;


                        auto newTemplate = getTemplate(LCS(tokMsg, matchCluster.value().logTemplate),
                                                       matchCluster.value().logTemplate);
                        if (newTemplate != matchCluster.value().logTemplate){
                            removeSeqFromPrefixTree(trieRoot, matchCluster.value());
                        }
                    }
                }
            }
            if (matchCluster.has_value()){
//                cout << "Outer TRUE" << endl;

                for (TemplateCluster logCluster : logClust) {
                    if (matchCluster.value().logTemplate == logCluster.logTemplate) {
                        logCluster.logIds.push_back(logID);
                        break;
                    }
                }
            }
            i++;
            if (i % 10000 == 0 || (i > 20000 && i%100 == 0) || i == content.size() ){
                auto now = chrono::system_clock::now();
                auto time = chrono::system_clock::to_time_t(now);
//                chrono::duration<double> elapsed_seconds = end-start;
//                elapsed_seconds.count()
                printf("%s Processed %2.2lu%% of log lines.\n",ctime(&time), 100*i/content.size());
            }
            if (i > 24660)
                cout << "BREAk" << endl;
        }
        return logClust;
    }
};

int main()
{
//    vector<string> lines = {"PacketResponder 1 for block blk_38865049064139660 terminating",
//                            "PacketResponder 0 for block blk_-6952295868487656571 terminating",
//                            "10.251.73.220:50010 is added to blk_7128370237687728475 size 67108864"};
    string line;
    vector<string> lines;
    ifstream myfile("../HDFS100k");
    if(!myfile) //Always test the file open.
    {
        std::cout<<"Error opening output file"<< std::endl;
        return -1;
    }
    while (getline(myfile, line))
        lines.push_back(line);

    auto p = Parser(.7);
    auto out =  p.parse(lines);

    cout << "OUT" << endl;
}