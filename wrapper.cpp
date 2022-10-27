#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <map>
#include <optional>
#include <set>
#include <cassert>
#include <fstream>

using namespace std;
namespace py =  pybind11;

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

    Parser() : tau(.7) {}
    Parser(float tau)
            : tau(tau){}
    Parser(vector<TemplateCluster> logClust, TrieNode trieRoot, float tau)
            : logClust(logClust), trieRoot(trieRoot), tau(tau){}

    vector<string> getTemplate(vector<string> lcs) {
//        cout << "getTemplate START" << endl;

        vector<string> res;
        if (lcs.empty())
            return res;

        reverse(lcs.begin(), lcs.end());
        int i = 0;
        for (const string& tok : lcs) {
            i++;
            if (tok == lcs[lcs.size() - 1]){
                res.push_back(tok);
                lcs.erase(lcs.begin() + lcs.size() - 1);
            }else
                res.emplace_back("<*>");
            if (lcs.empty())
                break;
        }
        if (i < lcs.size())
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
            parentn = &parentn->child[tok];
        }
        if (parentn->cluster.has_value())
            parentn->cluster = newCluster;
    }

    vector<string> LCS(vector<string> seq1, vector<string> seq2) {
        vector<vector<int>> lengths(seq1.size()+1,vector<int>(seq2.size()+1));
        int i = 0;
        int j = 0;
        for (const auto &itemI: lengths) {
            for (const auto &itemJ: itemI) {
                if (seq1[i] == seq2[j])
                    lengths[i+1][j+1] = lengths[i][j]+1;
                else
                    lengths[i+1][j+1] = max(lengths[i+1][j], lengths[i][j+1]);
                j++;
            }
            i++;
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
                assert(seq1[lenOfSeq1-1] != seq2[lenOfSeq2-1] && "Error in LCS");
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
        auto maxLen = -1;
        optional<TemplateCluster> maxLCS = nullopt;

        for (TemplateCluster templateCluster : cluster) {
            set<string> tempSet;
            for (const string& w : templateCluster.logTemplate) {
                msgSet.insert(w);
            }
            set<string> intersect;
            set_intersection(msgSet.begin(), msgSet.end(), tempSet.begin(), tempSet.end(),
                             inserter(intersect, intersect.begin()));
            if (intersect.size() < .5 * msgLen)
                continue;
            auto lcs = LCS(logMsg, templateCluster.logTemplate);
            auto lenLcs = lcs.size();
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

        for (TemplateCluster TemplateCluster : cluster) {
            if (TemplateCluster.logTemplate.size() < .5 * constLogMsg.size())
                continue;
            set<string> tokenSet;
            for (string w : constLogMsg) {
                tokenSet.insert(w);
            }
            if (all_of(constLogMsg.cbegin(), constLogMsg.cend(),
                            [&tokenSet](string s) { return s == "<*>" || tokenSet.count(s); }))
                return TemplateCluster;
        }
        return nullopt;
    }

    optional<TemplateCluster> prefixTreeMatch(TrieNode prefixTree, vector<string> constLogMsg, int start) {
//        cout << "prefixTreeMatch START" << endl;

        int i = 0;
        vector<string>::iterator iter;
        for (iter = constLogMsg.begin()+start; iter < constLogMsg.end(); iter++){
            if (prefixTree.child.count(*iter)){
                TrieNode child = prefixTree.child[*iter];
                vector<string> tmp =  child.cluster.value().logTemplate;
                if (!tmp.empty()){
                    vector<string> constLM;
                    copy_if (tmp.begin(), tmp.end(),
                                  back_inserter(constLogMsg),
                                  [](string s){return s != "<*>";});
                    if (constLM.size() >= tau * (*iter).size())
                        return child.cluster;
                }else
                    prefixTreeMatch(child, constLogMsg, i+1);
            }
            i++;
        }
        return nullopt;
    }

    vector<TemplateCluster> parse(const vector<string> content){
//        cout << "parse START" << endl;
        int i = 0;
        for (const string& logMsg : content){
//            cout << "Loop: " << i << " Msg: "<< logMsg << endl;

            int logID = i;
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

                        vector<int> ids;
                        ids.push_back(logID);
                        auto newCluster = TemplateCluster(tokMsg, ids);
                        logClust.push_back(newCluster);
                        addSeqToPrefixTree(trieRoot, newCluster);
                    }else{
//                        cout << "Inner TRUE" << endl;
                        std::string s;
                        for (const auto &piece : matchCluster.value().logTemplate) s += piece;


                        auto newTemplate = getTemplate(LCS(tokMsg, matchCluster.value().logTemplate));
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
            if (i % 10000 == 0 || i == content.size() ){
                printf("Processed %2.2lu%% of log lines.\n", 100*i/content.size());
            }
        }
        return logClust;
    }
};

//void testCluster(const TemplateCluster cls){
//    cout << cls.logTemplate[1] << endl;
//}
//
//void parse(const vector<string> s, float tau=.7){
//    vector<TemplateCluster> vect;
//    parse(s, vect, TrieNode(), tau);
//}

PYBIND11_MODULE(CPlusSpell, m) {
    m.doc() = "Log parsing module spellpy adapted into c++"; // Optional module docstring
//    m.def("parse", py::overload_cast<vector<string>,
//            vector<TemplateCluster>,
//            TrieNode,
//            float>(&parse),
//            "A function which parses the 'Content' section of a log generated from spellpy",
//            py::arg("content"), py::arg("logCLust"), py::arg("trie"), py::arg("tau")=.7);
//    m.def("parse", py::overload_cast<vector<string>, float>(&parse),
//            "A function which parses the 'Content' section of a log generated from spellpy",
//            py::arg("content"), py::arg("tau")=.7);

//    Tentativo di valori default con pybind
//    m.def("parse", py::overload_cast<vector<string>,
//        vector<TemplateCluster>,
//        TrieNode>(&parse),
//        "A function which parses the 'Content' section of a log generated from spellpy",
//        py::arg("content"),
//        py::arg("logCLust") = nullptr, py::arg("trie")=TrieNode());

    py::class_<TemplateCluster>(m, "TemplateCluster")
            .def(py::init<vector<string> &, vector<int> &>(),
                py::arg("logTemplate"), py::arg("logIds"))
            .def_readwrite("logTemplate", &TemplateCluster::logTemplate)
            .def_readwrite("logIDL", &TemplateCluster::logIds);
    py::class_<TrieNode>(m, "TrieNode")
            .def(py::init<>())
            .def(py::init<string &, int &>())
            .def_readwrite("cluster", &TrieNode::cluster)
            .def_readwrite("token", &TrieNode::token)
            .def_readwrite("templateNo", &TrieNode::templateNo)
            .def_readwrite("child", &TrieNode::child);
    py::class_<Parser>(m, "Parser")
        .def(py::init<>())
        .def(py::init<vector<TemplateCluster> &, TrieNode &, float &>())
        .def("parse", &Parser::parse,
            "A function which parses the 'Content' section of a log"
            " generated from spellpy",
            py::arg("content"))
        .def("LCS", &Parser::LCS,
                "Longest Common Subsequence between String Arrays",
                py::arg("seq1"),py::arg("seq2"))
        .def("LCSMatch", &Parser::LCSMatch,
                "Tries to find a match for a logMsg in a List of TemplateCLuster",
                py::arg("cluster"), py::arg("logMsg"))
        .def("addSeqToPrefixTree", &Parser::addSeqToPrefixTree,
                "Add Template to trie",
                py::arg("prefixTreeRoot"), py::arg("newCluster"))
        .def("getTemplate", &Parser::getTemplate,
                "Generate Template from partial message obtained via LCS",
                py::arg("lcs"));

}