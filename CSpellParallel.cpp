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
#include <shared_mutex>
#include <thread>

using namespace std;

vector<string> split(string s, const string& delimiter){
    vector<string> res;

    regex rgx(delimiter);
    sregex_token_iterator iter(s.begin(),s.end(),
                               rgx,-1);
    sregex_token_iterator end;
    for ( ; iter != end; ++iter){
        if (*iter != "")
            res.push_back(*iter);
    }

    return res;
}

class TemplateCluster {
public:
    vector<string> logTemplate;
    vector<int> logIds;

    mutable shared_mutex mutex;

    TemplateCluster(){}
    TemplateCluster(vector<string> tmp, vector<int> ids)
            : logTemplate(tmp), logIds(ids){}

    TemplateCluster(const TemplateCluster& other) {
        lock_guard<shared_mutex> l(other.mutex);
        logTemplate = other.logTemplate;
        logIds = other.logIds;
    }
    TemplateCluster(TemplateCluster&& other)  noexcept {
        lock_guard<shared_mutex> l(other.mutex);
        logTemplate = other.logTemplate;
        logIds = other.logIds;
    }
    TemplateCluster& operator=(TemplateCluster&& other)  noexcept {
        lock_guard<shared_mutex> l1(this->mutex), l2(other.mutex);
        swap(logTemplate, other.logTemplate);
        swap(logIds, other.logIds);
        return *this;
    }
};

class TrieNode {
public:
    optional<TemplateCluster> cluster;
    string token;
    int templateNo;
    map<string , TrieNode> child;

    mutable shared_mutex mutex;
    mutable shared_mutex switchLock;

    TrieNode(){}
    TrieNode(string token, int templateNo)
            : token(std::move(token)), templateNo(templateNo){}

    TrieNode(const optional<TemplateCluster> &cluster,
             string token,
             int templateNo,
             const map<string, TrieNode> &child) :
             cluster(cluster), token(std::move(token)), templateNo(templateNo), child(child) {}

    TrieNode(const TrieNode& other) {
        lock_guard<shared_mutex> l(other.mutex);
        cluster = *other.cluster;
        token = other.token;
        templateNo = other.templateNo;
        child = other.child;
    }
    TrieNode(TrieNode&& other)  noexcept {
        lock_guard<shared_mutex> l(other.mutex);
        swap(cluster, other.cluster);
        token = other.token;
        templateNo = other.templateNo;
        child = other.child;
    }
    TrieNode& operator=(TrieNode&& other)  noexcept {
        lock_guard<shared_mutex> l1(this->mutex), l2(other.mutex);
        swap(cluster, other.cluster);
        swap(token, other.token);
        swap(templateNo, other.templateNo);
        swap(child, other.child);
        return *this;
    }
    TrieNode& operator=(const TrieNode&& other)  noexcept {
        lock_guard<shared_mutex> l1(this->mutex), l2(other.mutex);
        cluster = *other.cluster;
        token = other.token;
        templateNo = other.templateNo;
        child = other.child;
        return *this;
    }

    void promoteLock(){
        switchLock.lock();
        mutex.unlock_shared();
        mutex.lock();
        switchLock.unlock();
    }
    void demoteLock(){
        switchLock.lock();
        mutex.unlock();
        mutex.lock_shared();
        switchLock.unlock();
    }

    void writeLock(){
        switchLock.lock();
        mutex.lock();
    }

    void writeUnlock(){
        mutex.unlock();
        switchLock.unlock();
    }
};

class Parser{
public:
    vector<TemplateCluster> logClust;
    TrieNode trieRoot;
    float tau;
    int id = 0;
    mutable shared_mutex clustLock;

    Parser() : tau(.5) {}
    explicit Parser(float tau)
            : tau(tau){}
    Parser(vector<TemplateCluster> logClust, TrieNode trieRoot, float tau)
            : logClust(logClust), trieRoot(trieRoot), tau(tau){}

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

    void removeSeqFromPrefixTree(TrieNode& prefixTreeRoot, TemplateCluster cluster) {
        printf("id: %d %s\n", id, "removeSeqFromPrefixTree");
        auto parentn = &prefixTreeRoot;
        vector<string> seq;
        copy_if (cluster.logTemplate.begin(), cluster.logTemplate.end(),
                 back_inserter(seq),
                 [](const string& s){return s != "<*>";});
        for (const string& tok : seq) {
            (*parentn).mutex.lock_shared();
            if ((*parentn).child.count(tok)){
                auto matched = &(*parentn).child[tok];
                if ((*matched).templateNo == 1){
                    (*parentn).promoteLock();
                    (*parentn).child.erase(tok);
                    (*parentn).mutex.unlock();
                    break;
                }else {
                    (*matched).templateNo--;
                    auto old = parentn;
                    parentn = matched;
                    (*old).mutex.unlock_shared();
                }
            }
        }
    }

    void addSeqToPrefixTree(TrieNode& prefixTreeRoot, TemplateCluster newCluster) {
//        cout << "addSeqToPrefixTree START" << endl;
        printf("id: %d %s\n", id, "addSeqToPrefixTree");

        auto parentn = &prefixTreeRoot;
        vector<string> seq;
        copy_if (newCluster.logTemplate.begin(), newCluster.logTemplate.end(),
                 back_inserter(seq),
                 [](const string& s){return s != "<*>";});
        for (const string& tok : seq) {
            (*parentn).mutex.lock_shared();
            if (parentn->child.count(tok))
                parentn->child[tok].templateNo++;
            else{
                (*parentn).promoteLock();
//                (*parentn).mutex.unlock_shared();
//                (*parentn).mutex.lock();
                parentn->child.insert({tok,TrieNode(tok, 1)});
//                parentn->child[tok] = TrieNode(tok, 1);
                (*parentn).demoteLock();
//                (*parentn).mutex.unlock();
//                (*parentn).mutex.lock_shared();
            }
            auto old = parentn;
            parentn = &parentn->child[tok];
            (*old).mutex.unlock_shared();
        }
        if (!parentn->cluster.has_value()) {
            (*parentn).writeLock();
            parentn->cluster = newCluster;
            (*parentn).writeUnlock();
        }
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

    optional<TemplateCluster*> LCSMatch(vector<TemplateCluster> &cluster, vector<string> logMsg) {
//        cout << "LCSMatch START" << endl;
        printf("id: %d %s\n", id, "LCSMatch");

        optional<TemplateCluster *> res;
        set<string> msgSet;
        for (const string& w : logMsg) {
            msgSet.insert(w);
        }
        double msgLen = logMsg.size();
        int maxLen = -1;
        optional<TemplateCluster *> maxLCS;

        for (TemplateCluster& templateCluster : cluster) {
            templateCluster.mutex.lock_shared();
            set<string> tempSet;
            for (auto w : templateCluster.logTemplate) {
                tempSet.insert(w);
            }
            set<string> intersect;
            set_intersection(msgSet.begin(), msgSet.end(), tempSet.begin(), tempSet.end(),
                             inserter(intersect, intersect.begin()));
            if (intersect.size() < .5 * msgLen) {
                templateCluster.mutex.unlock_shared();
                continue;
            }
            auto lcs = LCS(logMsg, templateCluster.logTemplate);
            int lenLcs = lcs.size();
            if (lenLcs > maxLen ||
                (lenLcs == maxLen &&
                 templateCluster.logTemplate.size() < (*maxLCS.value()).logTemplate.size())){
                maxLen = lenLcs;
                maxLCS = optional(&templateCluster);
            }
            templateCluster.mutex.unlock_shared();
        }

        if (maxLen >= tau * msgLen)
            res = maxLCS;

//        cout << "LCSMatch END" << endl;

        return res;
    }

    optional<TemplateCluster*> simpleLoopMatch(vector<TemplateCluster> &cluster, vector<string> constLogMsg) {
//        cout << "simpleLoopMatch START" << endl;
        printf("id: %d %s\n", id, "loopMatch");

        for (TemplateCluster& templateCluster : cluster) {
            templateCluster.mutex.lock_shared();
            if (templateCluster.logTemplate.size() < .5 * constLogMsg.size()) {
                templateCluster.mutex.unlock_shared();
                continue;
            }
            set<string> tokenSet;
            for (string w : constLogMsg) {
                tokenSet.insert(w);
            }
            if (all_of(templateCluster.logTemplate.cbegin(), templateCluster.logTemplate.cend(),
                       [&tokenSet](const string &tok) { return tok == "<*>" || tokenSet.count(tok); })) {
                templateCluster.mutex.unlock_shared();
                return &templateCluster;
            }
            templateCluster.mutex.unlock_shared();
        }
        return nullopt;
    }

    optional<TemplateCluster*> prefixTreeMatch(TrieNode &prefixTree, vector<string> constLogMsg, int start) {
//        cout << "prefixTreeMatch START" << endl;
//        printf("id: %d %s\n", id, "trieMatch");

        for (int i = start; i < constLogMsg.size(); i++) {
            prefixTree.mutex.lock_shared();
            if (prefixTree.child.count(constLogMsg[i])){
                prefixTree.child.at(constLogMsg[i]).mutex.lock_shared();
                TrieNode *child = &(prefixTree.child.at(constLogMsg[i]));
                if ((*child).cluster.has_value()) {
                    vector<string> tmp = (*child).cluster.value().logTemplate;
                    vector<string> constLM;
                    copy_if(tmp.begin(), tmp.end(),
                            back_inserter(constLM),
                            [](string s) { return s != "<*>"; });
                    if (constLM.size() >= tau * constLogMsg.size()) {
                        (*child).mutex.unlock_shared();
                        prefixTree.mutex.unlock_shared();
//                        return child.cluster.has_value() ? child.cluster : nullopt;
                        return &((*child).cluster.value());
                    }
                } else {
                    auto res = prefixTreeMatch((*child), constLogMsg, i + 1);
                    (*child).mutex.unlock_shared();
                    prefixTree.mutex.unlock_shared();
                    return res;
                }

                (*child).mutex.unlock_shared();
            }
            prefixTree.mutex.unlock_shared();
        }
        return nullopt;
    }

    vector<TemplateCluster> parse(vector<string> content, int end, int lastLine=0, int ID=0){
//        cout << "parse START" << endl;
        printf("ID: %d start: %d end: %d.\n", ID, lastLine, end);

//        int i = 1;
        this->id = ID;
        for (int i = lastLine+1; i <= end; i++) {
            printf("ID: %d line: %d.\n", ID, i);
            int logID = i;
            vector<string> tokMsg = split(content.at(i-1), "[\\s=:,]");
            vector<string> constLogMsg;
            copy_if (tokMsg.begin(), tokMsg.end(),
                     back_inserter(constLogMsg),
                     [](string s){return s != "<*>";});

            optional<TemplateCluster *>  matchCluster = prefixTreeMatch(trieRoot, constLogMsg, 0);
            if (!matchCluster.has_value()){
                matchCluster = simpleLoopMatch(logClust, constLogMsg);
                if (!matchCluster.has_value()){
                    matchCluster = LCSMatch(logClust, tokMsg);
//                    matchCluster = LCSMatch(logClust, tokMsg);
                    if (!matchCluster.has_value()){
//                        cout << "Inner FALSE" << endl;

                        vector<int> ids = {logID};
                        auto newCluster = TemplateCluster(tokMsg, ids);
                        printf("ID: %d %s\n", ID, "ADD");
                        clustLock.lock();
                        logClust.push_back(newCluster);
                        addSeqToPrefixTree(trieRoot, newCluster);
                        clustLock.unlock();
                    }else{
//                        cout << "Inner TRUE" << endl;
                        (*matchCluster.value()).mutex.lock_shared();
                        auto matchClustTemp = (*matchCluster.value()).logTemplate;
                        (*matchCluster.value()).mutex.unlock_shared();
                        auto newTemplate = getTemplate(LCS(tokMsg, matchClustTemp), matchClustTemp);
                        if (newTemplate != matchClustTemp){
                            printf("ID: %d %s\n", ID, "UPDATE");
                            removeSeqFromPrefixTree(trieRoot, *matchCluster.value());
                            (*matchCluster.value()).mutex.lock();
                            (*matchCluster.value()).logTemplate = newTemplate;
                            (*matchCluster.value()).mutex.unlock();
                            addSeqToPrefixTree(trieRoot, *matchCluster.value());
                        }
                    }
                }
            }
            if (matchCluster.has_value()){
//                cout << "Outer TRUE" << endl;

                for (TemplateCluster& cluster : logClust) {
                    cluster.mutex.lock_shared();
                    if ((*matchCluster.value()).logTemplate == cluster.logTemplate) {
                        cluster.mutex.unlock_shared();
                        cluster.mutex.lock();
                        cluster.logIds.push_back(logID);
                        cluster.mutex.unlock();
                        break;
                    }
                    cluster.mutex.unlock_shared();

                }
            }
            if (i % 10000 == 0 || i == content.size() ){
                auto now = chrono::system_clock::now();
                auto time = chrono::system_clock::to_time_t(now);
                auto timestamp = strtok(ctime(&time), "\n");
//                chrono::duration<double> elapsed_seconds = end-start;
//                elapsed_seconds.count()
                printf("ID: %d [%s] Processed %2.2lu%% of log lines.\n",id, timestamp, 100*i/content.size());
//                printf("%s Processed %2.2lu%% of log lines.\n",ctime(&time), 100*i/content.size());
            }
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

//    ifstream myfile("../HDFS100k");
    ifstream myfile("../HDFS_2k_Content");
//    ifstream myfile("../HDFSpartaa");
    if(!myfile) //Always test the file open.
    {
        std::cout<<"Error opening output file"<< std::endl;
        return -1;
    }
    while (getline(myfile, line))
        lines.push_back(line);

    auto p = Parser(.7);
//    auto out =  p.parse(lines, lines.size(), 0, 0);
//    return 0;
//    int tMax = thread::hardware_concurrency();
    int tMax = 2;

    vector<thread> threads;

    int chunk = (int)lines.size() / tMax;
    for (int i = 0; i < tMax; ++i) {
        int start = chunk * i;
//        thread t = thread(&Parser::parse, &p,lines, start, i);
//        threads.push_back(std::move(t));
        threads.emplace_back(&Parser::parse, &p,lines, chunk * (i+1), start, i);
    }

    for (auto& th : threads)
        th.join();

    cout << "OUT" << endl;
}