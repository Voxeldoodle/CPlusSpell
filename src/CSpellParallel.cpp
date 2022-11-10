#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <map>
#include <optional>
#include <set>
#include <cassert>
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

vector<string> getTemplate(vector<string> lcs, vector<string> seq) {
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

class TemplateCluster {
public:
    vector<string> logTemplate;
    vector<int> logIds;

    mutable shared_mutex mutex;
    mutable shared_mutex switchLock;

    TemplateCluster(){}
    TemplateCluster(vector<string> tmp, vector<int> ids)
            : logTemplate(tmp), logIds(ids){}

    TemplateCluster(const TemplateCluster& other) {
//        lock_guard<shared_mutex> l(other.mutex);
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

    bool promoteLock(){
        bool res = switchLock.try_lock();
        if (!res)
            return false;
        mutex.unlock_shared();
        mutex.lock();
        return true;
    }

    void demoteLock(){
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

    optional<TemplateCluster> getCluster() const{
        return cluster;
    }

    bool promoteLock(){
        bool res = switchLock.try_lock();
        if (!res)
            return false;
        mutex.unlock_shared();
        mutex.lock();
        return true;
    }

    void demoteLock(){
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

    void removeSeqFromPrefixTree(TrieNode& prefixTreeRoot, vector<string> logTemplate){
//        printf("ID: %d removeSeqFromPrefixTree\n", id);

        auto parentIter = &prefixTreeRoot;
        vector<string> seq;
        copy_if (logTemplate.begin(), logTemplate.end(),
                 back_inserter(seq),
                 [](const string& s){return s != "<*>";});
        (*parentIter).mutex.lock_shared();
        for (const string& tok : seq) {
            // Assume parentIter locked as Shared

            checkExists:
            // WARNING: possible deletion after finding the branch.
            if ((*parentIter).child.count(tok)){
                parentIter->child[tok].mutex.lock_shared();
                auto matched = &(*parentIter).child[tok];
                if ((*matched).templateNo == 1){
                    if (!(*parentIter).promoteLock()){
                        (*parentIter).mutex.unlock_shared();
                        (*parentIter).mutex.lock_shared();
                        goto checkExists;
                    }
                    // WARNING: possible unsafe destruction of branch with active locks.
                    (*parentIter).child.erase(tok);
                    (*parentIter).demoteLock();
                    break;
                }else {
                    if (!(*matched).promoteLock()){
                        (*matched).mutex.unlock_shared();
                        goto checkExists;
                    }
                    (*matched).templateNo--;
                    (*matched).demoteLock();
                    auto old = parentIter;
                    parentIter = matched;
                    (*old).mutex.unlock_shared();
                }
            }
        }
        (*parentIter).mutex.unlock_shared();
    }

    void addSeqToPrefixTree(TrieNode& prefixTreeRoot, TemplateCluster newCluster){
//        printf("ID: %d addSeqToPrefixTree\n", id);

        auto parentIter = &prefixTreeRoot;
        vector<string> seq;
        copy_if (newCluster.logTemplate.begin(), newCluster.logTemplate.end(),
                 back_inserter(seq),
                 [](const string& s){return s != "<*>";});
        (*parentIter).mutex.lock_shared();
        for (const string& tok : seq) {
            // Assume parentIter locked as Shared
            checkBranch:
            if (parentIter->child.count(tok))
                parentIter->child[tok].templateNo++;
            else{
                if (!(*parentIter).promoteLock()){
                    (*parentIter).mutex.unlock_shared();
                    (*parentIter).mutex.lock_shared();
                    goto checkBranch;
                }
                parentIter->child.insert({tok,TrieNode(tok, 1)});
                (*parentIter).demoteLock();
            }
            auto old = parentIter;
            // Lock child and unlock parent
            parentIter->child[tok].mutex.lock_shared();
            parentIter = &parentIter->child[tok];
            (*old).mutex.unlock_shared();
        }
        finalCheck:
        // If empty leaf add cluster, else unlock node
        if (!parentIter->cluster.has_value()) {
            if (!(*parentIter).promoteLock()){
                (*parentIter).mutex.unlock_shared();
                (*parentIter).mutex.lock_shared();
                goto finalCheck;
            }
            parentIter->cluster = newCluster;
            (*parentIter).writeUnlock();
        }else {
            (*parentIter).mutex.unlock_shared();
        }
    }

    optional<TemplateCluster*> LCSMatch(vector<TemplateCluster> &cluster, vector<string> logMsg){
        /*
         * Returns reference to matching TemplateCluster locked as shared.
         */
//        printf("ID: %d LCSMatch\n", id);

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
                // Unlock previous maximum
                if (maxLCS.has_value())
                    (*maxLCS.value()).mutex.unlock_shared();
                maxLCS = optional(&templateCluster);
            }

            // Keep maximum value locked
            if (maxLCS.value() != &templateCluster){
                templateCluster.mutex.unlock_shared();
            }
        }

        if (maxLen >= tau * msgLen)
            res = maxLCS;
        else {
            // Unlock max cluster if not selected
            if (maxLCS.has_value())
                (*maxLCS.value()).mutex.unlock_shared();
        }

        return res;
    }

    optional<TemplateCluster*> simpleLoopMatch(vector<TemplateCluster> &cluster, vector<string> constLogMsg){
        /*
         * Returns reference to matching TemplateCluster locked as shared.
         */
//        printf("ID: %d simpleLoopMatch\n", id);

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
//                templateCluster.mutex.unlock_shared();
                return &templateCluster;
            }
            templateCluster.mutex.unlock_shared();
        }
        return nullopt;
    }

    vector<string> prefixTreeMatch(TrieNode &prefixTree, vector<string> constLogMsg, int start){
//        printf("ID: %d prefixTreeMatch\n", id);

        prefixTree.mutex.lock_shared();
        for (int i = start; i < constLogMsg.size(); i++) {
//            WARNING: possible deletion after finding the branch.
            if (prefixTree.child.count(constLogMsg[i])){
                prefixTree.child.at(constLogMsg[i]).mutex.lock_shared();
                TrieNode *child = &(prefixTree.child.at(constLogMsg[i]));
                if ((*child).cluster.has_value()) {
                    vector<string> tmp = (*child).cluster.value().logTemplate;
                    vector<string> constLM;
                    copy_if(tmp.begin(), tmp.end(),
                            back_inserter(constLM),
                            [](string s) { return s != "<*>"; });
                    if (constLM.size() >= tau * constLogMsg.size()){
                        (*child).mutex.unlock_shared();
                        prefixTree.mutex.unlock_shared();
                        return (*child).cluster.value().logTemplate;
                    }
                } else {
                    auto res = prefixTreeMatch((*child), constLogMsg, i + 1);
                    (*child).mutex.unlock_shared();
                    prefixTree.mutex.unlock_shared();
                    return res;
                }
                (*child).mutex.unlock_shared();
            }
        }
        prefixTree.mutex.unlock_shared();
        return {};
//        return vector<string>();
    }

    void parallel_parse(vector<string> content, int end, int lastLine=0, int ID=0){
        printf("ID: %d start: %d end: %d.\n", ID, lastLine, end);

        this->id = ID;
        for (int i = lastLine+1; i <= end; i++) {
//            printf("ID: %d line: %d.\n", ID, i);
            int logID = i;
            vector<string> tokMsg = split(content.at(i-1), "[\\s=:,]");
            vector<string> constLogMsg;
            copy_if (tokMsg.begin(), tokMsg.end(),
                     back_inserter(constLogMsg),
                     [](string s){return s != "<*>";});
            vector<string> templateMatch = prefixTreeMatch(trieRoot, constLogMsg, 0);
            if (templateMatch.empty()){
                clustLock.lock_shared();
                optional<TemplateCluster *> matchCluster = simpleLoopMatch(logClust, constLogMsg);
                clustLock.unlock_shared();
                if (!matchCluster.has_value()){
                    clustLock.lock_shared();
                    matchCluster = LCSMatch(logClust, tokMsg);
                    clustLock.unlock_shared();
                    lockCheckpoint:
                    if (!matchCluster.has_value()){
                        vector<int> ids = {logID};
                        auto newCluster = TemplateCluster(tokMsg, ids);
//                        printf("ID: %d ADDING\n", id);
                        clustLock.lock();
                        logClust.push_back(newCluster);
                        addSeqToPrefixTree(trieRoot, newCluster);
                        clustLock.unlock();
                    }else{
                        auto matchClustTemplate = matchCluster.value()->logTemplate;
                        auto newTemplate = getTemplate(LCS(tokMsg, matchClustTemplate), matchClustTemplate);
                        if (newTemplate != matchClustTemplate){
//                            printf("ID: %d UPDATING\n", id);
                            removeSeqFromPrefixTree(trieRoot, matchCluster.value()->logTemplate);
                            if (!(*matchCluster.value()).promoteLock()){
                                (*matchCluster.value()).mutex.unlock_shared();
                                (*matchCluster.value()).mutex.lock_shared();
                                goto lockCheckpoint;
                            }
                            (*matchCluster.value()).logTemplate = newTemplate;
                            (*matchCluster.value()).demoteLock();
                            addSeqToPrefixTree(trieRoot, *matchCluster.value());
                        }
                        templateMatch = matchCluster.value()->logTemplate;
                        (*matchCluster.value()).mutex.unlock_shared();
                    }
                }else{
                    templateMatch = matchCluster.value()->logTemplate;
                    (*matchCluster.value()).mutex.unlock_shared();
                }
            }
            if (!templateMatch.empty()){
                clustLock.lock_shared();
                for (TemplateCluster& cluster : logClust) {
                    clusterCheckpoint:
                    cluster.mutex.lock_shared();
                    if (templateMatch == cluster.logTemplate) {
                        if (!cluster.promoteLock()){
                            cluster.mutex.unlock_shared();
                            goto clusterCheckpoint;
                        }
                        cluster.logIds.push_back(logID);
                        cluster.writeUnlock();
                        break;
                    }
                    cluster.mutex.unlock_shared();
                }
                clustLock.unlock_shared();
            }
        }

    }

    vector<TemplateCluster> parse(const vector<string> content, const int lastLine=0){
        vector<thread> threads;
//        int tMax = thread::hardware_concurrency()/2;
        int tMax = min(((int)thread::hardware_concurrency()), 4);
//        int tMax = 2;

        int chunk = (int)content.size() / tMax;
        for (int i = 0; i < tMax; ++i) {
            int start = chunk * i;
            threads.emplace_back(&Parser::parallel_parse, this,content, chunk * (i+1), start, i);
        }

        for (auto& th : threads)
            th.join();

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

//    ifstream myFile("../HDFS100k");
//    ifstream myFile("../HDFS_2k_Content");
    ifstream myFile("../HDFSpartaa");
    if(!myFile) //Always test the file open.
    {
        std::cout<<"Error opening output file"<< std::endl;
        return -1;
    }
    while (getline(myFile, line))
        lines.push_back(line);

    auto p = Parser(.7);
    auto out =  p.parse(lines, 0);

    cout << "OUT" << endl;
}