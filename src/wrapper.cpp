#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include "CSpell.cpp"

namespace py =  pybind11;

PYBIND11_MODULE(CPlusSpell, m) {
    m.doc() = "Log parsing module spellpy adapted into c++"; // Optional module docstring

    py::class_<TemplateCluster>(m, "TemplateCluster")
            .def(py::init<vector<string> &, vector<int> &>(),
                py::arg("logTemplate"), py::arg("logIds"))
            .def_readwrite("logTemplate", &TemplateCluster::logTemplate)
            .def_readwrite("logIDL", &TemplateCluster::logIds)
            .def(py::pickle(
                    [](const TemplateCluster &t) { // __getstate__
                        /* Return a tuple that fully encodes the state of the object */
                        return py::make_tuple(t.logTemplate, t.logIds);
                    },
                    [](py::tuple t) { // __setstate__
                        if (t.size() != 2)
                            throw std::runtime_error("Invalid state!");


                        /* Create a new C++ instance */
                        TemplateCluster tempClu(
                                t[0].cast<vector<string>>(),
                                t[1].cast<vector<int>>());
                        return tempClu;
                    }
            ));
    py::class_<TrieNode>(m, "TrieNode")
            .def(py::init<>())
            .def(py::init<string &, int &>())
            .def_readwrite("cluster", &TrieNode::cluster)
            .def_readwrite("token", &TrieNode::token)
            .def_readwrite("templateNo", &TrieNode::templateNo)
            .def_readwrite("child", &TrieNode::child)
            .def(py::pickle(
                    [](const TrieNode &t) { // __getstate__
                        /* Return a tuple that fully encodes the state of the object */
                        return py::make_tuple(t.cluster, t.token, t.templateNo, t.child);
                    },
                    [](py::tuple t) { // __setstate__
                        if (t.size() != 4)
                            throw std::runtime_error("Invalid state!");

                        /* Create a new C++ instance */
                        TrieNode trie(
                                t[0].cast<optional<TemplateCluster>>(),
                                t[1].cast<string>(),
                                t[2].cast<int>(),
                                t[3].cast<map<string, TrieNode>>());
                        return trie;
                    }
            ));
    py::class_<Parser>(m, "Parser")
        .def(py::init<>())
        .def(py::init<float &>(),
            py::arg("tau"))
        .def(py::init<vector<TemplateCluster> &, TrieNode &, float &>(),
            py::arg("logCluster"),py::arg("prefixTree"),py::arg("tau"))
        .def_readwrite("trieRoot", &Parser::trieRoot)
        .def_readwrite("logClust", &Parser::logClust)
        .def("parse", &Parser::parse,
            "A function which parses the 'Content' section of a log"
            " generated from spellpy",
            py::arg("content"), py::arg("lastLineId"))
        .def("LCS", &Parser::LCS,
                "Longest Common Subsequence between String Arrays",
                py::arg("seq1"),py::arg("seq2"))
        .def("LCSMatch", &Parser::LCSMatch,py::return_value_policy::copy,
                "Tries to find a match for a logMsg in a List of TemplateCLuster",
                py::arg("cluster"), py::arg("logMsg"))
        .def("addSeqToPrefixTree", &Parser::addSeqToPrefixTree,
                "Add Template to trie",
                py::arg("prefixTreeRoot"), py::arg("newCluster"))
        .def("getTemplate", &Parser::getTemplate,
                "Generate Template from partial message obtained via LCS",
                py::arg("lcs"), py::arg("seq"));

}