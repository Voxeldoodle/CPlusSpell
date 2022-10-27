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
                py::arg("lcs"), py::arg("seq"));

}