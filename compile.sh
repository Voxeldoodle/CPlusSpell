c++ -O3 -Wall -shared -std=c++17 -fPIC $(python3 -m pybind11 --includes) wrapper.cpp -o CPlusSpell$(python3-config --extension-suffix) && python3 test_spellpy.py