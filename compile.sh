
c++ -O3 -Wall -shared -std=c++17 -fPIC $(python3 -m pybind11 --includes) src/wrapper.cpp -o python/CPlusSpell$(python3-config --extension-suffix) && python3 python/test_spellpy.py