# CPlusSpell
Spell implementation in C++

# Description
Porting of the parsing functionality of the spellpy package from [nailo2c](https://github.com/nailo2c/spellpy)
which in turn is derived from [logpai's logparser](https://github.com/logpai/logparser), both referring to the
[original paper](https://www.cs.utah.edu/~lifeifei/papers/spell.pdf) `Spell: Streaming Parsing of System Event Logs`
from Min Du, Feifei Li @University of Utah.

## Compilation
- Linux/WSL: `c++ -O3 -Wall -shared -std=c++17 -fPIC $(python3 -m pybind11 --includes) wrapper.cpp -o CPlusSpell$(python3-config --extension-suffix)`