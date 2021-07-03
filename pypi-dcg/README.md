# PyDynCG: Dynamic Call Graph Generator Using pycallgraph2

## Installation

First, install `pycallgraph2`, and then install pydyncg. From the root
directory,
```
>>> git submodule init && git submodule update
>>> cd pycallgraph2 && python3 setup.py install && cd .. # install pycallgraph2
>>> python3 setup.py install # install pydyncg
```

## Usage

```
>>> pydyncg -h
usage: FASTEN dynamic call graph generator using pycallgraph2 [-h] [--product PRODUCT] [--forge FORGE] [--version VERSION] source_dir command [command_args ...]

positional arguments:
  source_dir         Package to analyze
  command            Command to execute
  command_args       Command arguments

optional arguments:
  -h, --help         show this help message and exit
  --product PRODUCT  Package name
  --forge FORGE      Source the package was downloaded from
  --version VERSION  Version of the package
```

It is recommended to provide command to execute and arguments like this:

```
>>> pydyncg mydir --product "prod" --forge "pypi" --version "1.0.0" -- <command> <command_args>
```
