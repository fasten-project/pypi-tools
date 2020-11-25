# Dynamic Python call graphs using pycallgraph2

## Installation

```
>>> git submodule init && git submodule update
>>> docker built -t pypi-dynamic .
```

## Usage

```
# Download package and store in directory <package>
>>> docker run --mount type=bind,source=$(pwd)/<package>,target=/cggen/source --net=host -it pypi-dynamic /cggen/source
```
