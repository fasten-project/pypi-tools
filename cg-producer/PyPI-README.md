# Library for generating PyPI Call Graphs through PyCG

This library can be used to produce the call graph of a package distributed through pip along with saving its
source code and the call graph in JSON format in a specified directory.

## Usage

Example usage after installing the specific package through `pip`:

```python
from pycg_producer.producer import CallGraphGenerator

coord = { "product": "pycg-stitch",
          "version": "0.0.8",
          "version_timestamp": "2000",
          "requires_dist": []}
generator = CallGraphGenerator("directoryName", coord)
print(generator.generate())
```
The CallGraphGenerator class recieves as input the name of the directory where the source code and the call graph JSON will be stored, along with a Python dictionary containing the product name and version of the specific PyPI coordinate.

*Note:* It is mandatory to provide also a ```version_timestamp``` and ```requires_dist``` fields, which can be left empty if they are not going to be used.