import os
import json
import argparse
import subprocess as sp


class CGGenerator:
    def __init__(self, source_dir):
        self.source_dir = os.path.abspath(source_dir)

    def execute_pycallgraph(self):
        opts = [
            "pycallgraph",
            "json",
            "--",
            "setup.py", "test",
        ]
        try:
            out, err = self._execute(opts)
        except Exception as e:
            raise GeneratorError(str(e))

        with open(os.path.join(self.source_dir, "pycallgraph.json")) as f:
            contents = json.load(f)

        return contents

    def generate(self):
        cg = self.execute_pycallgraph()
        return json.dumps(cg)

    def _execute(self, opts):
        cmd = sp.Popen(opts, stdout=sp.PIPE, stderr=sp.PIPE, cwd=self.source_dir)
        return cmd.communicate()

class GeneratorError(Exception):
    pass

def get_parser():
    parser = argparse.ArgumentParser(
        """Execute pycallgraph on the package residing on source dir
        """
    )
    parser.add_argument(
        'source_dir',
        type=str,
        help="Package to analyze"
    )
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    source_dir = args.source_dir

    generator = CGGenerator(source_dir)
    print (generator.generate())

if __name__ == "__main__":
    main()
