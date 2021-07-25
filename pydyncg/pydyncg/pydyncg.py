import os
import sys
import json
import time
import pkgutil
import argparse
import subprocess as sp

from stdlib_list import stdlib_list


class CGGenerator:
    def __init__(self, source_dir, command, command_args):
        self.source_dir = os.path.abspath(source_dir)
        self.command = command
        self.command_args = command_args

    def execute_pycallgraph(self):
        opts = [
            "pycallgraph",
            "json",
            "--"] + [self.command] + self.command_args
        try:
            out, err = self._execute(opts)
        except Exception as e:
            raise GeneratorError(str(e))

        pycallgraph_path = os.path.join(self.source_dir, "pycallgraph.json")
        if not os.path.exists(pycallgraph_path):
            print ("Execution encountered an error.")
            print (err)
            sys.exit(1)
        with open(pycallgraph_path) as f:
            contents = json.load(f)

        return contents

    def generate(self):
        cg = self.execute_pycallgraph()
        return cg

    def _execute(self, opts):
        cmd = sp.Popen(opts, stdout=sp.PIPE, stderr=sp.PIPE, cwd=self.source_dir)
        return cmd.communicate()

class GeneratorError(Exception):
    pass

class FASTENFormatter:
    def __init__(self, cg, package_path, product, forge, version, ignore_builtins):
        self.cg = cg
        self.product = product
        self.forge = forge
        self.version = version
        self.package_path = package_path
        self.ignore_builtins = ignore_builtins
        self.cnt_names = 0
        self.internal_name_to_id = {}
        self.resolved_name_to_id = {}
        self._ignore_funcs = ["_find_and_load", "_lock_unlock_module", "_handle_fromlist"]
        self._ignore_list = set([
            'Cython', 'setuptools', 'distutils', 'lib2to3', 'unittest', "site-packages",
            'difflib', 'ctypes', 'configparser', 'cython', 'getopt', "_sysconfigdata__linux_x86_64-linux-gnu",
            '_distutils_hack', 'pkg_resources', 'mock', 'importlib'] + stdlib_list("3.9"))
        self._ignore_files = self.get_builtin_files()

    def get_builtin_files(self):
        res = []
        builtin_modules = sys.builtin_module_names
        for item in pkgutil.iter_modules():
            if item.name in builtin_modules:
                res.append(os.path.join(item.module_finder.path, item.name + ".py"))
        return res

    def to_mod_name(self, path):
        if path.endswith(".py"):
            return os.path.splitext(path)[0].replace("/", ".")
        return path.replace("/", ".")

    def find_dependencies(self, package_path):
        res = []
        if not package_path:
            return res
        requirements_path = os.path.join(package_path, "requirements.txt")

        if not os.path.exists(requirements_path):
            return res

        reqs = []
        with open(requirements_path, "r") as f:
            lines = [l.strip() for l in f.readlines()]

        for line in lines:
            if not line:
                continue

            req = Requirement.parse(line)

            product = req.unsafe_name
            specs = req.specs

            constraints = []

            def add_range(begin, end):
                if begin and end:
                    if begin[1] and end[1]:
                        constraints.append("[{}..{}]".format(begin[0], end[0]))
                    elif begin[1]:
                        constraints.append("[{}..{})".format(begin[0], end[0]))
                    elif end[1]:
                        constraints.append("({}..{}]".format(begin[0], end[0]))
                    else:
                        constraints.append("({}..{})".format(begin[0], end[0]))
                elif begin:
                    if begin[1]:
                        constraints.append("[{}..]".format(begin[0]))
                    else:
                        constraints.append("({}..]".format(begin[0]))
                elif end:
                    if end[1]:
                        constraints.append("[..{}]".format(end[0]))
                    else:
                        constraints.append("[..{})".format(end[0]))

            begin = None
            end = None
            for key, val in sorted(specs, key=lambda x: x[1]):
                # if begin, then it is already in a range
                if key == "==":
                    if begin and end:
                        add_range(begin, end)
                        begin = None
                        end = None
                    if not begin:
                        constraints.append("[{}]".format(val))

                if key == ">":
                    if end:
                        add_range(begin, end)
                        end = None
                        begin = None
                    if not begin:
                        begin = (val, False)
                if key == ">=":
                    if end:
                        add_range(begin, end)
                        begin = None
                        end = None
                    if not begin:
                        begin = (val, True)

                if key == "<":
                    end = (val, False)
                if key == "<=":
                    end = (val, True)
            add_range(begin, end)

            res.append({"forge": "PyPI", "product": req.name, "constraints": constraints})

        return res

    def to_internal(self, item, inpath=None):
        mname = item["mod"]
        if mname.startswith(self.to_mod_name(self.package_path)):
            mname = mname[len(self.to_mod_name(self.package_path))+1:]

        if inpath and inpath in self._ignore_funcs:
            return None

        res = "/" + mname + "/"
        if inpath:
            res += inpath
            if item["is_func"] and not inpath.endswith("comp>"):
                res += "()"
        return res

    def to_external(self, item, inpath=None):
        modname = item["mod"]
        for path in sys.path:
            if modname.startswith(self.to_mod_name(path)):
                modname = modname[len(self.to_mod_name(path))+1:]

        if modname.startswith("."):
            return None

        if modname.split(".")[0] in self._ignore_list and self.ignore_builtins:
            return None

        if item["file"] in self._ignore_files and self.ignore_builtins:
            return None

        if inpath and inpath in self._ignore_funcs:
            return None

        res = "//" + modname.split(".")[0] + "/" + modname + "/"
        if inpath:
            res += inpath
            if item["is_func"] and not inpath.endswith("comp>"):
                res += "()"

        return res

    def get_nskey(self):
        res = str(self.cnt_names)
        self.cnt_names += 1
        return res

    def get_metadata(self, item):
        return {
            "numCalls": item["numCalls"],
            "time": item["time"]
        }

    def extract_nodes(self):
        def construct(item, modname, nskey):
            return {
                "sourceFile": item["file"],
                "namespaces": {
                    nskey: {
                        "namespace": modname,
                        "metadata": self.get_metadata(item),
                    }
                }
            }

        internal = {}
        external = {}
        for item in self.cg["nodes"]:
            if not "mod" in item or not "name" in item:
                continue
            if not item["mod"]:
                continue
            if item["name"].endswith("<module>"):
                if item["file"].startswith(self.package_path):
                    # internal module
                    modname = self.to_internal(item)
                    if not modname:
                        continue
                    nskey = self.get_nskey()
                    internal[modname] = construct(item, modname, nskey)
                    self.internal_name_to_id[item["name"]] = nskey
                else:
                    # external module
                    modname = self.to_external(item)
                    if not modname:
                        continue
                    nskey = self.get_nskey()
                    external[modname] = construct(item, modname, nskey)
                    self.resolved_name_to_id[item["name"]] = nskey

        for item in self.cg["nodes"]:
            if not "mod" in item or not "name" in item:
                continue
            if item["name"].endswith("<module>"):
                continue
            if item["name"] == "__main__":
                continue
            if not item["mod"]:
                continue
            if not item["name"].startswith(item["mod"]):
                continue

            if item["file"].startswith(self.package_path):
                fn = self.to_internal
                dct = internal
                cntdct = self.internal_name_to_id
            else:
                fn = self.to_external
                dct = external
                cntdct = self.resolved_name_to_id

            modname = fn(item)
            if not modname:
                continue
            if not dct.get(modname):
                nskey = self.get_nskey()
                dct[modname] = construct(item, modname, nskey)
                cntdct[item["mod"]] = nskey

            inpath = item["name"][len(item["mod"])+1:]
            fname = fn(item, inpath)
            if fname:
                nskey = self.get_nskey()
                dct[modname]["namespaces"][nskey] = {
                    "namespace": fn(item, inpath),
                    "metadata": self.get_metadata(item),
                }
                cntdct[item["name"]] = nskey

        return internal, external

    def extract_calls(self):
        resolvedCalls = []
        internalCalls = []
        for edge in self.cg["edges"]:
            src = edge["src"]
            dst = edge["dst"]

            found = True
            resolved = False
            if src in self.resolved_name_to_id:
                src_id = self.resolved_name_to_id[src]
                resolved = True
            elif src in self.internal_name_to_id:
                src_id = self.internal_name_to_id[src]
            else:
                found = False

            if dst in self.resolved_name_to_id:
                dst_id = self.resolved_name_to_id[dst]
                resolved = True
            elif dst in self.internal_name_to_id:
                dst_id = self.internal_name_to_id[dst]
            else:
                found = False

            if found and src_id == dst_id:
                continue

            if found and resolved:
                resolvedCalls.append([src_id, dst_id, {}])
            elif found:
                internalCalls.append([src_id, dst_id, {}])

        return internalCalls, resolvedCalls

    def generate(self):
        internal, external = self.extract_nodes()
        internalCalls, resolvedCalls = self.extract_calls()
        return {
            "product": self.product,
            "forge": self.forge,
            "version": self.version,
            "timestamp": int(time.time()),
            "generator": "pydyncg",
            "depset": self.find_dependencies(self.package_path),
            "modules": {
                "internal": internal,
                "external": external
            },
            "graph": {
                "internalCalls": internalCalls,
                "externalCalls": [],
                "resolvedCalls": resolvedCalls
            }
        }

def get_parser():
    parser = argparse.ArgumentParser(
        """FASTEN dynamic call graph generator using pycallgraph2"""
    )
    parser.add_argument(
        'source_dir',
        type=str,
        help="Package to analyze"
    )
    parser.add_argument(
        "--product",
        type=str,
        help="Package name"
    )
    parser.add_argument(
        "--forge",
        type=str,
        help="Source the package was downloaded from"
    )
    parser.add_argument(
        "--version",
        type=str,
        help="Version of the package"
    )
    parser.add_argument(
        "--ignore-builtins",
        action="store_true",
        help="Ignore builtin Python libraries"
    )

    parser.add_argument(
        "command",
        help="Command to execute",
        type=str
    )
    parser.add_argument(
        "command_args",
        nargs="*",
        help="Command arguments",
        type=str
    )
    return parser

def main():
    parser = get_parser()
    args = parser.parse_args()

    source_dir = args.source_dir
    product = args.product
    forge = args.forge
    version = args.version
    command = args.command
    command_args = args.command_args
    ignore_builtins = args.ignore_builtins

    generator = CGGenerator(source_dir, command, command_args)
    cg = generator.generate()

    formatter = FASTENFormatter(cg, source_dir, product, forge, version, ignore_builtins)
    print (json.dumps(formatter.generate()))

if __name__ == "__main__":
    main()
