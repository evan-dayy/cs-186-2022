import subprocess
import argparse
import os
import shutil
import traceback
import json
import difflib
import re
from collections import OrderedDict

object_id_re = re.compile("ObjectId\(\"[0123456789abcdef]*\"\)", flags=re.IGNORECASE)

def replace_object_id(document):
    matches = object_id_re.findall(document)
    for m in matches:
        document = document.replace(m, '"ObjectId"')
    return document

port = 27017

test_names = [
    "q0",
    "q1i", "q1ii", "q1iii", "q1iv",
    "q2i", "q2ii", "q2iii",
    "q3i", "q3ii",
]

type_exceptions = {
    'q2iii': ['budget'] # can be string or number
}

pytype_to_bjsontype = {
    list: "array",
    int: "number",
    type(None): "null",
    float: "number",
    str: "string",
    bool: "boolean",
    dict: "object"
}

test_orders = {
    # We want the orders of movieIds, ratings, and timestamps to match
    "q1iv": (True, {"movieIds": True, "ratings": True, "timestamps": True})
}

def remake_dir(path):
    if os.path.exists(path):
        if os.path.isdir(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
    os.mkdir(path)

def check_matching_format(actual, expected, exceptions, prefix=''):
    missing = []
    extra = []
    diffs = []
    for field in expected:
        if field not in actual:
            missing.append("- " + prefix + field)
        elif prefix + field not in exceptions:
            evalue = expected[field]
            avalue = actual[field]
            etype = pytype_to_bjsontype[type(evalue)]
            atype = pytype_to_bjsontype[type(avalue)]
            if etype != atype:
                diffs.append([
                    prefix+field, atype, etype,
                    json.dumps(avalue),
                    json.dumps(evalue)
                ])
            elif etype in ("object", "array"):
                if etype == 'array':
                    if not evalue or not avalue:
                        continue
                    # don't think too hard about this because its cursed
                    avalue = {'0': avalue[0]}
                    evalue = {'0': evalue[0]}

                m, e, d = check_matching_format(
                    avalue,
                    evalue,
                    exceptions,
                    prefix + field + '.'
                )
                missing.extend(m)
                extra.extend(e)
                diffs.extend(d)
    for field in actual:
        if field not in expected:
            extra.append("- " + prefix + field)
    return missing, extra, diffs

def to_consistent(j, order):
    if type(j) in (int, bool, str, float, type(None)):
        return j
    if type(j) == list:
        vals = map(lambda i: to_consistent(i, order), j)
        if order:
            return list(vals)
        else:
            # very cursed code that you shouldn't think about either
            return list(sorted(vals, key=lambda v: str(v)))

    if type(order) == tuple:
        order, suborder = order
    else:
        suborder = {}

    if type(j) == dict:
        return {k: to_consistent(v, suborder.get(k, False)) for k,v in j.items()}

def check_matching_values(actual, expected, test_name):
    order = test_orders.get(test_name, True)
    ah = to_consistent(actual, order)
    eh = to_consistent(expected, order)
    diff_lines = []
    your_lines = list(map(lambda a: json.dumps(a, sort_keys=True), ah))
    expected_lines = list(map(lambda e: json.dumps(e, sort_keys=True), eh))
    match = True
    for line in difflib.ndiff(your_lines, expected_lines):
        if not line.startswith('? '):
            if line[:2] in ('- ', '+ '):
                if line[2:].strip():
                    match = False
                else:
                    continue
            diff_lines.append(line)
    outfile = os.path.join('diffs', test_name + '.diff')
    with open(os.path.join('diffs', test_name + '.diff'), 'wt') as f:
        f.write("\n".join(diff_lines))
    if match:
        return "PASS"
    return "FAIL - see diffs/{}.diff".format(test_name)

def check_diff(test_name):
    expected = []
    actual = []
    with open(os.path.join('expected_output', test_name + '.dat'), 'rt') as f:
        for line in f.readlines():
            line = line.strip()
            if line: # ignore empty lines
                expected.append(json.loads(line))
    with open(os.path.join('your_output', test_name + '.dat'), 'rt') as f:
        for line in f.readlines():
            line = line.strip()
            if "ObjectId(" in line:
                return "FAIL: Your output includes ObjectId(...). " +\
                       "Make sure to explicitly project out the `_id` field!"
            if line: # ignore empty lines
                # this tries to parse a line of your output as json
                # print(line)
                actual.append(json.loads(line))
    if not actual:
        return "FAIL: Empty output"
    for a in actual:
        m,e,d = check_matching_format(a, expected[0], type_exceptions.get(test_name, []))
        if m or e or d:
            break
    msg = []
    if m:
        msg.append("! MISSING FIELDS")
        msg.extend(m)
        msg.append("")
    if e:
        msg.append("! EXTRA FIELDS")
        msg.extend(e)
        msg.append("")
    if d:
        msg.append("! MISMATCHED TYPES")
        for field, atype, etype, avalue, evalue in d:
            msg.append("- mismatch on field `{}`:".format(field))
            msg.append("    - expected type: `{}` (example: `{}`)".format(
                etype, evalue
            ))
            msg.append("    - actual type: `{}`, (example: `{}`)".format(
                atype, avalue
            ))
            msg.append("")
    if msg:
        with open(os.path.join('diffs', test_name + '.diff'), 'wt') as f:

            msg.append("! FORMAT MISMATCH")
            msg.append("- Example of expected document:")
            msg.extend(json.dumps(expected[0], indent=4).splitlines())
            msg.append("")
            msg.append("- Your document:")
            msg.extend(json.dumps(actual[0], indent=4).splitlines())
            msg.append("")
            for line in msg:
                if not line.startswith('!'):
                    f.write("  " + line)
                else:
                    f.write(line)
                f.write('\n')
        return "FAIL: Format mismatch. See diffs/{}".format(
            test_name + '.diff'
        )
    return check_matching_values(actual, expected, test_name)

def run_question(query_dir, question, batch_size):
    if not question.startswith('q'):
        question = 'q' + question
    if question not in test_names:
        print("ERROR: question `{}` not a valid test.".format(question))
        exit(1)
    with open(os.path.join(query_dir, question + '.js'), 'rt') as f:
        return run_query(f.read(), batch_size, question)

def run_query(query, batch_size, question):
    query = "DBQuery.shellBatchSize = {}; {}".format(batch_size, query)
    r = subprocess.run(
        ["mongo", "--port", str(port), "movies", "--eval", query],
        stdout=subprocess.PIPE
    )
    lines = []
    started=False
    errored=False
    for row in r.stdout.decode('utf-8').splitlines():
        if row.startswith('uncaught'):
            started = True
            errored = True
        if row.startswith('{'):
            started=True
        if row.startswith('Type'):
            continue
        if not started:
            continue
        lines.append(row)
    if errored:
        print("WARNING: The following error occurred while running your query for {}\n".format(question))
        print("\n".join(lines))
        return ""
    return "\n".join(lines)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description ='Run the tests for Project 6'
    )
    parser.add_argument('question', nargs='?', default='all')
    parser.add_argument('-g', '--gen', action='store_true')
    parser.add_argument('-d', '--data', default='movies')
    parser.add_argument('--dir', default='query')
    parser.add_argument('-e', '--expected', default='expected_output')
    parser.add_argument('-v', '--view', action='store_true')
    parser.add_argument('-f', '--format', action="store_true")
    parser.add_argument('-b', '--batch_size', default=10)

    args = parser.parse_args()

    if args.view:
        if args.question == 'all':
            print("ERROR: specify a question with to view its output")
            print("i.e. python3 test.py q0 --view")
            exit(1)
        print("Showing up to the first {} results of the query".format(args.batch_size))
        result = run_question(args.dir, args.question, args.batch_size)
        if not result.strip():
            print("(No output generated by query)")
        else:
            print(result)
        exit(0)

    if args.format:
        if args.question == 'all':
            print("ERROR: specify a question to view its output")
            print("i.e. python3 test.py q0 --format")
            exit(1)
        print("Showing formatted first document of the query")
        result = run_question(args.dir, args.question, 1)
        if not result.strip():
            print("(No output generated by query)")
        else:
            result = replace_object_id(result)
            print(json.dumps(json.loads(result), indent=4))
        exit()

    if not os.path.exists(args.dir):
        print("ERROR: Could not find input query dir `{}`".format(args.dir))
        exit(1)

    output_dir = "your_output"
    remake_dir('your_output')
    remake_dir('diffs')

    if args.gen:
        print("Generating expected output")
        output_dir = "expected_output"

    for test_name in test_names:
        if args.question not in ('all', test_name, test_name[1:]):
            continue
        with open(os.path.join(output_dir, test_name + ".dat"), 'wt') as f:
            result = run_question(args.dir, test_name, 2000)
            f.write(result)
        if not args.gen:
            try:
                result = check_diff (test_name)
            except:
                print(test_name, "ERROR:")
                traceback.print_exc()
                continue
            print("{:5}".format(test_name), result)

