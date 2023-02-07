import sys
import re

with open(sys.argv[1]) as f:
    lines = f.readlines()

with open(sys.argv[1], "w") as f:
    pattern = re.compile(r"^(\s*)containers:")
    for line in lines:
        m = pattern.match(line)
        if m:
            f.write(m.group(1) + "imagePullSecrets:\n" + m.group(1) + "  - name: regcred\n")
        f.write(line)

