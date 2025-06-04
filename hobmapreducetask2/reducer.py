import sys
from collections import defaultdict

counter = defaultdict(int)

for line in sys.stdin:
    try:
        method_name, count = line.strip().split("\t")
        counter[method_name] += int(count)
    except:
        continue

# Сортируем по убыванию количества
for method_name, count in sorted(counter.items(), key=lambda x: -x[1]):
    print("%s\t%d" % (method_name, count)) 