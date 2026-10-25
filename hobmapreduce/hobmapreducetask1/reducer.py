import sys
from collections import defaultdict

counter = defaultdict(int)

for line in sys.stdin:
    try:
        word, count = line.strip().split("\t")
    except Exception:
        continue

    counter[word] += int(count)

# Сортируем по убыванию количества, при равенстве - лексикографически
for k, v in sorted(counter.items(), key=lambda x: (-x[1], x[0])):
    print("%s\t%d" % (k, v)) 