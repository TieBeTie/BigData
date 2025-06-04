import re
import sys
from collections import defaultdict

for line in sys.stdin:
    try:
        _, text = line.strip().split("\t", 1)
    except Exception as e:
        continue

    # Словарь для подсчета встреч слов в правильном формате
    proper_names = defaultdict(int)
    # Множество слов, которые встречались в нижнем регистре
    lowercase_words = set()

    # Разбиваем текст на слова, убирая пунктуацию
    for word in re.findall(r'\b[A-Za-z]+\b', text):
        # Проверяем длину слова
        if not (6 <= len(word) <= 9):
            continue

        # Если слово полностью в нижнем регистре
        if word.islower():
            lowercase_words.add(word.lower())
            continue

        # Если слово начинается с заглавной и остальные маленькие
        if word[0].isupper() and word[1:].islower():
            proper_names[word.lower()] += 1

    # Выводим только те слова, которые не встречались в нижнем регистре
    for word, count in proper_names.items():
        if word not in lowercase_words:
            print("%s\t%s" % (word, count)) 