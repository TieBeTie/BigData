import sys
import re

# Паттерн для строки лога с ERROR
error_pattern = re.compile(r'^\[.*?\] \[.*?/ERROR\]: (.*)$')

# Паттерн для строки стектрейса (начинается с табуляции или пробелов)
stacktrace_pattern = re.compile(r'^\s+at\s+([^(]+)')

# Паттерн для строки "Caused by"
caused_by_pattern = re.compile(r'^\s*Caused by:')

current_error = False
stacktrace_lines = []

for line in sys.stdin:
    line = line.strip()
    
    # Проверяем, является ли строка началом ошибки
    error_match = error_pattern.match(line)
    if error_match:
        # Если у нас уже была ошибка, обрабатываем её стектрейс
        if current_error and stacktrace_lines:
            # Берем самую нижнюю строку стектрейса (последнюю в списке)
            bottom_trace = stacktrace_lines[-1]
            print("%s\t1" % bottom_trace)
        
        # Начинаем новую ошибку
        current_error = True
        stacktrace_lines = []
        continue
    
    # Если мы в процессе обработки ошибки
    if current_error:
        # Проверяем строку стектрейса
        stacktrace_match = stacktrace_pattern.match(line)
        if stacktrace_match:
            method_name = stacktrace_match.group(1).strip()
            # Убираем информацию о файле и номере строки
            method_name = re.sub(r'\([^)]*\).*$', '', method_name).strip()
            if method_name:
                stacktrace_lines.append(method_name)
        # Проверяем "Caused by"
        elif caused_by_pattern.match(line):
            continue
        # Если строка не является частью стектрейса, завершаем обработку ошибки
        elif not line.startswith('\t') and not line.startswith(' '):
            if stacktrace_lines:
                # Берем самую нижнюю строку стектрейса
                bottom_trace = stacktrace_lines[-1]
                print("%s\t1" % bottom_trace)
            current_error = False
            stacktrace_lines = []

# Обрабатываем последнюю ошибку, если она есть
if current_error and stacktrace_lines:
    bottom_trace = stacktrace_lines[-1]
    print("%s\t1" % bottom_trace) 