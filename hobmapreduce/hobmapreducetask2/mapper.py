import sys
import re

error_started = False
stacktrace_methods = []

for line in sys.stdin:
    line = line.rstrip()
    
    # Проверяем, начинается ли строка с ERROR
    if '/ERROR]:' in line and line.startswith('['):
        # Если у нас уже была ошибка, обрабатываем её стектрейс
        if error_started and stacktrace_methods:
            # Берем самую нижнюю строку стектрейса (последнюю)
            bottom_method = stacktrace_methods[-1]
            print("%s\t1" % bottom_method)
        
        # Начинаем новую ошибку
        error_started = True
        stacktrace_methods = []
        continue
    
    # Если мы обрабатываем ошибку
    if error_started:
        # Ищем строки стектрейса, которые начинаются с табуляции/пробелов и содержат "at"
        if (line.startswith('\t') or line.startswith('    ')) and 'at ' in line:
            # Извлекаем метод из строки вида: \tat org.bukkit.plugin.java.JavaPluginLoader.getPluginDescription(JavaPluginLoader.java:162)
            match = re.search(r'at\s+([^(]+)', line)
            if match:
                method = match.group(1).strip()
                stacktrace_methods.append(method)
        # Если строка не относится к стектрейсу и не пустая, завершаем обработку ошибки
        elif line and not line.startswith('\t') and not line.startswith('    ') and not line.startswith('Caused by:'):
            if stacktrace_methods:
                bottom_method = stacktrace_methods[-1]
                print("%s\t1" % bottom_method)
            error_started = False
            stacktrace_methods = []

# Обрабатываем последнюю ошибку в файле
if error_started and stacktrace_methods:
    bottom_method = stacktrace_methods[-1]
    print("%s\t1" % bottom_method) 