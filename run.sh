#!/bin/bash

echo "=== Spanner Task 1: Неблокирующее чтение ==="
echo "Checking environment..."

# Проверяем доступ к HDFS
if command -v hdfs &> /dev/null; then
    echo "✅ HDFS client available"
    hdfs dfs -ls /data/movielens/ 2>/dev/null || echo "⚠️ No access to /data/movielens/"
else
    echo "❌ HDFS client not found"
fi

# Проверяем Python и зависимости
if command -v python3 &> /dev/null; then
    echo "✅ Python3 available"
else
    echo "❌ Python3 not found"
    exit 1
fi

# Проверяем Google Cloud SDK
if command -v gcloud &> /dev/null; then
    echo "✅ gcloud available"
else
    echo "⚠️ gcloud not found - will need manual auth"
fi

echo ""
echo "=== Running Spanner Task 1 ==="

# Устанавливаем зависимости
echo "Installing dependencies..."
pip3 install --user google-cloud-spanner || {
    echo "❌ Failed to install google-cloud-spanner"
    exit 1
}

# Запускаем setup (создание instance и загрузка данных)
echo ""
echo "Step 1: Setting up Spanner instance and loading data..."
python3 spannertask1_setup.py || {
    echo "❌ Setup failed"
    exit 1
}

# Запускаем демонстрацию изоляции
echo ""
echo "Step 2: Running isolation demonstration..."
python3 spannertask1_isolation_demo.py || {
    echo "❌ Demo failed"
    exit 1
}

# Проверяем результаты
echo ""
echo "=== Results ==="
if [ -f "spannertask1_results.txt" ]; then
    echo "✅ Results file created: spannertask1_results.txt"
    echo "TOP-10 results:"
    head -15 spannertask1_results.txt
    echo "..."
    echo "Total lines: $(wc -l < spannertask1_results.txt)"
else
    echo "❌ Results file not found"
    exit 1
fi

echo ""
echo "✅ Spanner Task 1 completed successfully!" 