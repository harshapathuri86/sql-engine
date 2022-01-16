if [ $# -ne 1 ]; then
    echo "Incorrect usage! Syntax: ./2019101086.sh \"sql query\""
    exit 1
fi
python3 main.py "$1"
