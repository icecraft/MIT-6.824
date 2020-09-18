export LOG_FILE="raft.${1}.log"
for i in `seq 100`
do
    echo $i
    go test --run $1 -timeout 200s 1>${0}.${1}.progress.log
    if [[ $? != 0 ]]; then
            osascript -e 'display notification "raft tests failed!"'
        break
    fi
done
