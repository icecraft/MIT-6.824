export LOG_FILE="raft.${1}.log"
for i in `seq 1000`
do
    echo $i
    go test --run $1 -timeout 200s 1>${0}.${1}.progress.log
    if [[ $? != 0 ]]; then
        break
    fi
done
