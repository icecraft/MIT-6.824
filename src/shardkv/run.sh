
for i in `seq 1000`
do
    echo $i
    if [[ a$1 == a ]]; then
       go test -timeout 1000s |tee ${0}.${1}.progress.log
        if [[ $? != 0 ]]; then
            osascript -e 'display notification "shardkv tests failed!"'
            break
        fi
    else
        go test --run $1 -timeout 1000s | tee ${0}.${1}.progress.log
        if [[ $? != 0 ]]; then
            osascript -e 'display notification "shardkv tests failed!"'
            break
        fi
    fi
done
