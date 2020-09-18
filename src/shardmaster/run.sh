
for i in `seq 1000`
do
    echo $i
    if [[ a$1==a ]]; then
       go test -timeout 1000s |tee ${0}.${1}.progress.log
    else
        go test --run $1 -timeout 1000s | tee ${0}.${1}.progress.log
    fi
    if [[ $? != 0 ]]; then
        osascript -e 'display notification "shardmaster tests failed!"'
        break
    fi
done
