
for i in `seq 1000`
do
    echo $i
    go test --run $1 -timeout 1000s 1>${0}.${1}.progress.log
    if [[ $? != 0 ]]; then
        osascript -e 'display notification "kvraft tests failed!"'
        break
    fi
done
