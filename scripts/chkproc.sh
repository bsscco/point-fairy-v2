daemon=`netstat -tlnp | grep :::20000 | wc -l`
if [ "$daemon" -eq "0" ] ; then
        nohup java -jar /home/bsscco/point-fairy-v2/point-fairy-v2-*.jar &
fi