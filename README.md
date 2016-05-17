# 2016_bop_semifinal
2016 bop senifinal python code 
队员：路遥，刘林青，丁宇笙(me)

进了决赛了，复赛 \# rank 11
复赛官方用例5组，不加缓存，成绩在49.975左右，一个用例的查询时间大概是120ms，每次查询academic api时间是70ms

nohup.out 是日志文件，基本只剩下get请求的url，不过可以看到请求的id对
semifinal.py 是主要文件，包括api调用，服务器启动

## 加速的方法：
1. socket 长连接
2. 使用or and 语句查询拼接
3. 一个语句同时查询两次（单次查询时间波动较大，两次查询同时进行，并且使用先返回的结果，会更稳定）
4. 尽可能减少查询次数，AuId-AuId总共一次查询 其他情况总共两到三次串行查询
5. 尽可能快的json解析包

## 代码中遇到的问题等：
1. requests > urllib2 >httplib > socket长连接 时间消耗依次减少
2. 要是可以直接获得线程的结果就好了
3. @profile kernprof 可以查看每一行代码的运行时间，但不能看多线程中的部分。
4. flask 作为服务器很方便，效率不明。

### nohup.out中的一小段
```
139.217.26.105 - - [15/May/2016 19:55:33] "GET /semifinal?id2=2291201213&id1=2028959752 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:55:36] "GET /semifinal?id2=2123376285&id1=2157348764 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:55:37] "GET /semifinal?id2=69987893&id1=2039741363 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:11] "GET /semifinal?id2=2157823046&id1=2168326394 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:12] "GET /semifinal?id2=2157138196&id1=2157848968 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:14] "GET /semifinal?id2=2122517769&id1=2118826874 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:16] "GET /semifinal?id2=2162196924&id1=2110631042 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:17] "GET /semifinal?id2=2062290101&id1=2157348764 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:18] "GET /semifinal?id2=2117668619&id1=2102200338 HTTP/1.1" 200 -
139.217.26.105 - - [15/May/2016 19:56:19] "GET /semifinal?id2=2162838272&id1=2041946752 HTTP/1.1" 200 -
```


## 最后一次提交时候的排名：yaoyao，第9
49.99分的应该都是缓存的答案，在服务器请求的时候直接返回，49.80的我也很好奇他们怎么做到的。
<img alt="rank" src="https://github.com/dyslove123/2016_bop_semifinal/blob/master/rank.png" width="100%" height="100%">

## 这是我们平时提交的成绩，可以看的出来，还是比较稳定的。
<img alt="submissions" src="https://github.com/dyslove123/2016_bop_semifinal/blob/master/submissions.png" width="100%" height="100%">
