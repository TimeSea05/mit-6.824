# Debugging Log of Raft Lab

**Fri Mar 24 16:21:02 CST 2023**：通过测试`TestFailAgree2B`

`TestFailAgree2B`的测试过程为：首先创建三个服务器，使它们同步；同步之后将一个Follower从网络中断开，之后再重新加入到网络当中。

Bug出现在将断开的Follower重新加入网络后。

Follower被断开网络连接后，其`ticker`会检测到超时，于是便会启动Leader Election，但是由于网络不同，Follower无法获得来自其他服务器的投票，Leader Election失败。这样失败的过程会持续一次到多次，每次启动Leader Election，都会导致其`Term`增加。

Follower重新加入网络连接后，原来网络中的Leader会向其发送Heart Beat，但是Leader的Term比重新加入的Follower低，因此Heart Beat返回false，原来网络中的Leader变成Follower。但是在Follower重新加入网络后的同时，原来网络中的两台机器会对新的Log Entry进行同步，这样原来两台机器中都会有新的Log Entry，负责同步该Entry的线程在原来网络的Leader中。

网络中的Leader在变成Follower之后，网络中不再有Leader存在，会开始新一轮的Leader Election。首先重新加入的Follower是不可能被选举为Leader的，因为它的log不是up-to-date的；所以只有另外两个服务器之一会当选Leader。如果这两个服务器中另外一个没有成为过Leader的服务器被选为Leader，就会出现bug。

这是因为代码原来的设计中，Commit操作是由`startAgreement`线程进行的，Leader每次收到一个Command都会启动一个`startAgreement`线程，并由其Commit Log Entry，这样每一个Log Entry都会被复制。但是在这种情况中，新的Leader中有来自原来Leader中Log Entry，但是这个Log Entry并没有相应的`startAgreement`线程负责在safely replicated之后将其commit掉，所以Leader和Follower之间永远无法实现同步，总是差一个。

解决办法就是在raft peer的数据结构中增加一个字段记录`startAgreement`线程的数量，如果线程的数量小于待Commit的Log Entry的数量，那么需要连续多次进行Commit。在上面的例子中，显然线程数量是要比待Commit的Log Entry的数量少1的，那么在实现了上述描述之后，这些线程在Commit的时候，就可以多Commit一次，能够实现Leader和Follower之间的同步。