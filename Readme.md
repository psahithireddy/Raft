# Raft

Distribute consensus algorithm for leader election and log replication.
To be implemented in Go.

## Run:

go run test_raft.go

## Tests:

1. A new leader is reelected every few seconds with delay in leader heartbeat introduced for testing.
2. Append across the nodes
3. Append when a node is not able to accept new logs, across elections (run the code and wait for about a min)

## Resources

mit lab link, need to do 2A and 2B only : http://nil.csail.mit.edu/6.824/2021/labs/lab-raft.html\

raft explained : http://thesecretlivesofdata.com/raft/

mit tutorial : https://www.youtube.com/watch?v=UzzcUS2OHqo&t=204s

git repo : 1)https://github.com/zjzsliyang/6.824Lab

2. https://github.com/shinytang6/6.824-raft
