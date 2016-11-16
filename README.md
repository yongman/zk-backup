# zk-backup
simple tool to backup zk tree to a backup zk cluster

## Installation

```bash
go get github.com/YongMan/zk-backup
go install github.com/YongMan/zk-backup
```
# Usage
```
Usage of ./zk-backup:
  -excludepath string
        exclude path (default "/r3/failover/history,/r3/failover/doing")
  -sourceaddr string
        source zk cluster address
  -targetaddr string
        target zk cluster address

Example:
    zk-backup -sourceaddr 127.0.0.1:2181 -targetaddr 127.0.0.1:2182
```
