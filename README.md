etcdloadtest
============

`etcdloadtest` is a command line load test client for [etcd](https://github.com/coreos/etcd).

## Commands

### TestPUR [options]

TestPUR does the load test for put, update and read operations.

#### Options

- rounds -- No of cycle for which the operation is to be performed.

- total-concurrent-ops", 10, "total no of concuurent operations to be performed.

- total-client --total no of client connections to use.

- mode -- Mode in which TestPUR needs to run (all, put, update, get).

- total-prefixes --total no of unique prefixes to use.

- total-keys --total number of keys to watch.

- key-length --length of key for the operation.

- value-length --length of value for the operation

- consistency --Linearizable(l) or Serializable(s) for read operation, applicable for mode 'all' and 'get' only'.

#### Examples

```
.\etcdloadtest.exe TestPUR --consistency=s --mode=all --rounds=1 --total-client=10 --total-concurrent-ops=10 --total-keys=1000 --total-prefixes=10 --key-length=64 --value-length=64
# round 0: Time taken for put for keys: 1000 is : 169.0096ms
# round 0: Time taken for update for keys: 1000 is : 346.0198ms
# round 0: Time taken for get for keys : 1000, is : 116.0066ms
```