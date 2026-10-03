# outpostnet-service example: stable entry across instance replacement

Two nodes on one machine (separate processes, real QUIC tunnel, real TCP):

- `node-b` exports instance **v1** of service `echo` (`node-b.yaml`).
- `node-a` gives its application a stable entry `127.0.0.1:39080` = `echo@node-b` (`node-a.yaml`).
- `node-b` is then replaced by a new process on a different tunnel port serving instance **v2**
  (`node-b-replacement.yaml`); the peer directory is updated through the existing management API.
- Finally the v2 instance moves to another local port (v3) by editing the config file; the
  config watcher re-points the export in place.

The application URL never changes. Cross-node traffic is TCP only (UDP over the QUIC tunnel is
not implemented). Connections that are open while the peer address changes are closed; they are
not migrated (see `docs/service-layer.md`).

## Steps

Run from the repository root. Requires Go, `python3` and `curl`.

```bash
go build -o /tmp/outpostnet-service-example/outpostnet-service ./cmd/outpostnet-service
cp examples/service/*.yaml /tmp/outpostnet-service-example/
cd /tmp/outpostnet-service-example
for v in v1 v2 v3; do mkdir -p $v && echo $v > $v/index.html; done

# application instances (stand-ins for the real service)
python3 -m http.server 39091 --bind 127.0.0.1 --directory v1 &
python3 -m http.server 39092 --bind 127.0.0.1 --directory v2 &
python3 -m http.server 39093 --bind 127.0.0.1 --directory v3 &

# 1. two nodes
./outpostnet-service --config node-b.yaml &
./outpostnet-service --config node-a.yaml &
sleep 2
curl -s http://127.0.0.1:39080/                       # -> v1
curl -s --unix-socket node-a.sock http://unix/v1/peers # node-b connected at :39443

# 2. replace node-b: new process, new tunnel port, new instance
./outpostnet-service --config node-b-replacement.yaml --watch-interval 1s &
curl -s -X PUT -H 'Content-Type: application/json' --unix-socket node-a.sock \
  http://unix/v1/peers -d '[{"node_id":"node-b","addr":"127.0.0.1:39444"}]'
kill %4                                               # old node-b process
sleep 2
curl -s http://127.0.0.1:39080/                       # -> v2, same entry

# 3. instance moves to another local port on node-b
sed -i 's|127.0.0.1:39092   # instance v2|127.0.0.1:39093   # instance v3|' node-b-replacement.yaml
sleep 2
curl -s http://127.0.0.1:39080/                       # -> v3, same entry

kill %1 %2 %3 %5 %6
```

`%N` are the shell job numbers in the order started above (1-3 apps, 4 node-b, 5 node-a,
6 node-b replacement). Health: `curl http://127.0.0.1:39382/readyz` (node-a).

Node naming matters on the dynamic path: the lexicographically smaller `node_id` dials the QUIC
connection (existing peer-directory rule), and streams are opened only by the dialing side, so
the consuming node must have the smaller `node_id` (here `node-a` < `node-b`).
