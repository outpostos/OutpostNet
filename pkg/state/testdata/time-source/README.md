# Time-source fixtures

`chrony-node1.csv` and `timedatectl-node1.txt` were captured read-only from
outpost-node1 on 2026-11-15 UTC, using `chronyc -c tracking` and
`timedatectl show`. The chrony report was synchronized. The same host's
`timedatectl show-timesync` failed because timesyncd was unavailable.
No VM service, configuration or clock was changed.

`timesync-source-loss.txt` is the `timedatectl show-timesync` capture from
[systemd/systemd#16492](https://github.com/systemd/systemd/issues/16492).
It records source loss with an ignored NTP packet, including the older
`Ignored=yes PacketCount=620` format without a comma. Only the property
output is retained; shell prompts, service paths and journal lines are omitted.

Tests also derive explicit chrony unsynchronized/local-mode/malformed variants
from the captured CSV and inject command-not-found, timeouts and kernel error
bounds. These variants are synthetic; no stop/start VM capture is claimed.
The live chrony fault scenario is assigned to OutpostRange#51 by the architect's
[Net#63 comment](https://github.com/outpostos/OutpostNet/issues/63#issuecomment-6888153483).
