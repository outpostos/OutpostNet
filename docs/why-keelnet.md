# Why Keelnet? A Technical Comparison

## Differentiating Keelnet from Traditional Proxies

A common initial assessment of Keelnet is to view it as a more complex version of tools like `frp`. This comparison is valid only in the narrow context of simple port forwarding.

- **`frp`**: Optimized for simplicity in point-to-point TCP/UDP port forwarding. Requires two processes and two configuration files for a basic setup.
- **Keelnet**: A comprehensive service mesh platform designed for edge computing. A minimal deployment involves three processes and a configuration model based on cloud-native concepts like `Service`, `Workload`, and `Endpoint`.

For use cases strictly limited to exposing internal ports to a public network, `frp` offers a more direct and simpler solution. However, Keelnet is engineered to address a fundamentally different and more complex set of challenges.

---

## Core Design Philosophy: Resilience in Unstable Environments

Keelnet was not designed to be a replacement for `frp`. Its primary objective is to serve as a resilient networking foundation for edge computing platforms operating in extreme and unstable conditions.

Consider the following target operating environments:
- **Tactical Edge**: Unstable, low-bandwidth, and frequently interrupted network links (e.g., tactical military networks, disaster response teams).
- **Mobile Industrial IoT**: Assets like cargo ships or remote exploration platforms with high-latency, expensive satellite connectivity.

In these environments, standard cloud-native stacks (e.g., Kubernetes, Istio) face significant operational challenges:
- **Control Plane Dependency**: A disconnection from a central `apiserver` or `istiod` can severely degrade or cripple cluster functionality.
- **Network Brittleness**: Assumes stable IP addressing and low-latency connectivity, which is often not available at the edge.
- **Resource Footprint**: The resource requirements for running a full service mesh control plane can exceed the capacity of edge devices.

Keelnet is architected to provide robust service discovery, routing, and management in these environments where traditional cloud-native technologies are not viable.

---

## Architectural Principles of Keelnet

To achieve its design goals, Keelnet employs a distinct architecture. Understanding the following principles is key to understanding Keelnet's capabilities.

### Principle 1: Decentralized, Node-Local Agents

A prevalent misconception is that `OutpostNet-server` is a centralized proxy server analogous to `frps`.

- **`frp` Architecture**: Relies on a central `frps` server that acts as a mandatory intermediary for all traffic.
- **Keelnet Architecture**: The `OutpostNet-server` is a decentralized daemon process that runs on **every** node in the mesh. It functions as a local agent, analogous to the role of `kubelet` in a Kubernetes cluster or `istio-proxy` in an Istio data plane.

The correct deployment topology is as follows:

```
+------------------ Node 1 ------------------+      +------------------ Node 2 ------------------+
|                                            |      |                                            |
|  +--------+      +----------------------+  |      |  +--------+      +----------------------+  |
|  |  App   |      |                      |  |      |  |  App   |      |                      |  |
|  +--------+      |   OutpostNet-server-1   |  |      |  +--------+      |   OutpostNet-server-2   |  |
|  | Sidecar| ===> | (Node Agent)         |  |      |  | Sidecar| ===> | (Node Agent)         |  |
|  | Client |(unix)|                      | ======>|  | Client |(unix)|                      |  |
|  +--------+      +----------------------+  (gossip) +--------+      +----------------------+  |
|                                            |      |                                            |
+--------------------------------------------+      +--------------------------------------------+
```

### Principle 2: Segregation of Identities

With the `server-per-node` model, the roles of the client and server components become clear:

- **`OutpostNet-client`**: Deployed as a sidecar, it represents the application's network identity and handles traffic interception.
- **`OutpostNet-server`**: Deployed as a node daemon, it represents the node's network identity and manages connectivity to other nodes.

The `client` communicates with its local `server` via a high-performance Unix Domain Socket. The `server` daemons communicate with each other using a gossip protocol to exchange topology and service information, forming a resilient, peer-to-peer network.

### Principle 3: Abstraction for Autonomy and Discovery

The `Service/Workload/Endpoint` resource model, while seemingly complex, is the foundation for Keelnet's dynamic service discovery and offline autonomy.

- **Use Case: Network Re-IPing**
  - **Problem**: A node's IP address changes due to DHCP, network migration, or multi-homing.
  - **`frp` Behavior**: The connection breaks. Manual or scripted intervention is required to update the configuration with the new IP address.
  - **Keelnet Behavior**: The application's connection to the network is abstracted. Communication between the `client` and the local `server` (via Unix Socket) is unaffected by external IP changes. The `OutpostNet-server` on the affected node advertises its new address to its peers via the gossip protocol. The mesh automatically converges on the new topology, restoring connectivity without manual intervention.

- **Use Case: Control Plane Disconnection**
  - **Problem**: A node or a group of nodes loses connectivity to the central management plane.
  - **Traditional Cloud-Native Behavior**: New deployments, configuration updates, and policy changes fail. The data plane may continue to function but is effectively unmanageable.
  - **Keelnet Behavior**: Each `OutpostNet-server` maintains a local cache of service configurations and PKI credentials (certificates and keys). During a disconnection, nodes can continue to enforce the last known security policies and route traffic to known services, enabling a state of **offline autonomy**.

---

## The Role of "Express Mode"

Given Keelnet's sophisticated architecture, the purpose of a simplified "Express Mode" is to improve the developer experience and lower the barrier to entry.

> The goal of a good architecture is to manage complexity, often by presenting a simplified interface for common tasks.

Express Mode is not an architectural compromise; it is a usability feature designed to:

1.  **Abstract Complexity**: Provide a streamlined, `frp`-like interface for simple use cases, such as local development or single-node deployments, without requiring an understanding of the distributed systems concepts.
2.  **Enable Progressive Disclosure**: Allow users to begin with a simple, intuitive model and gradually adopt the more advanced features of the "Standard Mode" as their requirements evolve. The underlying core engine remains the same.
3.  **Improve Development Velocity**: Offer a single-command method for instantiating a local Keelnet environment, significantly accelerating development and testing cycles.

---

## Conclusion: When to Choose Keelnet

Keelnet is the appropriate choice when the requirements go beyond simple proxying and involve building resilient, distributed systems. Select Keelnet for its capabilities in:

- **High Resilience & Availability**: Ensuring service continuity in unreliable and dynamic network environments.
- **Offline Autonomy**: Maintaining operational integrity during disconnections from a central control plane.
- **Dynamic Service Discovery**: Adapting automatically to changes in network topology and IP addressing.
- **Zero-Trust Security**: Implementing a PKI-based, end-to-end cryptographic identity and access control model.
- **Architectural Scalability**: Providing a platform that can evolve from a single-node agent into a complex, multi-cluster distributed service mesh.

Keelnet's architectural depth is a direct trade-off for its robustness and resilience in challenging operational environments. 