# Sladder

[![Build Status](https://travis-ci.com/Sunmxt/sladder.svg?branch=master)](https://travis-ci.com/Sunmxt/sladder)

**Sladder** is simple and embeded membership framework for service discovery and cluster management.



### Model

**Sladder** provides an extensible cluster data model for node metadata exchange. Each node could has a set of key-value entries called ***metadata***. **Sladder** ensures metadata consistent among all peers. Consistent strategy is tunable via different ***engines*** and ***validators***.



### Engines

Gossip engine are currently supported, using an extended version of algorithm from [**《SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol》**](http://www.cs.cornell.edu/Info/Projects/Spinglass/public_pdfs/SWIM.pdf)。

Etcd engine is in plan.

