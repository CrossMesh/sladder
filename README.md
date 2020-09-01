# Sladder

[![Build Status](https://travis-ci.com/crossmesh/sladder.svg?branch=master)](https://travis-ci.com/crossmesh/sladder)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/3675699a86ab446c9944aeb2476a5c1e)](https://www.codacy.com/gh/CrossMesh/sladder?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=CrossMesh/sladder&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/crossmesh/sladder/branch/master/graph/badge.svg)](https://codecov.io/gh/crossmesh/sladder)

**Sladder** is simple and embeded membership framework for service discovery and cluster management.



### Model

**Sladder** provides an extensible cluster data model for node metadata exchange. Each node could has a set of key-value entries called ***metadata***. **Sladder** ensures metadata consistent among all peers. Consistent strategy is tunable via different ***engines*** and ***validators***.



### Engines

Gossip engine are currently supported. It use an extended version of algorithm from [**《SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol》**](http://www.cs.cornell.edu/Info/Projects/Spinglass/public_pdfs/SWIM.pdf). In CAP terms, gossip engine builds an AP system.

Etcd engine is in plan.

