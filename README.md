# Sladder

[![Build Status](https://travis-ci.com/Sunmxt/sladder.svg?branch=master)](https://travis-ci.com/Sunmxt/sladder)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/cafdd3da0f9446899223d786fcb63503)](https://www.codacy.com/manual/Sunmxt/sladder?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Sunmxt/sladder&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/Sunmxt/sladder/branch/master/graph/badge.svg)](https://codecov.io/gh/Sunmxt/sladder)

**Sladder** is simple and embeded membership framework for service discovery and cluster management.



### Model

**Sladder** provides an extensible cluster data model for node metadata exchange. Each node could has a set of key-value entries called ***metadata***. **Sladder** ensures metadata consistent among all peers. Consistent strategy is tunable via different ***engines*** and ***validators***.



### Engines

Gossip engine are currently supported. It use an extended version of algorithm from [**《SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol》**](http://www.cs.cornell.edu/Info/Projects/Spinglass/public_pdfs/SWIM.pdf). In CAP terms, gossip engine builds an AP system.

Etcd engine is in plan.

