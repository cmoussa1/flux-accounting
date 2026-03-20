.. _centralized-db-design:

############################################
Centralized Database Design and Architecture
############################################

.. note::
    flux-accounting is still beta software and many of the interfaces
    documented in this deign guide may change with regularity.

    This document is in DRAFT form.

*******
Purpose
*******

This document proposes a centralized, persistent PostgreSQL database design for
the flux-accounting system. The design is based on the current in-memory SQLite
schema (see the **Database Administration** section of the
:doc:`../guide/accounting-guide`), and is intended to support long-term
persistence, multi-client access, controlled short-term and long-term schema
evolution, and compatibility with existing flux-accounting components that
interact with the SQLite database.

This document also looks to propose a migration and compatibility strategy with
the rest of the module so that the schema can work out-of-the-box as well as
evolve over time without breaking older versions of both itself and
flux-accounting.

**********
Background
**********

flux-accounting currently constructs and manages a SQLite database in-memory
and keeps track of all of its relevant data there. Its schema currently
contains the following entities:

- *associations*: a 2-tuple combination of a *username* and *bank*
- *banks*: responsible for storing groups of users
- *jobs*: completed job records as it relates to an association's and bank's
  total job usage on a system
- *queues*: a list of queues that associations can have access to
- *projects*: a list of projects that associations can have access to
- *priority factor weights*: configurable weights for factors that determine
  how a job's priority can be calculated

This design is effective for a local environment (such as just one cluster),
but it presents a number of limitations when used as the primary accounting
solution for a center with many clusters:

- limited concurrency on high-traffic systems
- ineffective support for centralized administration
- difficulty coordinating data on a per-user, per-bank, or per-association
  basis

A centralized PostgreSQL database looks to address these issues while still
allowing the system to interact with smaller, local SQLite databases deployed
on individual clusters.

*****
Goals
*****

Functional goals
----------------

The centralized database should:

- persist across a service restart
- support one or more clusters
- support export into in-memory SQLite databases that are deployed on clusters
- support import of data from the in-memory SQLite databases that are deployed
  on the clusters

Operational goals
-----------------

The database design should:

- support transactional updates
- scale to support data from multiple clusters as well as synchronous activity
  with the database
- support schema upgrades that allow for backwards-compatible operatability
