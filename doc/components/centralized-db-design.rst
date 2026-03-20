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
- support schema upgrades that allow for backwards-compatible operability

*****************************
Existing SQLite logical model
*****************************

Several characteristics of the current schema are important when designing the
PostgreSQL version:

Associations
------------

Associations are currently keyed with a combination of their username and the
name of the bank they belong to, containing configurable and mutable properties
such as farishare, shares, usage, limits, etc.

Usage factor storage
--------------------

The current implementation *dynamically* adds columns such as
``usage_factor_period_0``, ``usage_factor_period_1``, and so on, depending on
properties that are chosen during database creation. This is not a good fit for
a persistent, evolving PostgreSQL schema.

Half-life period state
----------------------

The current half-life state is modeled in a way that implicitly assumes a single
cluster row. That should become explicitly cluster-scoped.

********************************
Proposed PostgreSQL architecture
********************************

Overview
--------

The recommended architecture is:

* a centralized PostgreSQL database as the canonical store
* import/export adapters between PostgreSQL and transient SQLite databases
* explicit schema versioning and migration control
* a normalized relational schema for long-lived state

Proposed schema
---------------

clusters
^^^^^^^^

A table designed around scoping data to clusters.

.. code-block::

  CREATE TABLE cluster (
    cluster_id      BIGSERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    active          BOOLEAN NOT NULL DEFAULT TRUE
  );

Introducing a ``cluster`` table allows the centralized database to support one
or more clusters cleanly.

banks
^^^^^

.. code-block::

  CREATE TABLE bank (
    bank_id              BIGSERIAL PRIMARY KEY,
    cluster_id           BIGINT NOT NULL REFERENCES cluster(cluster_id) ON DELETE CASCADE,
    name                 TEXT NOT NULL,
    active               BOOLEAN NOT NULL DEFAULT TRUE,
    parent_bank_id       BIGINT REFERENCES bank(bank_id) ON DELETE CASCADE,
    shares               INTEGER NOT NULL,
    job_usage            DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    priority             DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    ignore_older_than    BIGINT NOT NULL DEFAULT 0,
    created_at           TIMESTAMP NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (cluster_id, name)
  );

Banks will be cluster-scoped.

associations
^^^^^^^^^^^^

.. code-block::

  CREATE TABLE association (
    association_id      BIGSERIAL PRIMARY KEY,
    cluster_id          BIGINT NOT NULL REFERENCES cluster(cluster_id) ON DELETE CASCADE,
    username            TEXT NOT NULL,
    userid              INTEGER NOT NULL DEFAULT 65534,
    default_bank_id     BIGINT NOT NULL REFERENCES bank(bank_id) ON DELETE CASCADE,
    bank_id             BIGINT NOT NULL REFERENCES bank(bank_id) ON DELETE CASCADE,
    active              BOOLEAN NOT NULL DEFAULT TRUE,
    shares              INTEGER NOT NULL DEFAULT 1,
    job_usage           DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    fairshare           DOUBLE PRECISION NOT NULL DEFAULT 0.5,
    max_running_jobs    INTEGER NOT NULL DEFAULT 5,
    max_active_jobs     INTEGER NOT NULL DEFAULT 7,
    max_nodes           INTEGER NOT NULL DEFAULT 2147483647,
    max_cores           INTEGER NOT NULL DEFAULT 2147483647,
    max_sched_jobs      INTEGER NOT NULL DEFAULT 2147483647,
    default_project_id  BIGINT REFERENCES project(project_id) ON DELETE SET NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT now(),
    updated_at          TIMESTAMP NOT NULL DEFAULT now(),
    queues              TEXT,
    projects            TEXT,
    UNIQUE (cluster_id, username, bank_id)
  );

In a database that could hold information about multiple clusters, we need to
increase the granularity for associations to also include the cluster they
belong to in the primary key.

usage_factor
^^^^^^^^^^^^

.. code-block::

  CREATE TABLE usage_factor_state (
    usage_factor_state_id   BIGSERIAL PRIMARY KEY,
    association_id          BIGINT NOT NULL REFERENCES association(association_id) ON DELETE CASCADE,
    last_job_timestamp      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    UNIQUE (association_id)
  );

usage_factor_bin
^^^^^^^^^^^^^^^^

.. code-block::

  CREATE TABLE usage_factor_bin (
    usage_factor_state_id   BIGINT NOT NULL REFERENCES usage_factor_state(usage_factor_state_id) ON DELETE CASCADE,
    period_index            INTEGER NOT NULL,
    usage_value             DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    PRIMARY KEY (usage_factor_state_id, period_index)
  );

The current SQLite model dynamically adds ``usage_factor_period_N`` columns
depending on runtime decay configuration. That pattern should not be used in
PostgreSQL. The number of periods should be represented as data rather than as
schema. This design makes usage-factor history more flexible, easier to query
generically, and easier to evolve.

usage_decay_config
^^^^^^^^^^^^^^^^^^

.. code-block::

  CREATE TABLE usage_decay_config (
    cluster_id                    BIGINT PRIMARY KEY REFERENCES cluster(cluster_id) ON DELETE CASCADE,
    priority_usage_reset_period   INTEGER,
    priority_decay_half_life      INTEGER,
    end_half_life_period          DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now()
  );

jobs
^^^^

.. code-block::

  CREATE TABLE job (
    job_id               TEXT PRIMARY KEY,
    cluster_id           BIGINT NOT NULL REFERENCES cluster(cluster_id) ON DELETE RESTRICT,
    userid               INTEGER NOT NULL,
    username             TEXT,
    association_id       BIGINT REFERENCES association(association_id) ON DELETE RESTRICT,
    project_id           BIGINT REFERENCES project(project_id) ON DELETE RESTRICT,
    bank_id              BIGINT REFERENCES bank(bank_id) ON DELETE NULL,
    t_submit             DOUBLE PRECISION NOT NULL,
    t_run                DOUBLE PRECISION NOT NULL,
    t_inactive           DOUBLE PRECISION NOT NULL,
    ranks                TEXT NOT NULL,
    resource_set         TEXT NOT NULL,
    jobspec              TEXT NOT NULL,
    requested_duration   DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    actual_duration      DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at           TIMESTAMP NOT NULL DEFAULT now()
  );

.. code-block::
  
  CREATE TABLE priority_factor_weight (
    cluster_id      BIGINT NOT NULL REFERENCES cluster(cluster_id) ON DELETE CASCADE,
    factor          TEXT NOT NULL,
    weight          INTEGER NOT NULL,
    PRIMARY KEY (cluster_id, factor)
);

*************************
Schema evolution strategy
*************************

Overview
--------

The schema for this database, like the transient SQLite database for
flux-accounting, must be designed in a way where it can evolve over time. The
goal is to allow new versions of the software to introduce schema changes while
minimizing disruption to existing deployments.

Version tracking
^^^^^^^^^^^^^^^^

.. code-block::

  CREATE TABLE schema_migration (
    version         INTEGER PRIMARY KEY,
    name            TEXT NOT NULL,
    applied_at      TIMESTAMP NOT NULL DEFAULT now()
  );

.. code-block::

  CREATE TABLE schema_metadata (
    key     TEXT PRIMARY KEY,
    value   TEXT NOT NULL
  );
