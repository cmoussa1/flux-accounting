.. _flux-accounting-guide:

#####################
Flux Accounting Guide
#####################

*key terms: association, bank*

.. note::
    flux-accounting is still beta software and many of the interfaces
    documented in this guide may change with regularity.

    This document is in DRAFT form.

********
Overview
********

By default, a Flux system instance treats users equally and schedules work
based on demand, without consideration of a user's history of resource
consumption, or what share of available resources their organization considers
they should be entitled to use relative to other competing users.

Flux-accounting adds a database which stores site policy, *banks* with
with user/project associations, and metrics representing historical usage.
It also adds a Flux jobtap plugin that sets the priority on each job that
enters the system based on multiple factors including *fair share* values.
The priority determines the order in which jobs are considered by the scheduler
for resource allocation.  In addition, the jobtap plugin holds or rejects job
requests that exceed user/project specific limits or have exhausted their
bank allocations.

The database is populated and queried with command line tools prefixed with
``flux account``.  Accounting scripts are run regularly by
:core:man1:`flux-cron` to pull historical job information from the Flux
``job-list`` and ``job-info`` interfaces into the accounting database,
and to push bank and limit data to the jobtap plugin.

At this time, the database is expected to be installed on a cluster management
node, co-located with the rank 0 Flux broker, managing accounts for that
cluster only.  Sites would typically populate the database and keep it up to
date automatically using information regularly pulled or pushed from an
external source like an identity management system.

******************************
Installation and Configuration
******************************

System Prerequisites
====================

The `Flux Administrator's Guide <https://flux-framework.readthedocs.io/projects/flux-core/en/latest/guide/admin.html>`_ documents relevant information for
the administration and management of a Flux system instance.

The following instructions assume that Flux is configured and working, that
the Flux *statedir* (``/var/lib/flux``) is writable by the ``flux`` user,
and that the ``flux`` user is the system instance owner.

Installing Software Packages
============================

The ``flux-accounting`` package should be installed on the management node
from your Linux distribution package manager. Once installed, the service
that accepts ``flux account`` commands and interacts with the flux-accounting
database can be started.

You can enable the service with ``systemctl``; if not configured with a custom
path, the flux-accounting systemd unit file will be installed to the same
location as flux-core's systemd unit file:

.. code-block:: console

  $ sudo systemctl enable flux-accounting

The service can then be controlled with ``systemd``. To utilize the service,
the following prerequisites must be met:

1. A flux-accounting database has been created with ``flux account create-db``.
The service establishes a connection with the database in order to read from
and write to it. If the service has been started before the creation of the
database, you may encounter unexpected behavior from running ``flux account``
commands, such as ``sqlite3.OperationalError: attempt to write a readonly database``.

2. An active Flux system instance is running. The flux-accounting service will
only run after the system instance is started.

Accounting Database Creation
============================

The accounting database is created with the command below.  Default
parameters are assumed, including the accounting database path of
``/var/lib/flux/FluxAccounting.db``.

.. code-block:: console

 $ sudo -u flux flux account create-db

.. note::
    The flux accounting commands should always be run as the flux user. If they
    are run as root, some commands that rewrite the database could change the
    owner to root, causing flux-accounting scripts run from flux cron to fail.

Banks must be added to the system, for example:

.. code-block:: console

 $ sudo -u flux flux account add-bank root 1
 $ sudo -u flux flux account add-bank --parent-bank=root sub_bank_A 1

Users that are permitted to run on the system must be assigned banks,
for example:

.. code-block:: console

 $ sudo -u flux flux account add-user --username=user1234 --bank=sub_bank_A

Enabling Multi-factor Priority
==============================

When flux-accounting is installed, the job manager uses a multi-factor
priority plugin to calculate job priorities.  The Flux system instance must
configure the ``job-manager`` to load this plugin.

.. code-block:: toml

 [job-manager]
 plugins = [
   { load = "mf_priority.so" },
 ]

See also: :core:man5:`flux-config-job-manager`.

The plugin can also be manually loaded with ``flux jobtap load``. Be sure to
send all flux-accounting data to the plugin after it is loaded:

.. code-block:: console

 $ flux jobtap load mf_priority.so
 $ flux account-priority-update

Automatic Accounting Database Updates
=====================================

If updating flux-accounting to a newer version on a system where a
flux-accounting DB is already configured and set up, it is important to update
the database schema, as tables and columns may have been added or removed in
the newer version. The flux-accounting database schema can be updated with the
following command:

.. code-block:: console

 $ sudo -u flux flux account-update-db

A series of actions should run periodically to keep the accounting
system in sync with Flux:

- A script fetches inactive jobs and inserts them into a ``jobs`` table in the
  flux-accounting DB.
- The job-archive module scans inactive jobs and dumps them to a sqlite
  database.
- A script reads the archive database and updates the job usage data in the
  accounting database.
- A script updates the per-user fair share factors in the accounting database.
- A script pushes updated factors to the multi-factor priority plugin.

The Flux system instance must configure the ``job-archive`` module to run
periodically:

.. code-block:: toml

 [archive]
 period = "1m"

See also: :core:man5:`flux-config-archive`.

The scripts should be run by :core:man1:`flux-cron`:

.. code-block:: console

 # /etc/flux/system/cron.d/accounting

 30 * * * * bash -c "flux account-fetch-job-records; flux account-update-usage; flux account-update-fshare; flux account-priority-update"

Periodically fetching and storing job records in the flux-accounting database
can cause the DB to grow large in size. Since there comes a point where job
records become no longer useful to flux-accounting in terms of job usage and
fair-share calculation, you can run ``flux account scrub-old-jobs`` to
remove old job records. If no argument is passed to this command, it will
delete any job record that has completed more than 6 months ago. This can be
tuned by specifying the number of weeks to go back when determining which
records to remove. The example below will remove any job record more than 4
weeks old:

.. code-block:: console

 $ flux account scrub-old-jobs 4

By default, the memory occupied by a SQLite database does not decrease when
records are ``DELETE``'d from the database. After scrubbing old job records
from the flux-accounting database, if space is still an issue, the ``VACUUM``
command will clean up the space previously occupied by those deleted records.
You can run this command by connecting to the flux-accounting database in a
SQLite shell:

.. code-block:: console

 $ sqlite3 FluxAccounting.db
 sqlite> VACUUM;

Note that running ``VACUUM`` can take minutes to run and also requires an
exclusive lock on the database; it will fail if the database has a pending SQL
statement or open transaction.

***********************
Database Administration
***********************

The flux-accounting database is a SQLite database which stores user account
information and bank information. Administrators can add, disable, edit, and
view user and bank information by interfacing with the database through
front-end commands provided by flux-accounting. The information in this
database works with flux-core to calculate job priorities submitted by users,
enforce basic job accounting limits, and calculate fair-share values for
users based on previous job usage.

Each user belongs to at least one bank. This user/bank combination is known
as an *association*, and henceforth will be referred to as an *association*
throughout the rest of this document.

.. note::
    In order to interact with the flux-accounting database, you must have read
    and write permissions to the directory that the database resides in. The
    SQLite documentation_ states that since "SQLite reads and writes an ordinary
    disk file, the only access permissions that can be applied are the normal
    file access permissions of the underlying operating system."

The front-end commands provided by flux-accounting allow an administrator to
interact with association or bank information.  ``flux account -h`` will list
all possible commands that interface with the information stored in their
respective tables in the flux-accounting database. The current database
consists of the following tables:

+--------------------------+--------------------------------------------------+
| table name               | description                                      |
+==========================+==================================================+
| association_table        | stores associations                              |
+--------------------------+--------------------------------------------------+
| bank_table               | stores banks                                     |
+--------------------------+--------------------------------------------------+
| job_usage_factor_table   | stores past job usage factors for associations   |
+--------------------------+--------------------------------------------------+
| t_half_life_period_table | keeps track of the current half-life period for  |
|                          | calculating job usage factors                    |
+--------------------------+--------------------------------------------------+
| queue_table              | stores queues, their limits properties, as well  |
|                          | as their associated priorities                   |
+--------------------------+--------------------------------------------------+
| project_table            | stores projects for associations to charge their |
|                          | jobs against                                     |
+--------------------------+--------------------------------------------------+
| jobs                     | stores inactive jobs for job usage and fair      |
|                          | share calculation                                |
+--------------------------+--------------------------------------------------+

To view all associations in a flux-accounting database, the ``view-bank`` 
command will print this DB information in a hierarchical format. An example is
shown below showing all associations under the root bank:

.. code-block:: console

 $ flux account view-bank root -t

 Account                         Username           RawShares            RawUsage           Fairshare
 root                                                       1                 0.0
  bank_A                                                    1                 0.0
   bank_A                          user_1                   1                 0.0                 0.5
  bank_B                                                    1                 0.0
   bank_B                          user_2                   1                 0.0                 0.5
   bank_B                          user_3                   1                 0.0                 0.5
  bank_C                                                    1                 0.0
   bank_C_a                                                 1                 0.0
    bank_C_a                       user_4                   1                 0.0                 0.5
   bank_C_b                                                 1                 0.0
    bank_C_b                       user_5                   1                 0.0                 0.5
    bank_C_b                       user_6                   1                 0.0                 0.5


****************************
Job Usage Factor Calculation
****************************

An association's job usage represents their usage on a cluster in relation to
the size of their jobs and how long they ran. The raw job usage value is
defined as the sum of products of the number of nodes used (``nnodes``) and
time elapsed (``t_elapsed``):

.. code-block:: console

  RawUsage = sum(nnodes * t_elapsed)

This job usage factor per association has a half-life decay applied to it as
time passes. By default, this half-life decay is applied to jobs every week
for four weeks; jobs older than four weeks no longer play a role in determining
an association's job usage factor. The configuration parameters that determine
how to represent a half-life for jobs and how long to consider jobs as part of
an association's overall job usage are represented by **PriorityDecayHalfLife**
and  **PriorityUsageResetPeriod**, respectively. These parameters are
configured when the flux-accounting database is first created.

Example Job Usage Calculation
=============================

Below is an example of how flux-accounting calculates an association's current
job usage. Let's say a user has the following job records from the most
recent half-life period (by default, jobs that have completed in the
last week):

.. code-block:: console

     UserID Username  JobID         T_Submit            T_Run       T_Inactive  Nodes                                                                               R
  0    1002     1002    102 1605633403.22141 1605635403.22141 1605637403.22141      2  {"version":1,"execution": {"R_lite":[{"rank":"0","children": {"core": "0"}}]}}
  1    1002     1002    103 1605633403.22206 1605635403.22206 1605637403.22206      2  {"version":1,"execution": {"R_lite":[{"rank":"0","children": {"core": "0"}}]}}
  2    1002     1002    104 1605633403.22285 1605635403.22286 1605637403.22286      2  {"version":1,"execution": {"R_lite":[{"rank":"0","children": {"core": "0"}}]}}
  3    1002     1002    105 1605633403.22347 1605635403.22348 1605637403.22348      1  {"version":1,"execution": {"R_lite":[{"rank":"0","children": {"core": "0"}}]}}
  4    1002     1002    106 1605633403.22416 1605635403.22416 1605637403.22416      1  {"version":1,"execution": {"R_lite":[{"rank":"0","children": {"core": "0"}}]}}

From these job records, we can gather the following information:

* total nodes used (``nnodes``): 8
* total time elapsed (``t_elapsed``): 10000.0

So, the usage of the association from this current half life is:

.. code-block:: console

  sum(nnodes * t_elapsed) = (2 * 2000) + (2 * 2000) + (2 * 2000) + (1 * 2000) + (1 * 2000)
                          = 4000 + 4000 + 4000 + 2000 + 2000
                          = 16000

This current job usage is then added to the association's previous job usage
stored in the flux-accounting database. This sum then represents the
association's overall job usage.

****************************
Multi-Factor Priority Plugin
****************************

The multi-factor priority plugin is a jobtap_ plugin that generates
an integer job priority for incoming jobs in a Flux system instance. It uses
a number of factors to calculate a priority and, in the future, can add more
factors. Each factor :math:`F` has an associated integer weight :math:`W`
that determines its importance in the overall priority calculation. The
current factors present in the multi-factor priority plugin are:

fair-share
  The ratio between the amount of resources allocated vs. resources
  consumed. See the :ref:`Glossary definition <glossary-section>` for a more
  detailed explanation of how fair-share is utilized within flux-accounting.

queue
  A configurable factor assigned to a queue.

bank
  A configurable factor assigned to a bank.

urgency
  A user-controlled factor to prioritize their own jobs.

Thus the priority :math:`P` is calculated as follows:

:math:`P = (F_{fairshare} \times W_{fairshare}) + (F_{queue} \times W_{queue}) + (F_{bank} \times W_{bank}) + (F_{urgency} - 16)`

Each of these factors can be configured with a custom weight to increase their
relevance to the final calculation of a job's integer priority. By default,
fair-share has a weight of 100000 and the queue the job is submitted in has a
weight of 10000. These can be modified to change how a job's priority is
calculated. For example, if you wanted the queue to be more of a factor than
fair-share, you can adjust each factor's weight accordingly:

.. code-block:: toml

 [accounting.factor-weights]
 fairshare = 1000
 queue = 100000
 bank = 500

In addition to generating an integer priority for submitted jobs in a Flux
system instance, the multi-factor priority plugin also enforces per-association
job limits to regulate use of the system. The two per-association limits
enforced by this plugin are:

* **max_active_jobs**: a limit on how many *active* jobs an association can have at
  any given time. Jobs submitted after this limit has been hit will be rejected
  with a message saying that the association has hit their active jobs limit.

* **max_running_jobs**: a limit on how many *running* jobs an association can have
  at any given time. Jobs submitted after this limit has been hit will be held
  by adding a ``max-running-jobs-user-limit`` dependency until one of the
  association's currently running jobs finishes running.

Both "types" of jobs, *running* and *active*, are based on Flux's definitions
of job states_. *Active* jobs can be in any state but INACTIVE. *Running* jobs
are jobs in either RUN or CLEANUP states.

Queue Permissions Configuration
===============================

The priority plugin can enforce restrictions on which associations can submit
jobs under certain queues. This is done by configuring an association's list of
permissible queues in their ``queues`` attribute. If configured, these
permissions will be shared with the priority plugin and enforced when an
association submits a job. If an association tries to submit a job to a queue
where they do not have access, their job will be rejected during the job's
validation.

To enforce these kinds of permissions, ensure that both flux-accounting's
``queue_table`` is configured with the queues you want to restrict access to as
well as the associations' ``queues`` attributes.

.. note::

  If an association submits a job under a queue which flux-accounting does not
  know about (i.e it is not in flux-accounting's ``queue_table``), it will
  **still allow** the job to run.

example
-------

As an example, let's configure flux-accounting with the following three queues:

.. code-block:: console

  $ flux account add-queue bronze
  $ flux account add-queue silver
  $ flux account add-queue gold

And an association's ``queues`` attribute:

.. code-block:: console

  $ flux account add-user --username=user1 --bank=bankA --queues="bronze"

If the association attempts to submit a job to the ``silver`` queue, their job
will be rejected on submission:

.. code-block:: console

  $ flux job submit --queue=silver my_job
  Queue not valid for user: silver

Queue Priority Calculation Configuration
========================================

Mentioned above, the queue that a job is submitted under can influence its
calculated priority. Priorities specific to queues can be configured in the
flux-accounting database when they are first added:

.. code-block:: console

  $ flux account add-queue bronze --priority=100

Or changed later on:

.. code-block:: console

  $ flux account edit-queue bronze --priority=500

If a priority is not specified when a queue is added, it will have a priority
of 0, meaning it will not positively or negatively affect a job's integer
priority.

example
-------

Given an association with a fair-share value of 0.5, the priority plugin loaded
and configured to just use its default factor weights, let's walk through how a
job's priority could be affected by running under certain queues. Assume the
following configuration for queues and their associated priorities:

+-------------+----------+
| queue       | priority |
+=============+==========+
| bronze      | 100      |
+-------------+----------+
| silver      | 300      |
+-------------+----------+
| gold        | 500      |
+-------------+----------+

If the association submitted their job with default urgency in the ``bronze``
queue, their priority would be:

:math:`P = (0.5 \times 100000) + (100 \times 10000) + (16 - 16) = (50000) + (1000000) = 1050000`

versus this same job submitted in the ``gold`` queue:

:math:`P = (0.5 \times 100000) + (500 \times 10000) + (16 - 16) = (50000) + (5000000) = 5050000`

Queue Limit Configuration
=========================

Like per-association job limits, queues in flux-accounting can be configured to
enforce a max running jobs limit for associations. Each queue has a
``max_running_jobs`` property that can set when creating a queue for the first
time or changed at a later date:

.. code-block:: console

    $ flux account edit-queue bronze --max-running-jobs=3

The above example will set a max running jobs limit of 3 running jobs
per-association. Any subsequently submitted jobs will be held with a
queue-specific dependency until one of the association's currently running jobs
in that queue completes.

.. _glossary-section:

********
Glossary
********

association
  A 2-tuple combination of a username and bank name.

bank
  An account that contains associations.

fair-share
  A metric used to ensure equitable resource allocation among associations
  within a shared system. It represents the ratio between the amount of
  resources an association is allocated versus the amount actually consumed.
  The fair-share value influences an association's priority when submitting
  jobs to the system, adjusting dynamically to reflect current usage compared
  to allocated quotas. High consumption relative to allocation can decrease an
  association's fair-share value, reducing their priority for future resource
  allocation, thereby promoting balanced usage across all associations to
  maintain system fairness and efficiency.

.. note::

 The design of flux-accounting was driven by LLNL site requirements. Years ago,
 the design of `Slurm accounting`_ and its `multi-factor priority
 plugin`_ were driven by similar LLNL site requirements. We chose to
 reuse terminology and concepts from Slurm to facilitate a smooth transition to
 Flux. The flux-accounting code base is all completely new, however.

.. _documentation: https://sqlite.org/omitted.html

.. _Slurm accounting: https://slurm.schedmd.com/accounting.html

.. _multi-factor priority plugin: https://slurm.schedmd.com/priority_multifactor.html

.. _jobtap: https://flux-framework.readthedocs.io/projects/flux-core/en/latest/man7/flux-jobtap-plugins.html#flux-jobtap-plugins-7

.. _states: https://flux-framework.readthedocs.io/projects/flux-rfc/en/latest/spec_21.html
