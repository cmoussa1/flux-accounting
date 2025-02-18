.. flux-help-section: flux account

=========================
flux-account-add-queue(1)
=========================


SYNOPSIS
========

**flux** **account** **add-queue** QUEUE [OPTIONS]

DESCRIPTION
===========

.. program:: flux account add-queue

:program:`flux account add-queue` will add a queue to the ``queue_table`` in
the flux-accounting database. Different properties and limits can be set for
each queue:

.. option:: --min-nodes-per-job

    The minimum number of nodes required to run jobs in this queue.

.. option:: --max-nodes-per-job

    The maximum number of nodes required to run jobs in this queue.

.. option:: --max-time-per-job

    The max time a job can be running in this queue.

.. option:: --priority

    An associated priority to be applied to jobs submitted to this queue.
