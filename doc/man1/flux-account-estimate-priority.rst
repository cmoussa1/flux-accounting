.. flux-help-section: flux account

=================================
flux-account-estimate-priority(1)
=================================


SYNOPSIS
========

**flux** **account** **estimate-priority** [USERNAME] [OPTIONS]


DESCRIPTION
===========

.. program:: flux account estimate-priority

:program:`flux account estimate-priority` estimates the job priority that the
multi-factor priority plugin would assign for a user, bank, queue, and
urgency. The command reads the current flux-accounting database and does not
submit a job.

If USERNAME is omitted, the command estimates priority for the caller's user
account. If no bank is selected, the user's stored default bank is used.

The estimate uses the built-in priority formula:

.. code-block:: text

    priority = round(fairshare * w_fairshare
                     + queue_priority * w_queue
                     + bank_priority * w_bank
                     + (urgency - 16) * w_urgency)

Urgency ``0`` is a hold and returns the minimum priority. Urgency ``31`` is an
expedite and returns the maximum priority. Other values are rounded half away
from zero and clamped to the Flux priority range.

The command uses database values. A running plugin may briefly lag the database
until the next priority update or plugin reload. Queue, bank, fair-share, and
weight changes are therefore reflected according to the database at command
time, not by querying the loaded plugin.

Accounting limits are not evaluated. The estimate does not check job size,
running job limits, active job limits, queue resource limits, project access,
or other policy checks that require a concrete jobspec. It only mirrors the
association and queue validation needed before priority calculation.

If ``--queue`` is omitted, the queue contribution is zero. This command does
not ask Flux to resolve a scheduler default queue. A queue unknown to
flux-accounting also contributes zero unless ``deny_unknown_queues`` is enabled
in the accounting configuration, in which case the estimate is rejected.


OPTIONS
=======

.. option:: --bank=BANK

    Estimate priority for USERNAME under BANK. This option is mutually
    exclusive with ``--all-banks``.

.. option:: --all-banks

    Estimate priority for every association owned by USERNAME. Results are
    ordered by bank. Rejected associations are included in the output and the
    command succeeds.

.. option:: --queue=QUEUE

    Estimate priority as if the job requested QUEUE. If QUEUE is known to
    flux-accounting, the association must be permitted to use it.

.. option:: --urgency=N

    Estimate priority with urgency N. Valid values are ``0`` through ``31``.
    Only the instance owner may request urgency greater than ``16``.

.. option:: --json

    Output a JSON list of association estimate objects. Each object includes
    identity fields, factor values, weights, acceptance status, rejection
    reason, computed priority, and ``limits_evaluated: false``.

.. option:: -o/--format=FORMAT

    Specify output format using Python's string format syntax. This takes
    precedence over ``--json`` if both options are supplied.

.. option:: -v/--verbose

    Include the formula substitution for each accepted association.


OUTPUT
======

The default table includes rejected association details and notes about
unevaluated limits, omitted queues, and current-database semantics. JSON and
custom-format output omit these prose notes so scripts can consume only the
requested machine-readable fields.

For a single-bank estimate, a rejected association causes the command to fail.
With ``--all-banks``, rejected rows are included and accepted rows still show
their computed priority.
