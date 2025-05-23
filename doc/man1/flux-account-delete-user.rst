.. flux-help-section: flux account

===========================
flux-account-delete-user(1)
===========================


SYNOPSIS
========

**flux** **account** **delete-user** USERNAME BANK

DESCRIPTION
===========

.. program:: flux account delete-user

:program:`flux account delete-user` will set an association's ``active``
field to ``0`` in the ``association_table``, disabling them from being able
to submit and run jobs. It will not remove the association from the SQLite
database, however, as their job usage can still contribute to their bank's
fair-share up until the job usage reset period. Associations can be reactivated
by simply re-adding them to the ``association_table`` with
``flux account add-user``.

To actually remove an association from the ``association_table``, pass the
``--force`` option.

.. warning::
    Permanently deleting rows from the ``association_table`` or ``bank_table``
    can affect the fair-share calculation for other rows in their respective
    tables. Proceed with caution when deleting rows with ``--force``.

SEE ALSO
========

:man1:`flux-account-add-user`
