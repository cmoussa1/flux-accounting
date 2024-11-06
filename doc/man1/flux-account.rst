.. flux-help-section: flux account

===============
flux-account(1)
===============


SYNOPSIS
========

**flux** **account** [*COMMAND*] [OPTIONS]

DESCRIPTION
===========

.. program:: flux account

:program:`flux account` provides an interface to the SQLite database containing
information regarding banks, associations, queues, projects, and archived jobs.
It also provides administrative commands like exporting and populating the DB's
information to and from ``.csv`` files, updating the database when new versions
of flux-accounting are released, and more.

DATABASE ADMINISTRATION
~~~~~~~~~~~~~~~~~~~~~~~

``create-db``

Create the flux-accounting database.

See flux account-create-db(1) for more details.

``pop-db``

Populate a flux-accounting database with ``.csv`` files.

See flux account-pop-db(1) for more details.

``export-db``

Export a flux-accounting database into ``.csv`` files.

See flux account-export-db(1) for more details.

USER MANAGEMENT
~~~~~~~~~~~~~~~

``view-user``

View information about an association in the flux-accounting database.

See flux account-view-user(1) for more details.

``add-user``

Add an association to the flux-accounting database.

See flux account-add-user(1) for more details.

``delete-user``

Set an association to inactive in the flux-accounting database.

See flux account-delete-user(1) for more details.

``edit-user``

Modify an attribute for an association in the flux-accounting database.

See flux account-edit-user(1) for more details.

BANKS
=====

``view-bank``

``add-bank``

``delete-bank``

``edit-bank``

``list-banks``

QUEUES
======

``view-queue``

``add-queue``

``delete-queue``

``edit-queue``

PROJECTS
========

``view-project``

``add-project``

``delete-project``

``list-projects``

JOB RECORDS
===========

``view-job-records``

``update-usage``

``scrub-old-jobs``
