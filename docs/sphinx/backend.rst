
.. _backend:

Backend Operations
=====================

This describes the internal API that is common to all backends and all objects.


Location Information
-----------------------

Each object has a "location" which represents its on-disk position relative to other objects in the hierarchy.

.. doxygenclass:: tidas::backend_path
        :members:


