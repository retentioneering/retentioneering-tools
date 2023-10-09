Tracking options
================

Instructions for managing tracking
----------------------------------

1. Disable tracking of Retentioneering methods, classes, and helpers. To turn off tracking for these elements, you can use the following code:

.. code-block:: python

    from retentioneering import RETE_CONFIG

    RETE_CONFIG.tracking.is_tracking_allowed = False
    RETE_CONFIG.save()

2. By using the `Do Not Track <https://support.google.com/chrome/answer/2790761?hl=en&co=GENIE.Platform%3DDesktop>`_ configuration in your browser, you have the ability to turn off tracking for the transition graph and preprocessing graph in Retentioneering.

.. note::

    It is important to note that turning off tracking may also limit the data available for analysis and impact the overall functionality and power of the Retentioneering product. It is recommended to weigh the benefits and drawbacks before making a decision on whether to turn off tracking or not.

    At Retentioneering, we are committed to protecting the privacy and security of our users. As such, we do not collect any sensitive user information including the information related to user datasets. This helps to ensure that our users' personal information remains confidential and secure.
