E-wallet dataset
================

Summary
-------

The dataset was obtained from an e-wallet system to provide a real-world example for testing Retentioneering tools. The dataset contains a stream of user actions on the web platform. The data was collected during three months. To protect end users privacy, as well as the the platform, all dates and keys have been modified. Do not try to reveal the identity of the platform.

License
-------

This work is licensed under the Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International 4.0 License. To view a copy of this license, visit `https://creativecommons.org/licenses/by-nc-sa/4.0/ <https://creativecommons.org/licenses/by-nc-sa/4.0/>`_. Retentioneering command cannot guarantee the completeness and correctness of the data or the validity of results based on the use of the dataset.  If you have any further questions or comments, please contact `retentioneering support <https://t.me/retentioneering_support>`_.

How to use
----------

The dataset is available as a CSV-file following this link.

:red:`TODO: insert the link`

Dataset description
-------------------

The dataset contains the list of the web-site pages visited by a user. Each record has the following fields:

— ``user_id``: the unique user identifier;

— ``event``: the name of a visited page;

— ``timestamp``: the time of the visit. The format is ``YYYY-MM-DDThh:mm:ss``.

Here is the full list of events:

.. raw:: html

    <table class="dataframe">
      <thead>
        <tr style="text-align: right;">
          <th>Page Name</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody>
        <tr>
          <th>home</th>
          <td>the first page of the platform</td>
        </tr>
        <tr>
          <th>landing</th>
          <td>one of the landing pages</td>
        </tr>
        <tr>
          <th>service_terms</th>
          <td>complete terms and conditions of the platform</td>
        </tr>
        <tr>
          <th>services</th>
          <td>pages with several additional services for unauthorized users</td>
        </tr>
        <tr>
          <th>registration</th>
          <td>a registration form</td>
        </tr>
        <tr>
          <th>login</th>
          <td>a login form</td>
        </tr>
        <tr>
          <th>profile_recovery</th>
          <td>a form that helps to restore the profile</td>
        </tr>
        <tr>
          <th>email_confirmed</th>
          <td>a message that the email is confirmed</td>
        </tr>
        <tr>
          <th>verify_email</th>
          <td>an email confirmation form</td>
        </tr>
        <tr>
          <th>main</th>
          <td>the main page in the authorized zone</td>
        </tr>
        <tr>
          <th>open_account_main</th>
          <td>a page to create a new account</td>
        </tr>
        <tr>
          <th>accounts_main</th>
          <td>a page with a list of all open accounts</td>
        </tr>
        <tr>
          <th>open_account_kids</th>
          <td>a page to create a new child account</td>
        </tr>
        <tr>
          <th>accounts_kids</th>
          <td>a page with a list of all open children's accounts</td>
        </tr>
        <tr>
          <th>account</th>
          <td>the main page for the account, it also shows the account balance</td>
        </tr>
        <tr>
          <th>account_details</th>
          <td>a page that shows banking information for the account, such as account number etc.</td>
        </tr>
        <tr>
          <th>account_info</th>
          <td>a page that shows some additional information for the account</td>
        </tr>

        <tr>
          <th>wallet</th>
          <td>the main page for financial operations</td>
        </tr>
        <tr>
          <th>wallet_deposit</th>
          <td>the money deposit page</td>
        </tr>
        <tr>
          <th>payment_selection</th>
          <td>a page for choosing of system through which deposit will be made</td>
        </tr>
        <tr>
          <th>wallet_deposit_success</th>
          <td>a successful deposit message</td>
        </tr>
        <tr>
          <th>wallet_deposit_fail</th>
          <td>a failed deposit message</td>
        </tr>
        <tr>
          <th>wallet_transfer</th>
          <td>the money transfer page</td>
        </tr>
        <tr>
          <th>wallet_withdrawal</th>
          <td>the money withdrawal page</td>
        </tr>
        <tr>
          <th>order_history</th>
          <td>a page with a list of all account activity</td>
        </tr>
        <tr>
          <th>order_statistics</th>
          <td>a page with some basic activity statistics</td>
        </tr>

        <tr>
          <th>id_verification</th>
          <td>a form to verify the user's identity</td>
        </tr>
        <tr>
          <th>profile</th>
          <td>the profile page</td>
        </tr>
        <tr>
          <th>profile_edit</th>
          <td>a page that allows editing personal information</td>
        </tr>
        <tr>
          <th>tariff_plans</th>
          <td>a page with conditions and a choice of different plans</td>
        </tr>
        <tr>
          <th>subscriptions</th>
          <td>the subscription management page</td>
        </tr>

        <tr>
          <th>promo</th>
          <td>a page showing some of the benefits of using the platform</td>
        </tr>
        <tr>
          <th>loyalty_program</th>
          <td>a page with different bonuses within the loyalty program</td>
        </tr>
        <tr>
          <th>referral_program</th>
          <td>a page that describes and helps manage the referral program</td>
        </tr>
        <tr>
          <th>special_offers</th>
          <td>a page with list of some limited time offers for authorized users</td>
        </tr>
        <tr>
          <th>support</th>
          <td>the support chat page</td>
        </tr>
        <tr>
          <th>page_not_found</th>
          <td>a request for non-existent page</td>
        </tr>
      </tbody>
    </table>
