Simple shop dataset
===================

SUMMARY
-------

The Simple shop dataset is our artificial dataset simulating purchases in an online store. There are two products in the store catalog, two delivery methods and two types of payment.

HOW TO USE
----------

The dataset can be loaded either as a pandas DataFrame or as an ``Eventstream``.

.. code-block:: python

    from retentioneering import datasets

    dataframe = datasets.load_simple_shop(as_dataframe=True)
    stream = datasets.load_simple_shop()

DATASET DESCRIPTION
-------------------

The dataset contains a list of customer actions and choices on the website. Each record/line in the file has the following fields/format:

— ``user_id``: the unique user identifier;

— ``event``: a name of customer action;

— ``timestamp``: a visit time. The format is ``YYYY-MM-DDThh:mm:ss.mmmmmm``.

The full  list of events:

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
          <th>main</th>
          <td>visiting the first page of the store</td>
        </tr>
        <tr>
          <th>catalog</th>
          <td>viewing product catalog</td>
        </tr>
        <tr>
          <th>product1</th>
          <td>adding the first item to the cart</td>
        </tr>
        <tr>
          <th>product2</th>
          <td>adding the first item to the cart</td>
        </tr>
        <tr>
          <th>cart</th>
          <td>visiting the page with selected products</td>
        </tr>
        <tr>
          <th>delivery_choice</th>
          <td>visiting the page with the choice of delivery method</td>
        </tr>
        <tr>
          <th>delivery_courier</th>
          <td>choice of the first delivery method</td>
        </tr>
        <tr>
          <th>delivery_pickup</th>
          <td>choice of the second delivery method</td>
        </tr>
        <tr>
          <th>payment_choice</th>
          <td>visiting the page with the choice of payment method</td>
        </tr>
        <tr>
          <th>payment_card</th>
          <td>choice of the first delivery method</td>
        </tr>
        <tr>
          <th>payment_cash</th>
          <td>choice of the second delivery method</td>
        </tr>
        <tr>
          <th>payment_done</th>
          <td>successful purchase</td>
        </tr>
      </tbody>
    </table>
