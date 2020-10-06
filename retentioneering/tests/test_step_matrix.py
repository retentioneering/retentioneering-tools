# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


from math import isclose

import retentioneering
from retentioneering import datasets

# global constants
REL_TOL = 0.0001
ABS_TOL = 0.0001

# import data
retentioneering.config.update({
    'event_col': 'event',
    'event_time_col': 'timestamp',
    'user_col': 'user_id'
})
data = datasets.load_simple_shop()
test_datasets = [dict(test_dataset=data)]


# util function to cycle through parameters for each test
def pytest_generate_tests(metafunc):
    # called once per each test function
    funcarglist = metafunc.cls.params[metafunc.function.__name__]
    argnames = sorted(funcarglist[0])
    metafunc.parametrize(
        argnames, [[funcargs[name] for name in argnames] for funcargs in funcarglist]
    )


class TestStepMatrix:
    # for each test we would like to run all cases
    params = {}.fromkeys(["test_step_matrix",
                          "test_step_matrix_thresh",
                          "test_step_matrix_targets",
                          "test_step_matrix_centered",
                          "test_step_matrix_sorting",
                          "test_step_matrix_diff"], test_datasets)

    def test_step_matrix(self, test_dataset):
        assert step_matrix_test(test_dataset, max_steps=20,
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=1,
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=100,
                                              precision=3,
                                              show_plot=False)

    def test_step_matrix_thresh(self, test_dataset):
        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.5,
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=1,
                                              show_plot=False)

    def test_step_matrix_targets(self, test_dataset):
        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.5,
                                              targets=['payment_done'],
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.5,
                                              targets=['payment_done', '_TEST_EVENT_'],
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.05,
                                              targets=['product1', ['cart', 'payment_done']],
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.05,
                                              targets=['product1', ['cart', 'payment_done']],
                                              accumulated='only',
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.05,
                                              targets=['product1', ['cart', 'payment_done']],
                                              accumulated='both',
                                              show_plot=False)

    def test_step_matrix_centered(self, test_dataset):
        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.05,
                                              centered={'event': 'cart',
                                                        'left_gap': 5,
                                                        'occurrence': 1},
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.05,
                                              centered={'event': 'cart',
                                                        'left_gap': 10,
                                                        'occurrence': 2},
                                              targets=['payment_done'],
                                              show_plot=False)

        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.05,
                                              centered={'event': 'cart',
                                                        'left_gap': 0,
                                                        'occurrence': 1},
                                              targets=['payment_done'],
                                              show_plot=False)

    def test_step_matrix_sorting(self, test_dataset):
        custom_order = ['catalog', 'cart', 'delivery_choice', 'delivery_courier', 'payment_card', 'payment_choice',
                        'payment_done', 'ENDED', 'THRESHOLDED_6']
        assert step_matrix_test(test_dataset, max_steps=20,
                                              thresh=0.5,
                                              centered={'event': 'payment_done',
                                                        'left_gap': 10,
                                                        'occurrence': 1},
                                              targets=[['catalog', 'cart']],
                                              sorting=custom_order,
                                              show_plot=False)

    def test_step_matrix_diff(self, test_dataset):
        assert step_matrix_diff_test(test_dataset, max_steps=20,
                                                   thresh=0.,
                                                   groups=({3744822}, {736567298}),
                                                   show_plot=False)

        g1 = set(test_dataset[test_dataset['event'] == 'payment_done']['user_id'].unique())
        g2 = set(test_dataset['user_id'].unique()) - g1
        assert step_matrix_diff_test(test_dataset, max_steps=20,
                                                   thresh=0.,
                                                   groups=(g1, g2),
                                                   show_plot=False)

        assert step_matrix_diff_test(test_dataset, max_steps=20,
                                                   thresh=0.,
                                                   groups=(g1, g2),
                                                   targets=['payment_done', 'cart'],
                                                   accumulated='both',
                                                   show_plot=False)

        assert step_matrix_diff_test(test_dataset, max_steps=20,
                                                   thresh=0.,
                                                   groups=(g1, g2),
                                                   targets=['payment_done', 'cart'],
                                                   accumulated='both',
                                                   centered={'event': 'delivery_choice',
                                                             'left_gap': 10,
                                                             'occurrence': 1},
                                                   show_plot=False)


def step_matrix_test(test_dataset, **kwargs):
    """
    """
    res_matrix = test_dataset.rete.step_matrix(**kwargs)

    # making sure columns sum up to 1:
    res_list = list(res_matrix.sum())
    return all(isclose(1, i, rel_tol=REL_TOL, abs_tol=ABS_TOL) for i in res_list)


def step_matrix_diff_test(test_dataset, **kwargs):
    """
    """
    res_matrix = test_dataset.rete.step_matrix(**kwargs)

    # making sure columns sum up to 0:
    res_list = list(res_matrix.sum())
    return all(isclose(0, i, rel_tol=REL_TOL, abs_tol=ABS_TOL) for i in res_list)
