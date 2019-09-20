# Copyright (C) 2019 Maxim Godzi, Anatoly Zaytsev, Dmitrii Kiselev
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

import eli5
from eli5.sklearn import PermutationImportance
from retentioneering.visualization import plot
import pandas as pd


class ModelDescriptor(object):

    def __init__(self, model, data, target, **kwargs):
        self.data = data
        self.target = target
        if hasattr(model, 'get_params'):
            model_filter = model.get_params(model)
            kwargs = {i: j for i, j in kwargs.items() if i in model_filter}

        try:
            self.mod = model(**kwargs)
        except:
            raise ValueError('Please use only keyword arguments from your model')
        self._fit_model()
        self.kwargs = kwargs

    def _fit_model(self):
        self.mod.fit(self.data, self.target)

    # def _fit_shap(self, test_sample):
    #     if not hasattr(self, 'shap_values'):
    #         import shap
    #         explainer = shap.KernelExplainer(self.mod.predict_proba, self.data, link="logit")
    #         shap_values = explainer.shap_values(test_sample, nsamples=100)
    #         setattr(self, 'shap_values', shap_values)
    #         setattr(self, 'shap_explainer', explainer)
    #
    # def shap_descriptor(self, test_sample, test_index=None):
    #     import shap
    #     """
    #     Describes model using [SHAP](https://github.com/slundberg/shap) force_plot
    #
    #     :param test_sample: test feature subsample
    #     :param test_index: idx of test example
    #     :return:
    #     """
    #     self._fit_shap(test_sample)
    #     explainer = getattr(self, 'shap_explainer')
    #     shap_values = getattr(self, 'shap_values')
    #     if test_index is None:
    #         test_index = 0
    #     else:
    #         test_index = test_sample.index.tolist().index(test_index)
    #     shap.force_plot(explainer.expected_value[1], shap_values[1][test_index, :],
    #                     test_sample.iloc[test_index, :], link="logit")

    def permutation_importance(self, test_sample, test_target, node_params=None, **kwargs):
        """
        Calculates permutation importance of features.
        If node_params is not None, then plots graph weighted by permutation importance.

        :param test_sample: test feature subsample
        :param test_target: vector of targets for test sample
        :param node_params: mapping describes which node should be highlighted by target or source type
            Node param should be represented in the following form

            ```{
                    'lost': 'bad_target',
                    'passed': 'nice_target',
                    'onboarding_welcome_screen': 'source',
                }```

            If mapping is not given, it will be constracted from config
        :return: Nothing
        """
        self.show_quality_metrics(test_sample, test_target)
        if hasattr(self.mod, 'coef_'):
            self._plot_perm_imp(__LogRegWrapper__(self.mod.coef_[0]), test_sample, node_params, **kwargs)
            return
        perm = PermutationImportance(self.mod, random_state=0).fit(test_sample, test_target)
        eli5.show_weights(perm, feature_names=[' '.join(i) if type(i) == tuple else i for i in test_sample.columns])
        self._plot_perm_imp(perm, test_sample, node_params, **kwargs)

    def show_quality_metrics(self, test_sample, test_target):
        """
        Print metrics of quality for model

        :param test_sample: test feature subsample
        :param test_target: vector of targets for test sample
        :return:
        """
        if hasattr(self.mod, 'predict_proba'):
            from sklearn.metrics import accuracy_score
            from sklearn.metrics import roc_auc_score
            from sklearn.metrics import average_precision_score
            preds = self.mod.predict_proba(test_sample)[:, 1]
            split = {}
            for i in range(1, 100):
                split.update({i: accuracy_score(test_target, preds > (i / 100))})
            best_split = pd.Series(split).idxmax() / 100
            print(f"""
            ROC-AUC: {roc_auc_score(test_target, preds)}
            PR-AUC: {average_precision_score(test_target, preds)}
            Accuracy: {accuracy_score(test_target, preds > best_split)}
            """)
        else:
            from sklearn.metrics import mean_squared_error
            from sklearn.metrics import mean_absolute_error
            from sklearn.metrics import r2_score
            import numpy as np
            preds = self.mod.predict(test_sample)
            print(f"""
            RMSE: {np.sqrt(mean_squared_error(test_target, preds))}
            MAE: {mean_absolute_error(test_target, preds)}
            R-squared: {r2_score(test_target, preds)}
            """)

    @staticmethod
    def _plot_perm_imp(perm, test_sample, node_params, **kwargs):
        weights = dict(zip(test_sample.columns.tolist(), perm.feature_importances_))

        if node_params is None:
            node_params = {}
            node_flg = True
        else:
            node_flg = False
        node_weights = {}
        for node, val in weights.items():
            if len(node) > 1:
                continue
            if node_flg:
                node_params.update({
                    node[0]: 'nice_node' if weights[node] >= 0 else 'bad_node',
                })
            node_weights.update({
                node[0]: val
            })

        edge_cols = [i for i in test_sample.columns if len(i) == 2]
        if len(edge_cols) == 0:
            print("Sorry, you use only unigrams, change ngram_range to (1, 2) or greater")
            return
        data = []
        for key in edge_cols:
            data.append([key[0], key[1], weights.get(key)])

        plot.graph(pd.DataFrame(data), node_params, node_weights=node_weights, **kwargs)

    def visualize_results(self, plot_type='projections'):
        raise NotImplementedError('Sorry! This function is not ready now.')
        pass

    def predict(self, features):
        """
        Predicts probability of positive and negative targets (in classifacation task)
        or values of regeression_targets (in regression task)

        :param features: features for model
        :return: pd.DataFrame with predictions
        """
        if hasattr(self.mod, 'predict_proba'):
            return pd.DataFrame(self.mod.predict_proba(features), index=features.index, columns=[False, True])
        else:
            return pd.DataFrame(self.mod.predict(features), index=features.index, columns=['prediction'])


class __LogRegWrapper__(object):
    def __init__(self, coef):
        self.coef = coef

    @property
    def feature_importances_(self):
        return self.coef
