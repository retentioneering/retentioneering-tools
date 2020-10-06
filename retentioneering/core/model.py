# * Copyright (C) 2020 Maxim Godzi, Anatoly Zaytsev, Retentioneering Team
# * This Source Code Form is subject to the terms of the Retentioneering Software Non-Exclusive License (License)
# * By using, sharing or editing this code you agree with the License terms and conditions.
# * You can obtain License text at https://github.com/retentioneering/retentioneering-tools/blob/master/LICENSE.md


from sklearn.inspection import permutation_importance as perm_imp
from retentioneering.visualization import plot
import pandas as pd


class ModelDescriptor(object):

    def __init__(self, model, data, target, **kwargs):
        self.data = data
        self.target = target
        self.feature_extraction_kwargs = kwargs
        self.feature_extraction_kwargs.update({'ngram_range': kwargs.get('feature_range')})
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

    def permutation_importance_raw(self, test, **kwargs):
        test_sample = self.prepare_test(test)
        test_target = test.rete.get_positive_users()
        test_target = test_sample.index.isin(test_target)
        return self.permutation_importance(test_sample, test_target, node_params=None, **kwargs)

    def permutation_importance(self, test_sample, test_target, node_params=None, **kwargs):
        """
        Calculates permutation importance of features.
        If ``node_params`` is not `None`, then plots graph weighted by permutation importance.

        Parameters
        -------
        test_sample: pd.DataFrame
            Test feature subsample.
        test_target: np.array
            Vector of targets for test sample.
        node_params: dict, optional
            Event mapping describing which nodes or edges should be highlighted by different colors for better visualisation. Dictionary keys are ``event_col`` values, while keys have the following possible values:
                - ``bad_target``: highlights node and all incoming edges with red color;
                - ``nice_target``: highlights node and all incoming edges with green color;
                - ``bad_node``: highlights node with red color;
                - ``nice_node``: highlights node with green color;
                - ``source``: highlights node and all outgoing edges with yellow color.
            Example ``node_params`` is shown below:
            ```
            {
                'lost': 'bad_target',
                'purchased': 'nice_target',
                'onboarding_welcome_screen': 'source',
                'choose_login_type': 'nice_node',
                'accept_privacy_policy': 'bad_node',
            }
            ```
            If ``node_params=None``, it will be constructed from ``retention_config`` variable, so that:
            ```
            {
                'positive_target_event': 'nice_target',
                'negative_target_event': 'bad_target',
                'source_event': 'source',
            }
            ```
            Default: ``None``
        """
        self.show_quality_metrics(test_sample, test_target)
        perm = perm_imp(self.mod, test_sample, test_target, random_state=0)
        perm.pop('importances')
        perm['feature'] = test_sample.columns
        self.permutation_importance_table = pd.DataFrame(perm).sort_values('importances_mean', ascending=False)
        plot.permutation_importance(self.permutation_importance_table, **kwargs)
        self._plot_perm_imp(perm, test_sample, node_params, **kwargs)
        return self.permutation_importance_table

    def show_quality_raw(self, test):
        test_sample = self.prepare_test(test)
        test_target = test.rete.get_positive_users()
        test_target = test_sample.index.isin(test_target)
        return self.show_quality_metrics(test_sample, test_target, use_print=False)

    def show_quality_metrics(self, test_sample, test_target, use_print=True):
        """
        Calculate model quality metrics.

        Parameters
        -------
        test_sample: pd.DataFrame
            Test feature subsample.
        test_target: np.array
            Vector of targets for test sample.
        use_print: bool, optional
            If ``True``, prints out ROC-AUC, PR-AUC and accuracy in prediction task and RMSE, MAE and R-Squared in regression task.
        Returns
        -------
        ROC-AUC, PR-AUC and accuracy metrics in prediction task and RMSE, MAE and R-Squared in regression task.

        Return type
        -------
        Int
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
            roc = roc_auc_score(test_target, preds)
            aps = average_precision_score(test_target, preds)
            ac = accuracy_score(test_target, preds > best_split)
            if use_print:
                print(f"""
                ROC-AUC: {roc}
                PR-AUC: {aps}
                Accuracy: {ac}
                """)
            return roc, aps, ac
        else:
            from sklearn.metrics import mean_squared_error
            from sklearn.metrics import mean_absolute_error
            from sklearn.metrics import r2_score
            import numpy as np
            preds = self.mod.predict(test_sample)
            if use_print:
                print(f"""
                RMSE: {np.sqrt(mean_squared_error(test_target, preds))}
                MAE: {mean_absolute_error(test_target, preds)}
                R-squared: {r2_score(test_target, preds)}
                """)

    @staticmethod
    def _plot_perm_imp(perm, test_sample, node_params, **kwargs):
        weights = dict(zip(test_sample.columns.tolist(), perm["importances_mean"]))

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

        plot.graph(pd.DataFrame(data), node_params, node_weights=node_weights, is_model=True, **kwargs)

    def visualize_results(self, plot_type='projections'):
        raise NotImplementedError('Sorry! This function is not ready now.')
        pass

    def predict(self, features):
        """
        Predicts probability of ``positive_target_event`` and ``negative_target_event`` in classifacation task or values of ``regeression_targets`` in regression task.

        Parameters
        -------
        features: pd.DataFrame
            Dataframe with test features. For more information refer to ``prepare_test()`` method.

        Returns
        -------
        Dataframe with predictions.

        Return type
        -------
        pd.DataFrame
        """
        if hasattr(self.mod, 'predict_proba'):
            return pd.DataFrame(self.mod.predict_proba(features), index=features.index, columns=[False, True])
        else:
            return pd.DataFrame(self.mod.predict(features), index=features.index, columns=['prediction'])

    def prepare_test(self, test):
        """
        Transforms test clickstream into train.

        Parameters
        ---------
        test: pd.DataFrame
            Raw clickstream.

        Returns
        --------
        Dataframe with test features.

        Return type
        -------
        pd.DataFrame
        """
        test = test.rete.extract_features(**self.feature_extraction_kwargs)
        test = test.loc[:, self.data.columns.tolist()]
        return test.fillna(0)

    def predict_raw(self, data):
        """
        Predicts probability of ``positive_target_event`` and ``negative_target_event`` in classifacation task or values of ``regeression_targets`` in regression task.

        Parameters
        --------
        data: pd.DataFrame
            Raw clickstream.

        Returns
        --------
        Dataframe with predictions.

        Return type
        --------
        pd.DataFrame
        """
        features = self.prepare_test(data)
        return self.predict(features)


class __LogRegWrapper__(object):
    def __init__(self, coef):
        self.coef = coef

    @property
    def feature_importances_(self):
        return self.coef
