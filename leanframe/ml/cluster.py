# Copyright 2023 Google LLC, LeanFrame Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Clustering models. This module is styled after Scikit-Learn's cluster module:
https://scikit-learn.org/stable/modules/clustering.html"""

from __future__ import annotations

from typing import cast, Dict, List, Optional, TYPE_CHECKING

from google.cloud import bigquery

if TYPE_CHECKING:
    import leanframe

import leanframe.ml.api_primitives
import leanframe.ml.core


class KMeans(leanframe.ml.api_primitives.BaseEstimator):
    """K-Means clustering.

    Args:
        n_clusters: int, default=8. The number of clusters to form as well as the number of centroids to generate.
    """

    def __init__(self, n_clusters=8):
        self.n_clusters = n_clusters
        self._bqml_model: Optional[leanframe.ml.core.BqmlModel] = None

    @staticmethod
    def _from_bq(session: leanframe.Session, model: bigquery.Model) -> KMeans:
        assert model.model_type == "KMEANS"

        kwargs = {}

        # See https://cloud.google.com/bigquery/docs/reference/rest/v2/models#trainingrun
        last_fitting = model.training_runs[-1]["trainingOptions"]
        if "numClusters" in last_fitting:
            kwargs["n_clusters"] = int(last_fitting["numClusters"])

        new_kmeans = KMeans(**kwargs)
        new_kmeans._bqml_model = leanframe.ml.core.BqmlModel(session, model)
        return new_kmeans

    @property
    def _bqml_options(self) -> Dict[str, str | int | float | List[str]]:
        """The model options as they will be set for BQML"""
        return {"model_type": "KMEANS", "num_clusters": self.n_clusters}

    def fit(self, X: leanframe.DataFrame, transforms: Optional[List[str]] = None):
        """Fit the model to training data

        Args:
            X: a BigFrames Dataframe with training data

            transforms: an optional list of SQL expressions to apply over top of
                the model inputs as preprocessing. This preprocessing will be
                automatically reapplied to new input data (e.g. in .predict), and
                may contain steps (like ML.STANDARD_SCALER) that fit to the
                training data"""
        self._bqml_model = leanframe.ml.core.create_bqml_model(
            train_X=X,
            transforms=transforms,
            options=self._bqml_options,
        )

    def predict(self, X: leanframe.DataFrame) -> leanframe.DataFrame:
        """Predict the closest cluster for each sample in X.

        Args:
            X: a BigFrames DataFrame to predict.

        Returns: a BigFrames Dataframe representing predicted result.
        """
        if not self._bqml_model:
            raise RuntimeError("A model must be fitted before predict")

        return cast(
            leanframe.dataframe.DataFrame, self._bqml_model.predict(X)[["CENTROID_ID"]]
        )

    def to_gbq(self, model_name: str, replace: bool = False) -> KMeans:
        """Save the model to Google Cloud BigQuey.

        Args:
            model_name: the name of the model.
            replace: whether to replace if the model already exists. Default to False.

        Returns: saved model."""
        if not self._bqml_model:
            raise RuntimeError("A model must be fitted before it can be saved")

        new_model = self._bqml_model.copy(model_name, replace)
        return new_model.session.read_gbq_model(model_name)
