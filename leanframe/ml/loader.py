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

from __future__ import annotations

from typing import TYPE_CHECKING, Union

from google.cloud import bigquery

if TYPE_CHECKING:
    import leanframe

import leanframe.ml.cluster
import leanframe.ml.decomposition
import leanframe.ml.linear_model


def from_bq(
    session: leanframe.Session, model: bigquery.Model
) -> Union[
    leanframe.ml.decomposition.PCA,
    leanframe.ml.cluster.KMeans,
    leanframe.ml.linear_model.LinearRegression,
]:
    """Load a BQML model to BigFrames ML.

    Args:
        session: a BigFrames session.
        model: a BigQuery model.

    Returns: a BigFrames ML model object."""
    if model.model_type == "LINEAR_REGRESSION":
        return leanframe.ml.linear_model.LinearRegression._from_bq(session, model)
    elif model.model_type == "KMEANS":
        return leanframe.ml.cluster.KMeans._from_bq(session, model)
    elif model.model_type == "PCA":
        return leanframe.ml.decomposition.PCA._from_bq(session, model)
    else:
        raise NotImplementedError(
            f"Model type {model.model_type} is not yet supported by BigFrames"
        )
