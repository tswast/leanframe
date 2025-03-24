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

from typing import cast, TYPE_CHECKING

if TYPE_CHECKING:
    import leanframe

import leanframe.ml.api_primitives
import leanframe.ml.core

_REMOTE_LLM_MODEL_CODE = "CLOUD_AI_LARGE_LANGUAGE_MODEL_V1"
_TEXT_GENERATE_RESULT_COLUMN = "ml_generate_text_result"


class PaLMTextGenerator(leanframe.ml.api_primitives.BaseEstimator):
    """PaLM text generator LLM model.

    Args:
        session: BQ session to create the model
        connection_name: connection to connect with remote service. str of the format <PROJECT_NUMBER/PROJECT_ID>.<REGION>.<CONNECTION_NAME>"""

    def __init__(self, session: leanframe.Session, connection_name: str):
        self.session = session
        self.connection_name = connection_name
        self._bqml_model: leanframe.ml.core.BqmlModel = self._create_bqml_model()

    def _create_bqml_model(self):
        options = {"remote_service_type": _REMOTE_LLM_MODEL_CODE}

        return leanframe.ml.core.create_bqml_remote_model(
            session=self.session, connection_name=self.connection_name, options=options
        )

    def predict(self, X: leanframe.DataFrame) -> leanframe.DataFrame:
        """Predict the result from input DataFrame.

        Args:
            X: input DataFrame, which needs to contain a column with name "prompt". Only the column will be used as input.

        Returns: output DataFrame with only 1 column as the JSON output results."""
        df = self._bqml_model.generate_text(X)
        return cast(
            leanframe.DataFrame,
            df[[_TEXT_GENERATE_RESULT_COLUMN]],
        )
