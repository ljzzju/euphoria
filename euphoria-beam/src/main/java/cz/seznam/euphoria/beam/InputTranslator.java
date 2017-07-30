/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cz.seznam.euphoria.beam;

import cz.seznam.euphoria.beam.io.BeamBoundedSource;
import cz.seznam.euphoria.core.client.dataset.Dataset;
import cz.seznam.euphoria.core.executor.FlowUnfolder;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.List;

class InputTranslator implements OperatorTranslator<FlowUnfolder.InputOperator> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollectionList<?> translate(FlowUnfolder.InputOperator operator,
                                      BeamExecutorContext context) {
    final List<PCollection<Object>> pcs = new ArrayList<>();
    final Dataset<Object> dataset = operator.output();
    if (dataset.getSource() == null) {
      throw new IllegalArgumentException();
    }
    final int numPartitions = dataset.getSource().getPartitions().size();
    for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
      pcs.add((PCollection) context.getPipeline().apply(
          Read.from(BeamBoundedSource.wrap(dataset.getSource(), partitionId))));
    }
    return PCollectionList.of(pcs);
  }
}
