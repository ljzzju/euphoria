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

import cz.seznam.euphoria.core.client.graph.DAG;
import cz.seznam.euphoria.core.client.graph.Node;
import cz.seznam.euphoria.core.client.operator.Operator;
import cz.seznam.euphoria.shaded.guava.com.google.common.collect.Iterables;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

class BeamExecutorContext {

  private final DAG<Operator<?, ?>> dag;
  private final Map<Operator<?, ?>, PCollectionList<?>> outputs = new HashMap<>();
  private final Pipeline pipeline;

  BeamExecutorContext(DAG<Operator<?, ?>> dag, Pipeline pipeline) {
    this.dag = dag;
    this.pipeline = pipeline;
  }

  PCollectionList<?> getInput(Operator<?, ?> operator) {
    return Iterables.getOnlyElement(getInputs(operator));
  }

  List<PCollectionList<?>> getInputs(Operator<?, ?> operator) {
    return getInputOperators(operator).stream()
        .map(p -> {
          final PCollectionList<?> out = outputs.get(dag.getNode(p).get());
          if (out == null) {
            throw new IllegalArgumentException(
                "Output missing for operator " + p.getName());
          }
          return out;
        })
        .collect(toList());
  }

  List<Operator<?, ?>> getInputOperators(Operator<?, ?> operator) {
    return dag.getNode(operator).getParents().stream()
        .map(Node::get)
        .collect(toList());
  }

  Optional<PCollectionList<?>> getOutput(Operator<?, ?> operator) {
    return Optional.ofNullable(outputs.get(operator));
  }

  void setOutput(Operator<?, ?> operator, PCollectionList<?> output) {
    PCollectionList<?> prev = outputs.put(operator, output);
    if (prev != null) {
      throw new IllegalStateException(
          "Operator(" + operator.getName() + ") output already processed");
    }
  }

  Pipeline getPipeline() {
    return pipeline;
  }
}
