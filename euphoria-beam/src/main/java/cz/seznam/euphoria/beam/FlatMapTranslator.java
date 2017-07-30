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

import cz.seznam.euphoria.beam.io.ListCollector;
import cz.seznam.euphoria.core.client.functional.UnaryFunctor;
import cz.seznam.euphoria.core.client.operator.FlatMap;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

import java.util.ArrayList;
import java.util.List;

class FlatMapTranslator implements OperatorTranslator<FlatMap> {

  @Override
  @SuppressWarnings("unchecked")
  public PCollectionList<?> translate(FlatMap operator, BeamExecutorContext context) {
    final UnaryFunctor functor = operator.getFunctor();
    final Mapper mapper = new Mapper(functor);
    final List<PCollection<Object>> pcs = new ArrayList<>();
    for (PCollection<?> partition : context.getInput(operator).getAll()) {
      pcs.add(partition.apply(FlatMapElements.<Object, Object>via(mapper)));
    }
    return PCollectionList.of(pcs);
  }

  private static class Mapper extends SimpleFunction<Object, Iterable<Object>> {

    private final UnaryFunctor functor;
    private transient ListCollector collector;

    Mapper(UnaryFunctor functor) {
      this.functor = functor;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Iterable<Object> apply(Object input) {
      if (collector == null) {
        collector = new ListCollector();
      }
      collector.clear();
      functor.apply(input, collector);
      return collector.get();
    }
  }
}
