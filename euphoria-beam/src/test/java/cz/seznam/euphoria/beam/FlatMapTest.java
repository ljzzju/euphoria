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

import cz.seznam.euphoria.core.client.flow.Flow;
import cz.seznam.euphoria.core.client.io.ListDataSink;
import cz.seznam.euphoria.core.client.io.ListDataSource;
import cz.seznam.euphoria.core.client.operator.MapElements;
import cz.seznam.euphoria.core.executor.Executor;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;


/**
 * Test {@code FlatMap} operator's integration with beam.
 */
public class FlatMapTest {
  
  private Executor executor;

  @Before
  public void setUp() {
    final String[] args = {"--runner=DirectRunner"};
    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    executor = new BeamExecutor(options);
  }

  @Test
  public void testSimpleMap() throws ExecutionException, InterruptedException, TimeoutException {

    final Flow flow = Flow.create();

    final ListDataSource<Integer> input = ListDataSource.bounded(
        Arrays.asList(1, 2, 3),
        Arrays.asList(2, 3, 4));

    final ListDataSink<Integer> output = ListDataSink.get(2);

    MapElements.of(flow.createInput(input))
        .using(i -> i + 1)
        .output()
        .persist(output);

    executor.submit(flow).get(5000, TimeUnit.MILLISECONDS);

    assertEquals(Arrays.asList(2, 3, 4), output.getOutput(0));
    assertEquals(Arrays.asList(3, 4, 5), output.getOutput(1));
  }
}
