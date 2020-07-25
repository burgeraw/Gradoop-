/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gellyStreamingMaster;

import gellyStreaming.gradoop.oldModel.GraphStream;
import gellyStreaming.gradoop.oldModel.SimpleEdgeStream;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Test;

public class TestNumberOfEntities extends AbstractTestBase {

    @Test
	public void testNumberOfVertices() throws Exception {
		/*
		 * Test numberOfVertices() with the sample graph
	     */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		graph.numberOfVertices().map(new MapFunction<Long, Tuple1<Long>>() {
			@Override
			public Tuple1<Long> map(Long value) throws Exception {
				return new Tuple1<>(value);
			}
		}).writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testNumberOfEdges() throws Exception {
		/*
		 * Test numberOfEdges() with the sample graph
	     */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1\n" +
                "2\n" +
                "3\n" +
                "4\n" +
                "5\n" +
                "6\n" +
                "7\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		graph.numberOfEdges().map(new MapFunction<Long, Tuple1<Long>>() {
			@Override
			public Tuple1<Long> map(Long value) throws Exception {
				return new Tuple1<>(value);
			}
		}).writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}
}
