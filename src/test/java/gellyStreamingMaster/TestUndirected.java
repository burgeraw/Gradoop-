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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.NullValue;
import org.junit.Test;

public class TestUndirected extends AbstractTestBase {

	@Test
	public void testProgram() throws Exception {
        /*
		 * Test undirected() with the sample graph
	     */
        final String resultPath = getTempDirPath("result");
        final String expectedResult = "1,2,12\n" +
                "2,1,12\n" +
                "1,3,13\n" +
                "3,1,13\n" +
                "2,3,23\n" +
                "3,2,23\n" +
                "3,4,34\n" +
                "4,3,34\n" +
                "3,5,35\n" +
                "5,3,35\n" +
                "4,5,45\n" +
                "5,4,45\n" +
                "5,1,51\n" +
                "1,5,51\n";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		GraphStream<Long, NullValue, Long> graph = new SimpleEdgeStream<>(GraphStreamTestUtils.getLongLongEdgeDataStream(env), env);
		graph.undirected().getEdges().writeAsCsv(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
        compareResultsByLinesInMemory(expectedResult, resultPath);
	}
}
