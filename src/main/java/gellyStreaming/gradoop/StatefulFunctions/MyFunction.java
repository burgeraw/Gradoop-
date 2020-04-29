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
package gellyStreaming.gradoop.StatefulFunctions;

import gellyStreaming.gradoop.StatefulFunctions.MyMessages.MyInputMessage;
import gellyStreaming.gradoop.StatefulFunctions.MyMessages.MyOutputMessage;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.statefun.sdk.state.PersistedValue;

final class MyFunction implements StatefulFunction {

  @Persisted
  private final PersistedValue<Integer> degree = PersistedValue.of("degree", Integer.class);
  @Persisted
  private final PersistedTable<String, String> incomingEdges = PersistedTable.of("inList", String.class, String.class);
  @Persisted
  private final PersistedTable<String, String> outgoingEdges = PersistedTable.of("outList", String.class, String.class);

  @Override
  public void invoke(Context context, Object input) {
    if (!(input instanceof MyInputMessage)) {
      throw new IllegalArgumentException("Unknown message received " + input);
    }
    MyInputMessage in = (MyInputMessage) input;
    if(in.getSrcId().equals(context.self().id())) {
      outgoingEdges.set(in.getTrgId(), in.getValue());
    } else if (in.getTrgId().equals(context.self().id())) {
      incomingEdges.set(in.getSrcId(), in.getValue());
    }
    Integer newDegree = degree.getOrDefault(0) + 1;
    degree.set(newDegree);
    MyOutputMessage out = new MyOutputMessage(context.self().id(), ""+degree.get(), incomingEdges.entries().toString(), outgoingEdges.entries().toString());

    context.send(MyConstants.RESULT_EGRESS1, out);
  }
}
