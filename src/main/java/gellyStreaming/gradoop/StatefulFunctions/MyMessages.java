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


final class MyMessages {

  static final class MyInputMessage {
    private final String srcId;
    private final String trgId;
    private final String value;

    MyInputMessage(String srcId, String trgId, String value) {
      this.srcId = srcId;
      this.trgId = trgId;
      this.value = value;
    }

    String getSrcId() {
      return srcId;
    }

    String getTrgId() {
      return trgId;
    }

    String getValue() { return value; }
  }

  static final class MyInputMessage2 {
    private final String srcId;
    private final String trgId;

    MyInputMessage2(String srcId, String trgId) {
      this.srcId = srcId;
      this.trgId = trgId;
    }

    String getSrcId() { return srcId;}

    String getTrgId() { return trgId;}
  }

  static final class CheckIfTriangle {
    private final Iterable<String> potentialTriangles;

    CheckIfTriangle(Iterable<String> potentialTriangles) {
      this.potentialTriangles = potentialTriangles;
    }

    Iterable<String> getPotentialTriangles() { return this.potentialTriangles; }

  }

  static final class UpdateTriangleCount {
    private final Integer newTriangles;

    UpdateTriangleCount(Integer newTriangles) {
      this.newTriangles = newTriangles;
    }

    Integer getNewTriangles() { return newTriangles; }
  }

  static final class MyTriangleOutputMessage {
    private final String vertexId1SRC;
    private final String vertexId2;
    private final String vertexId3;
    private final Integer triangleCount;

    MyTriangleOutputMessage(String vertexId1SRC, String vertexId2, String vertexId3, Integer triangleCount) {
      this.triangleCount = triangleCount;
      this.vertexId1SRC = vertexId1SRC;
      this.vertexId2 = vertexId2;
      this.vertexId3 = vertexId3;
    }

    String getVertexId1SRC() { return vertexId1SRC; }

    String getVertexId2() { return vertexId2; }

    String getVertexId3() { return vertexId3; }

    Integer getTriangleCount() { return  triangleCount; }

    @Override
    public String toString() {
      return "MyTriangleOutputMessage{" +
              "vertexId1SRC='" + vertexId1SRC + '\'' +
              ", vertexId2='" + vertexId2 + '\'' +
              ", vertexId3='" + vertexId3 + '\'' +
              ", triangleCount=" + triangleCount +
              '}';
    }
  }

  static final class MyOutputMessage {
    private final String vertexId;
    private final String degree;
    private final String inList;
    private final String outList;

    MyOutputMessage(String vertexId, String degree, String inList, String outList) {
      this.vertexId = vertexId;
      this.degree = degree;
      this.inList = inList;
      this.outList = outList;
    }

    String getVertexId() {
      return vertexId;
    }

    String getDegree() {
      return degree;
    }

    String getInList() {return inList;}

    String getOutList() {return outList;}

    @Override
    public String toString() {
      return String.format("VertexID: %s, degree: %s, Incomming EdgeIDs: %s, Outgoing EdgeIds: %s)", getVertexId(), getDegree(), getInList(), getOutList());
    }
  }
}
