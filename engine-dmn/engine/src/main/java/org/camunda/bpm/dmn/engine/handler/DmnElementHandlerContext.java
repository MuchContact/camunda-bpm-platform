/* Licensed under the Apache License, Version 2.0 (the "License");
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

package org.camunda.bpm.dmn.engine.handler;

import org.camunda.bpm.dmn.engine.DmnDecision;
import org.camunda.bpm.dmn.engine.DmnDecisionModel;
import org.camunda.bpm.dmn.engine.type.DataTypeTransformerFactory;
import org.camunda.bpm.model.dmn.DmnModelInstance;

public interface DmnElementHandlerContext {

  DmnModelInstance getModelInstance();

  DmnDecisionModel getDecisionModel();

  Object getParent();

  DmnDecision getDecision();

  DataTypeTransformerFactory getDataTypeTransformerFactory();

}
