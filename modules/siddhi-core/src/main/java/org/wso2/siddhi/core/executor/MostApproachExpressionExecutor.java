/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.core.executor;

import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;

import static org.wso2.siddhi.query.api.definition.Attribute.Type;
import static org.wso2.siddhi.query.api.definition.Attribute.Type.OBJECT;

/**
 * Created by pchou on 18/9/26.
 */

public class MostApproachExpressionExecutor implements ExpressionExecutor {
    private ExpressionExecutor leftExpressionExecutor;
    private ExpressionExecutor rightExpressionExecutor;

    public MostApproachExpressionExecutor(
            ExpressionExecutor leftExpressionExecutor,
            ExpressionExecutor rightExpressionExecutor) {
        this.leftExpressionExecutor = leftExpressionExecutor;
        this.rightExpressionExecutor = rightExpressionExecutor;
    }

    @Override
    public Object execute(ComplexEvent event) {
        if (!(event instanceof StateEvent)) {
            return null;
        }
        StateEvent stateEvent = (StateEvent) event;
        StateEvent storedEvent = stateEvent.getNext();
        if (storedEvent == null) {
            StateEvent cloned = cloneStateEvent(stateEvent);
            stateEvent.setNext(cloned);
            return cloned;
        } else {
            double current = Math.abs(
                    getDoubleValue(this.leftExpressionExecutor, event)
                            - getDoubleValue(this.rightExpressionExecutor, event));
            double history =  Math.abs(
                    getDoubleValue(this.leftExpressionExecutor, storedEvent)
                            - getDoubleValue(this.rightExpressionExecutor, storedEvent));
            if (current < history) {
                StateEvent cloned = cloneStateEvent(stateEvent);
                stateEvent.setNext(cloned);
                return cloned;
            } else {
                return storedEvent;
            }
        }
    }

    @Override
    public Type getReturnType() {
        return OBJECT;
    }

    @Override
    public ExpressionExecutor cloneExecutor(String key) {
        return new MostApproachExpressionExecutor(leftExpressionExecutor.cloneExecutor(key),
            rightExpressionExecutor.cloneExecutor(key));
    }

    private StateEvent cloneStateEvent(StateEvent orignal) {
        StateEvent clonedSE = new StateEvent(orignal.getStreamEvents().length, orignal.getOutputData().length);
        for (int i = 0; i < orignal.getStreamEvents().length; i++) {
            clonedSE.setEvent(i, orignal.getStreamEvent(i));
        }
        return clonedSE;
    }

    private double getDoubleValue(ExpressionExecutor executor, ComplexEvent event) {
        Object value = executor.execute(event);
        if (value == null) {
            throw new SiddhiAppRuntimeException("[MostApproachExpressionExecutor]"
                    + "Failed to execute event to a non-nullable");
        }

        switch (executor.getReturnType()) {
            case BOOL:
                return ((Boolean) value).booleanValue() ? 1 : 0;
            case DOUBLE:
                return ((Double) value).doubleValue();
            case FLOAT:
                return ((Float) value).floatValue();
            case INT:
                return ((Integer) value).intValue();
            case LONG:
                return ((Long) value).longValue();
            default:
                throw new SiddhiAppRuntimeException("[MostApproachExpressionExecutor] Unknown return type");
        }
    }
}
