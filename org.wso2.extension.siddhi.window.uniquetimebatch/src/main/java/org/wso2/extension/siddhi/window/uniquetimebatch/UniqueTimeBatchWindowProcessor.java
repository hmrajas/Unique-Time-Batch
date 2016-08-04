/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
package org.wso2.extension.siddhi.window.uniquetimebatch;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UniqueTimeBatchWindowProcessor extends WindowProcessor implements SchedulingProcessor, FindableProcessor {

    private long timeInMilliSeconds;
    private long nextEmitTime = -1;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<>(false);
    private ComplexEventChunk<StreamEvent> eventsToBeExpired = null;
    private ConcurrentHashMap<String, StreamEvent> map = new ConcurrentHashMap<>();
    private StreamEvent resetEvent = null;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private boolean isStartTimeEnabled = false;
    private long startTime = 0;
    private VariableExpressionExecutor[] variableExpressionExecutors;

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
        this.eventsToBeExpired = new ComplexEventChunk<>(false);
        if (attributeExpressionExecutors.length == 2) {
            variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("UniqueTime window's parameter time should be either" +
                            " int or long, but found " + attributeExpressionExecutors[0].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Unique Time Batch window should have constant for time parameter but " +
                        "found a dynamic attribute " + attributeExpressionExecutors[0].getClass().getCanonicalName());
            }
            // start time
            isStartTimeEnabled = true;
            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                startTime = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
            } else {
                startTime = Long.parseLong(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue()));
            }
        } else {
            throw new ExecutionPlanValidationException("Unique Time Batch window should only have two parameters. (<int|long|time> windowTime), but found " + attributeExpressionExecutors.length + " input attributes");
        }
        //variableExpressionExecutors[0]= (VariableExpressionExecutor) attributeExpressionExecutors[0];
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
        synchronized (this) {
            if (nextEmitTime == -1) {
                long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                if (isStartTimeEnabled) {
                    nextEmitTime = getNextEmitTime(currentTime);
                } else {
                    nextEmitTime = executionPlanContext.getTimestampGenerator().currentTime() + timeInMilliSeconds;
                }
                scheduler.notifyAt(nextEmitTime);
            }
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            boolean sendEvents;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            } else {
                sendEvents = false;
            }
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                currentEventChunk.add(clonedStreamEvent);
            }
            StreamEvent oldEvent;
            streamEventChunk.clear();
            if (sendEvents) {
                if (eventsToBeExpired.getFirst() != null) {
                    while (eventsToBeExpired.hasNext()) {
                        StreamEvent expiredEvent = eventsToBeExpired.next();
                        expiredEvent.setTimestamp(currentTime);
                    }
                    streamEventChunk.add(eventsToBeExpired.getFirst());
                }
                if (eventsToBeExpired != null) {
                    eventsToBeExpired.clear();
                }
                if (currentEventChunk.getFirst() != null) {
                    // add reset event in front of current events
                    streamEventChunk.add(resetEvent);
                    resetEvent = null;
                    if (eventsToBeExpired != null) {
                        currentEventChunk.reset();
                        while (currentEventChunk.hasNext()) {
                            StreamEvent streamEvent = currentEventChunk.next();
                            StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(streamEvent);
                            toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                            StreamEvent eventClonedForMap = streamEventCloner.copyStreamEvent(streamEvent);
                            eventClonedForMap.setType(StreamEvent.Type.EXPIRED);
                            oldEvent = map.put(generateKey(eventClonedForMap), eventClonedForMap);
                            this.eventsToBeExpired.add(toExpireEvent);
                            eventsToBeExpired.reset();
                            while (eventsToBeExpired.hasNext()) {
                                StreamEvent expiredEvent = eventsToBeExpired.next();
                                if (oldEvent != null) {
                                    if (expiredEvent.equals(oldEvent)) {
                                        this.eventsToBeExpired.remove();
                                        currentEventChunk.insertBeforeCurrent(oldEvent);
                                        oldEvent.setTimestamp(currentTime);
                                        eventsToBeExpired.reset();
                                    }
                                }
                            }
                        }
                    }
                    resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                    resetEvent.setType(ComplexEvent.Type.RESET);
                    streamEventChunk.add(currentEventChunk.getFirst());
                }
                currentEventChunk.clear();
            }
        }
        if (streamEventChunk.getFirst() != null) {
            streamEventChunk.setBatch(true);
            nextProcessor.process(streamEventChunk);
            streamEventChunk.setBatch(false);
        }
    }

    private long getNextEmitTime(long currentTime) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeInMilliSeconds;
        return currentTime + (timeInMilliSeconds - elapsedTimeSinceLastEmit);
    }


    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        if (eventsToBeExpired != null) {
            return new Object[]{currentEventChunk.getFirst(), eventsToBeExpired.getFirst(), resetEvent};
        } else {
            return new Object[]{currentEventChunk.getFirst(), resetEvent};
        }
    }

    @Override
    public void restoreState(Object[] state) {
        if (state.length > 2) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            eventsToBeExpired.clear();
            eventsToBeExpired.add((StreamEvent) state[1]);
            map = (ConcurrentHashMap<String, StreamEvent>) state[1];
            resetEvent = (StreamEvent) state[2];
        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            resetEvent = (StreamEvent) state[1];
        }
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, map.values(), streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap) {
        if (eventsToBeExpired == null) {
            eventsToBeExpired = new ComplexEventChunk<>(false);
        }
        return OperatorParser.constructOperator(map.values(), expression, matchingMetaStateHolder,
                executionPlanContext, variableExpressionExecutors, eventTableMap);
    }

    /**
     * Used to generate key in map to get the old event for current event. It will map key which we give as unique
     * attribute with the event
     *
     * @param event the stream event that need to be processed
     */
    private String generateKey(StreamEvent event) {
        StringBuilder stringBuilder = new StringBuilder();
        for (VariableExpressionExecutor executor : variableExpressionExecutors) {
            stringBuilder.append(event.getAttribute(executor.getPosition()));
        }
        return stringBuilder.toString();
    }
}