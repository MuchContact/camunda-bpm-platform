/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH
 * under one or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership. Camunda licenses this file to you under the Apache License,
 * Version 2.0; you may not use this file except in compliance with the License.
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
package org.camunda.bpm.client.topic.impl;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.camunda.bpm.client.backoff.BackoffStrategy;
import org.camunda.bpm.client.exception.ExternalTaskClientException;
import org.camunda.bpm.client.impl.EngineClient;
import org.camunda.bpm.client.impl.EngineClientException;
import org.camunda.bpm.client.impl.ExternalTaskClientLogger;
import org.camunda.bpm.client.task.ExternalTask;
import org.camunda.bpm.client.task.ExternalTaskHandler;
import org.camunda.bpm.client.task.impl.ExternalTaskImpl;
import org.camunda.bpm.client.task.impl.ExternalTaskServiceImpl;
import org.camunda.bpm.client.topic.TopicSubscription;
import org.camunda.bpm.client.topic.impl.dto.TopicRequestDto;
import org.camunda.bpm.client.variable.impl.TypedValueField;
import org.camunda.bpm.client.variable.impl.TypedValues;
import org.camunda.bpm.client.variable.impl.VariableValue;

import static java.util.Arrays.asList;

/**
 * @author Tassilo Weidner
 */
public class TopicSubscriptionManager implements Runnable {

  protected static final TopicSubscriptionManagerLogger LOG = ExternalTaskClientLogger.TOPIC_SUBSCRIPTION_MANAGER_LOGGER;

  protected ReentrantLock ACQUISITION_MONITOR = new ReentrantLock(false);
  protected Condition IS_WAITING = ACQUISITION_MONITOR.newCondition();
  protected volatile AtomicBoolean isRunning = new AtomicBoolean(false);

  protected ExternalTaskServiceImpl externalTaskService;

  protected EngineClient engineClient;

  protected CopyOnWriteArrayList<TopicSubscription> subscriptions;
  protected List<TopicRequestDto> taskTopicRequests;
  protected Map<String, ExternalTaskHandler> externalTaskHandlers;

  protected Thread thread;
  protected Map<String, Thread> consumerThreads = new ConcurrentHashMap<>();

  protected BackoffStrategy backoffStrategy;
  protected AtomicBoolean isBackoffStrategyDisabled;

  protected TypedValues typedValues;

  protected long clientLockDuration;

  public TopicSubscriptionManager(EngineClient engineClient, TypedValues typedValues, long clientLockDuration) {
    this.engineClient = engineClient;
    this.subscriptions = new CopyOnWriteArrayList<>();
    this.taskTopicRequests = new ArrayList<>();
    this.externalTaskHandlers = new HashMap<>();
    this.clientLockDuration = clientLockDuration;
    this.typedValues = typedValues;
    this.externalTaskService = new ExternalTaskServiceImpl(engineClient);
    this.isBackoffStrategyDisabled = new AtomicBoolean(false);
  }

  protected void startNewThread(TopicSubscription subscription) {
      String topicName = subscription.getTopicName();
      consumerThreads.computeIfAbsent(topicName, (k) -> {
          ConsumerRunner consumerRunner = new ConsumerRunner(k);
          Thread thread1 = new Thread(consumerRunner);
          thread1.start();
          return thread1;
      });

  }

    public void run() {
        subscriptions.forEach(this::prepareAcquisition);
        subscriptions.forEach(this::startNewThread);
    }

  protected void acquire() {
    taskTopicRequests.clear();
    externalTaskHandlers.clear();
    subscriptions.forEach(this::prepareAcquisition);

    if (!taskTopicRequests.isEmpty()) {
      List<ExternalTask> externalTasks = fetchAndLock(taskTopicRequests);

      externalTasks.forEach(externalTask -> {
        String topicName = externalTask.getTopicName();
        ExternalTaskHandler taskHandler = externalTaskHandlers.get(topicName);

        if (taskHandler != null) {
          handleExternalTask(externalTask, taskHandler);
        }
        else {
          LOG.taskHandlerIsNull(topicName);
        }
      });

      if (!isBackoffStrategyDisabled.get()) {
        runBackoffStrategy(externalTasks);
      }
    }
  }

  protected void acquire(String topic) {
    Optional<TopicSubscription> first = subscriptions.stream().filter(it -> it.getTopicName().equals(topic)).findFirst();
    if (first.isPresent()) {
        TopicSubscription topicSubscription = first.get();
        TopicRequestDto taskTopicRequest = TopicRequestDto.fromTopicSubscription(topicSubscription, clientLockDuration);
        List<ExternalTask> externalTasks = fetchAndLock(asList(taskTopicRequest));

        externalTasks.forEach(externalTask -> {
            String topicName = externalTask.getTopicName();
            ExternalTaskHandler taskHandler = externalTaskHandlers.get(topicName);

            if (taskHandler != null) {
                handleExternalTask(externalTask, taskHandler);
            } else {
                LOG.taskHandlerIsNull(topicName);
            }
        });

        if (!isBackoffStrategyDisabled.get()) {
            runBackoffStrategy(externalTasks);
        }
    }
  }
  protected void prepareAcquisition(TopicSubscription subscription) {
//    TopicRequestDto taskTopicRequest = TopicRequestDto.fromTopicSubscription(subscription, clientLockDuration);
//    taskTopicRequests.add(taskTopicRequest);

    String topicName = subscription.getTopicName();
    ExternalTaskHandler externalTaskHandler = subscription.getExternalTaskHandler();
    externalTaskHandlers.put(topicName, externalTaskHandler);
  }

  protected List<ExternalTask> fetchAndLock(List<TopicRequestDto> subscriptions) {
    List<ExternalTask> externalTasks = Collections.emptyList();

    try {
      LOG.fetchAndLock(subscriptions);
      externalTasks = engineClient.fetchAndLock(subscriptions);
    } catch (EngineClientException e) {
      LOG.exceptionWhilePerformingFetchAndLock(e);
    }

    return externalTasks;
  }

  @SuppressWarnings("rawtypes")
  protected void handleExternalTask(ExternalTask externalTask, ExternalTaskHandler taskHandler) {
    ExternalTaskImpl task = (ExternalTaskImpl) externalTask;

    Map<String, TypedValueField> variables = task.getVariables();
    Map<String, VariableValue> wrappedVariables = typedValues.wrapVariables(task, variables);
    task.setReceivedVariableMap(wrappedVariables);

    try {
      taskHandler.execute(task, externalTaskService);
    } catch (ExternalTaskClientException e) {
      LOG.exceptionOnExternalTaskServiceMethodInvocation(task.getTopicName(), e);
    } catch (Throwable e) {
      LOG.exceptionWhileExecutingExternalTaskHandler(task.getTopicName(), e);
    }
  }

  public synchronized void stop() {
    if (isRunning.compareAndSet(true, false)) {
      resume();

      try {
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.exceptionWhileShuttingDown(e);
      }
    }
  }

  public synchronized void start() {
    if (isRunning.compareAndSet(false, true)) {
      thread = new Thread(this, TopicSubscriptionManager.class.getSimpleName());
      thread.start();
    }
  }

  protected void subscribe(TopicSubscription subscription) {
      if (!subscriptions.addIfAbsent(subscription)) {
          String topicName = subscription.getTopicName();
          throw LOG.topicNameAlreadySubscribedException(topicName);
      }
      prepareAcquisition(subscription);
      subscriptions.forEach(this::startNewThread);

      resume();
  }

  protected void unsubscribe(TopicSubscriptionImpl subscription) {
    subscriptions.remove(subscription);

  }

  public EngineClient getEngineClient() {
    return engineClient;
  }

  public List<TopicSubscription> getSubscriptions() {
    return subscriptions;
  }

  public boolean isRunning() {
    return isRunning.get();
  }

  public void setBackoffStrategy(BackoffStrategy backOffStrategy) {
    this.backoffStrategy = backOffStrategy;
  }

  protected void runBackoffStrategy(List<ExternalTask> externalTasks) {
    try {
      backoffStrategy.reconfigure(externalTasks);
      long waitTime = backoffStrategy.calculateBackoffTime();
      suspend(waitTime);
    } catch (Throwable e) {
      LOG.exceptionWhileExecutingBackoffStrategyMethod(e);
    }
  }

  protected void suspend(long waitTime) {
    if (waitTime > 0 && isRunning.get()) {
      ACQUISITION_MONITOR.lock();
      try {
        if (isRunning.get()) {
          IS_WAITING.await(waitTime, TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException e) {
        LOG.exceptionWhileExecutingBackoffStrategyMethod(e);
      }
      finally {
        ACQUISITION_MONITOR.unlock();
      }
    }
  }

  protected void resume() {
    ACQUISITION_MONITOR.lock();
    try {
      IS_WAITING.signal();
    }
    finally {
      ACQUISITION_MONITOR.unlock();
    }
  }

  public void disableBackoffStrategy() {
    this.isBackoffStrategyDisabled.set(true);
  }

  private  class ConsumerRunner implements Runnable{
      private String topic;

      public ConsumerRunner(String topic) {
          this.topic = topic;
      }

      @Override
      public void run() {
          try {
              while (isRunning.get()) {
                  try {
                      acquire(topic);
                  } catch (Throwable e) {
                      LOG.exceptionWhileAcquiringTasks(e);
                  }
              }
          } finally {
              consumerThreads.remove(topic);
          }

      }
  }
}
