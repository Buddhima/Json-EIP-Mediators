package com.buddhima;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.axiom.om.OMElement;
import org.apache.axis2.AxisFault;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.synapse.*;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.Value;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.eip.EIPConstants;
import org.apache.synapse.mediators.eip.EIPUtils;
import org.apache.synapse.mediators.eip.SharedDataHolder;
import org.apache.synapse.util.MessageHelper;
import org.jaxen.JaxenException;

import java.util.*;

public class JsonAggregateMediator extends AbstractMediator implements ManagedLifecycle, FlowContinuableMediator {

	private static final Log log = LogFactory.getLog(JsonAggregateMediator.class);

	/** The duration as a number of milliseconds for this aggregation to complete */
	private long completionTimeoutMillis = 0;
	/** The maximum number of messages required to complete aggregation */
	private Value minMessagesToComplete;
	/** The minimum number of messages required to complete aggregation */
	private Value maxMessagesToComplete;

	/**
	 * JSONPath that specifies a correlation expression that can be used to combine messages. An
	 * example maybe //department@id="11"
	 */
	private String correlateExpression = null;
	/**
	 * An JSONPath expression that may specify a selected element to be aggregated from a group of
	 * messages to create the aggregated message
	 * e.g. //getQuote/return would pick up and aggregate the //getQuote/return elements from a
	 * bunch of matching messages into one aggregated message
	 */
	private String aggregationExpression = "$";

	/** This holds the reference sequence name of the */
	private String onCompleteSequenceRef = null;
	/** Inline sequence definition holder that holds the onComplete sequence */
	private SequenceMediator onCompleteSequence = null;

	/** The active aggregates currently being processd */
	private Map<String, JsonAggregate> activeAggregates =
			Collections.synchronizedMap(new HashMap<String, JsonAggregate>());

	private String id = null;

	/** Property which contains the Enclosing element of the aggregated message */
	private String enclosingElementPropertyName = null;

	/** Lock object to provide the synchronized access to the activeAggregates on checking */
	private final Object lock = new Object();

	/** Reference to the synapse environment */
	private SynapseEnvironment synapseEnv;

	private Configuration configuration = Configuration.defaultConfiguration();

	public boolean mediate(MessageContext synapseContext) {

		SynapseLog synLog = getLog(synapseContext);

		if (synLog.isTraceOrDebugEnabled())
			synLog.traceOrDebug("Start : Json Aggregate mediator");

		try {

			org.apache.axis2.context.MessageContext context = ((Axis2MessageContext) synapseContext).getAxis2MessageContext();

			String jsonPayload = JsonUtil.jsonPayloadToString(context);

			if (synLog.isTraceTraceEnabled())
				synLog.traceTrace("Message : " + jsonPayload);

			JsonAggregate jsonAggregate = null;

			String correlationIdName = (id != null ? EIPConstants.AGGREGATE_CORRELATION + "." + id :
					EIPConstants.AGGREGATE_CORRELATION);

			// if a correlateExpression is provided and there is a coresponding
			// element in the current message prepare to correlate the messages on that
			Object result = null;
			if (correlateExpression != null) {

				try {
					result = JsonPath.using(configuration).parse(jsonPayload).read(correlateExpression);
				} catch (Exception ex) {
					// ignore
				}

				if (result instanceof List) {
					if (((List) result).isEmpty()) {
						handleException("Failed to evaluate correlate expression: " + correlateExpression, synapseContext);
					}
				}
			}
			if (result != null) {

				while (jsonAggregate == null) {

					synchronized (lock) {

						if (activeAggregates.containsKey(correlateExpression)) {

							jsonAggregate = activeAggregates.get(correlateExpression);
							if (jsonAggregate != null) {
								if (!jsonAggregate.getLock()) {
									jsonAggregate = null;
								}
							}

						} else {

							if (synLog.isTraceOrDebugEnabled()) {
								synLog.traceOrDebug("Creating new Json Aggregator - " +
										(completionTimeoutMillis > 0 ? "expires in : "
												+ (completionTimeoutMillis / 1000) + "secs" :
												"without expiry time"));
							}
							if (isAlreadyTimedOut(synapseContext)) {
								return false;
							}

							Double minMsg = -1.0;
							if (minMessagesToComplete != null) {
								minMsg = Double.parseDouble(minMessagesToComplete.evaluateValue(synapseContext));
							}
							Double maxMsg = -1.0;
							if (maxMessagesToComplete != null) {
								maxMsg = Double.parseDouble(maxMessagesToComplete.evaluateValue(synapseContext));
							}

							jsonAggregate = new JsonAggregate(
									synapseContext.getEnvironment(),
									correlateExpression,
									completionTimeoutMillis,
									minMsg.intValue(),
									maxMsg.intValue(), this);

							if (completionTimeoutMillis > 0) {
								synapseContext.getConfiguration().getSynapseTimer().
										schedule(jsonAggregate, completionTimeoutMillis);
							}
							jsonAggregate.getLock();
							activeAggregates.put(correlateExpression, jsonAggregate);
						}
					}
				}

			} else if (synapseContext.getProperty(correlationIdName) != null) {
				// if the correlattion cannot be found using the correlateExpression then
				// try the default which is through the AGGREGATE_CORRELATION message property
				// which is the unique original message id of a split or iterate operation and
				// which thus can be used to uniquely group messages into aggregates

				Object o = synapseContext.getProperty(correlationIdName);
				String correlation;

				if (o != null && o instanceof String) {
					correlation = (String) o;
					while (jsonAggregate == null) {
						synchronized (lock) {
							if (activeAggregates.containsKey(correlation)) {
								jsonAggregate = activeAggregates.get(correlation);
								if (jsonAggregate != null) {
									if (!jsonAggregate.getLock()) {
										jsonAggregate = null;
									}
								} else {
									break;
								}
							} else {
								if (synLog.isTraceOrDebugEnabled()) {
									synLog.traceOrDebug("Creating new Json Aggregator - " +
											(completionTimeoutMillis > 0 ? "expires in : "
													+ (completionTimeoutMillis / 1000) + "secs" :
													"without expiry time"));
								}

								if (isAlreadyTimedOut(synapseContext)) {
									return false;
								}

								Double minMsg = -1.0;
								if (minMessagesToComplete != null) {
									minMsg = Double.parseDouble(minMessagesToComplete.evaluateValue(synapseContext));
								}
								Double maxMsg = -1.0;
								if (maxMessagesToComplete != null) {
									maxMsg = Double.parseDouble(maxMessagesToComplete.evaluateValue(synapseContext));
								}

								jsonAggregate = new JsonAggregate(
										synapseContext.getEnvironment(),
										correlation,
										completionTimeoutMillis,
										minMsg.intValue(),
										maxMsg.intValue(), this);

								if (completionTimeoutMillis > 0) {
									synchronized(jsonAggregate) {
										if (!jsonAggregate.isCompleted()) {
											synapseContext.getConfiguration().getSynapseTimer().
													schedule(jsonAggregate, completionTimeoutMillis);
										}
									}
								}
								jsonAggregate.getLock();
								activeAggregates.put(correlation, jsonAggregate);
							}
						}
					}

				} else {
					synLog.traceOrDebug("Unable to find aggrgation correlation property");
					return true;
				}
			} else {
				synLog.traceOrDebug("Unable to find aggrgation correlation JSONPath or property");
				return true;
			}

			// if there is an aggregate continue on aggregation
			if (jsonAggregate != null) {
				//this is a temporary fix
				synapseContext.getEnvelope().build();
				boolean collected = jsonAggregate.addMessage(synapseContext);
				if (synLog.isTraceOrDebugEnabled() && collected) {
					synLog.traceOrDebug("Collected a message during aggregation");
					synLog.traceTrace("Collected message : " + synapseContext);
				}

				// check the completeness of the aggregate and if completed aggregate the messages
				// if not completed return false and block the message sequence till it completes

				if (jsonAggregate.isComplete(synLog)) {
					if (synLog.isTraceOrDebugEnabled())
						synLog.traceOrDebug("Json Aggregation completed - invoking onComplete");

					boolean onCompleteSeqResult = completeAggregate(jsonAggregate);

					if (synLog.isTraceOrDebugEnabled())
						synLog.traceOrDebug("End : Json Aggregate mediator");

					return onCompleteSeqResult;
				} else {
					jsonAggregate.releaseLock();
				}

			} else {
				// if the aggregation correlation cannot be found then continue the message on the
				// normal path by returning true

				if (synLog.isTraceOrDebugEnabled())
					synLog.traceOrDebug("Unable to find an aggregate for this message - skip");
				return true;
			}

		} catch (Exception e) {
			handleException("Unable to execute the JSONPath over the message", e, synapseContext);
		}

		if (synLog.isTraceOrDebugEnabled())
			synLog.traceOrDebug("End : Json Aggregate mediator");

		// When Aggregation is not completed return false to hold the flow
		return false;
	}

	public boolean mediate(MessageContext messageContext, ContinuationState contState) {
		SynapseLog synLog = getLog(messageContext);

		if (synLog.isTraceOrDebugEnabled()) {
			synLog.traceOrDebug("Json Aggregate mediator : Mediating from ContinuationState");
		}

		boolean result;

		SequenceMediator onCompleteSequence = getOnCompleteSequence();
		if (!contState.hasChild()) {
			result = onCompleteSequence.mediate(messageContext, contState.getPosition() + 1);
		} else {
			FlowContinuableMediator mediator =
					(FlowContinuableMediator) onCompleteSequence.getChild(contState.getPosition());

			result = mediator.mediate(messageContext, contState.getChildContState());

		}
		return result;
	}

	/**
	 * Invoked by the Aggregate objects that are timed out, to signal timeout/completion of
	 * itself
	 * @param aggregate the timed out Aggregate that holds collected messages and properties
	 */
	public boolean completeAggregate(JsonAggregate aggregate) {

		boolean markedCompletedNow = false;
		boolean wasComplete = aggregate.isCompleted();
		if (wasComplete) {
			return false;
		}

		if (log.isDebugEnabled()) {
			log.debug("Aggregation completed or timed out");
		}

		// cancel the timer
		synchronized(this) {
			if (!aggregate.isCompleted()) {
				aggregate.cancel();
				aggregate.setCompleted(true);

				MessageContext lastMessage = aggregate.getLastMessage();
				if (lastMessage != null) {
					Object aggregateTimeoutHolderObj =
							lastMessage.getProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
									EIPConstants.EIP_SHARED_DATA_HOLDER);

					if (aggregateTimeoutHolderObj != null) {
						SharedDataHolder sharedDataHolder = (SharedDataHolder) aggregateTimeoutHolderObj;
						sharedDataHolder.markTimeoutState();
					}
				}
				markedCompletedNow = true;
			}
		}

		if (!markedCompletedNow) {
			return false;
		}

		MessageContext newSynCtx = getAggregatedMessage(aggregate);

		if (newSynCtx == null) {
			log.warn("An aggregation of messages timed out with no aggregated messages", null);
			return false;
		} else {
			// Get the aggregated message to the next mediator placed after the aggregate mediator
			// in the sequence
			if (newSynCtx.isContinuationEnabled()) {
				try {
					aggregate.getLastMessage().setEnvelope(
							MessageHelper.cloneSOAPEnvelope(newSynCtx.getEnvelope()));
				} catch (AxisFault axisFault) {
					log.warn("Error occurred while assigning aggregated message" +
							" back to the last received message context");
				}
			}
		}

		aggregate.clear();
		activeAggregates.remove(aggregate.getCorrelation());

		if ((correlateExpression != null &&
				!correlateExpression.equals(aggregate.getCorrelation())) ||
				correlateExpression == null) {

			if (onCompleteSequence != null) {

				ContinuationStackManager.
						addReliantContinuationState(newSynCtx, 0, getMediatorPosition());
				boolean result = onCompleteSequence.mediate(newSynCtx);
				if (result) {
					ContinuationStackManager.removeReliantContinuationState(newSynCtx);
				}
				return result;

			} else if (onCompleteSequenceRef != null
					&& newSynCtx.getSequence(onCompleteSequenceRef) != null) {

				ContinuationStackManager.updateSeqContinuationState(newSynCtx, getMediatorPosition());
				return newSynCtx.getSequence(onCompleteSequenceRef).mediate(newSynCtx);

			} else {
				handleException("Unable to find the sequence for the mediation " +
						"of the aggregated message", newSynCtx);
			}
		}
		return false;
	}

	/*
	 * Check whether aggregate is already timed-out and we are receiving a message after the timeout interval
	 */
	private boolean isAlreadyTimedOut(MessageContext synCtx) {

		Object aggregateTimeoutHolderObj =
				synCtx.getProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
						EIPConstants.EIP_SHARED_DATA_HOLDER);

		if (aggregateTimeoutHolderObj != null) {
			SharedDataHolder sharedDataHolder = (SharedDataHolder) aggregateTimeoutHolderObj;
			if (sharedDataHolder.isTimeoutOccurred()) {
				if (log.isDebugEnabled()) {
					log.debug("Received a response for already timed-out Aggregate");
				}
				return true;
			}
		}
		return false;
	}

	/**
	 * Get the aggregated message from the specified Aggregate instance
	 *
	 * @param aggregate the Aggregate object that holds collected messages and properties of the
	 * aggregation
	 * @return the aggregated message context
	 */
	private MessageContext getAggregatedMessage(JsonAggregate aggregate) {

		MessageContext newCtx = null;
		List<Object> objectList = new ArrayList<Object>();

		for (MessageContext synCtx : aggregate.getMessages()) {

			if (newCtx == null) {
				try {
					newCtx = MessageHelper.cloneMessageContextForAggregateMediator(synCtx);
				} catch (AxisFault axisFault) {
					handleException("Error creating a copy of the message", axisFault, synCtx);
				}

				if (log.isDebugEnabled()) {
					log.debug("Generating Aggregated message from : " + newCtx.getEnvelope());
				}
			}

			org.apache.axis2.context.MessageContext ctx = ((Axis2MessageContext) synCtx).getAxis2MessageContext();

			String jsonPayload = JsonUtil.jsonPayloadToString(ctx);

			if (log.isDebugEnabled()) {
				log.debug("Generating Aggregated message from : " + jsonPayload);
			}

			try {

				if (log.isDebugEnabled()) {
					log.debug("Merging message : " + jsonPayload + " using JSONPath : " + aggregationExpression);
				}

				Object extractedJsonObject = JsonPath.using(configuration).parse(jsonPayload).read(aggregationExpression);

				if (extractedJsonObject != null) {

					if (log.isDebugEnabled()) {
						log.debug("Extracted result : " + extractedJsonObject.toString());
					}

					objectList.add(extractedJsonObject);
				}
			} catch (Exception ex) {
				handleException("Error evaluating expression: " + aggregationExpression + " from payload: " + jsonPayload, ex, synCtx);
			}

		}

		if (enclosingElementPropertyName != null) {

			String aggregatedJsonString = JsonPath.
					using(configuration).
					parse("{}").put("$", enclosingElementPropertyName, objectList).
					jsonString();

			org.apache.axis2.context.MessageContext newAxisCtx = ((Axis2MessageContext) newCtx).getAxis2MessageContext();

			try {
				JsonUtil.getNewJsonPayload(newAxisCtx, aggregatedJsonString, true, true);
			} catch (AxisFault fault) {
				handleException("Error while adding new JSON payload", fault, newCtx);
			}
		} else {
			handleException("enclosingElementProperty is required for JSON aggregate", newCtx);
		}

		return newCtx;
	}

	public void init(SynapseEnvironment se) {
		synapseEnv = se;
		if (onCompleteSequence != null) {
			onCompleteSequence.init(se);
		} else if (onCompleteSequenceRef != null) {
			SequenceMediator referredOnCompleteSeq =
					(SequenceMediator) se.getSynapseConfiguration().
							getSequence(onCompleteSequenceRef);

			if (referredOnCompleteSeq == null || referredOnCompleteSeq.isDynamic()) {
				se.addUnavailableArtifactRef(onCompleteSequenceRef);
			}
		}
	}

	public void destroy() {
		if (onCompleteSequence != null) {
			onCompleteSequence.destroy();
		} else if (onCompleteSequenceRef != null) {
			SequenceMediator referredOnCompleteSeq =
					(SequenceMediator) synapseEnv.getSynapseConfiguration().
							getSequence(onCompleteSequenceRef);

			if (referredOnCompleteSeq == null || referredOnCompleteSeq.isDynamic()) {
				synapseEnv.removeUnavailableArtifactRef(onCompleteSequenceRef);
			}
		}
	}












	public String getCorrelateExpression() {
		return correlateExpression;
	}

	public void setCorrelateExpression(String correlateExpression) {
		this.correlateExpression = correlateExpression;
	}

	public long getCompletionTimeoutMillis() {
		return completionTimeoutMillis;
	}

	public void setCompletionTimeoutMillis(long completionTimeoutMillis) {
		this.completionTimeoutMillis = completionTimeoutMillis;
	}

	public String getAggregationExpression() {
		return aggregationExpression;
	}

	public void setAggregationExpression(String aggregationExpression) {
		this.aggregationExpression = aggregationExpression;
	}

	public String getOnCompleteSequenceRef() {
		return onCompleteSequenceRef;
	}

	public void setOnCompleteSequenceRef(String onCompleteSequenceRef) {
		this.onCompleteSequenceRef = onCompleteSequenceRef;
	}

	public SequenceMediator getOnCompleteSequence() {
		return onCompleteSequence;
	}

	public void setOnCompleteSequence(SequenceMediator onCompleteSequence) {
		this.onCompleteSequence = onCompleteSequence;
	}

	public Map getActiveAggregates() {
		return activeAggregates;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Value getMinMessagesToComplete() {
		return minMessagesToComplete;
	}

	public void setMinMessagesToComplete(Value minMessagesToComplete) {
		this.minMessagesToComplete = minMessagesToComplete;
	}

	public Value getMaxMessagesToComplete() {
		return maxMessagesToComplete;
	}

	public void setMaxMessagesToComplete(Value maxMessagesToComplete) {
		this.maxMessagesToComplete = maxMessagesToComplete;
	}

	public String getEnclosingElementPropertyName() {
		return enclosingElementPropertyName;
	}

	public void setEnclosingElementPropertyName(String enclosingElementPropertyName) {
		this.enclosingElementPropertyName = enclosingElementPropertyName;
	}
}
