package com.buddhima;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import org.apache.axis2.AxisFault;
import org.apache.axis2.Constants;
import org.apache.axis2.context.OperationContext;
import org.apache.synapse.*;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.continuation.ContinuationStackManager;
import org.apache.synapse.core.SynapseEnvironment;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.endpoints.Endpoint;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.mediators.FlowContinuableMediator;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.eip.EIPConstants;
import org.apache.synapse.mediators.eip.SharedDataHolder;
import org.apache.synapse.mediators.eip.Target;
import org.apache.synapse.util.MessageHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class JsonIterateMediator extends AbstractMediator implements ManagedLifecycle, FlowContinuableMediator {

	private boolean continueParent = false;
	private boolean preservePayload = false;
	private String expression = null;
	private String attachPath = null;
	private String id = null;

	private Target target = null;

	private SynapseEnvironment synapseEnv;
	private Configuration configuration = Configuration.defaultConfiguration();


	public boolean mediate(MessageContext synapseContext) {

		SynapseLog synapseLog = getLog(synapseContext);

		if (synapseLog.isTraceOrDebugEnabled()) {
			synapseLog.traceOrDebug("Start : Json Iterate mediator");
		}

		try {

			org.apache.axis2.context.MessageContext context = ((Axis2MessageContext) synapseContext).getAxis2MessageContext();

			String jsonPayload = JsonUtil.jsonPayloadToString(context);

			if (synapseLog.isTraceTraceEnabled()) {
				synapseLog.traceTrace("Message : " + jsonPayload);
			}

			synapseContext.setProperty(id != null ? EIPConstants.EIP_SHARED_DATA_HOLDER + "." + id :
					EIPConstants.EIP_SHARED_DATA_HOLDER, new SharedDataHolder());

			List splitElements = JsonIterateUtils.getDetachedMatchingElements(jsonPayload, expression, configuration);

			if (synapseLog.isTraceOrDebugEnabled()) {
				synapseLog.traceOrDebug("Splitting with JSONPath : " + expression + " resulted in " +
						splitElements.size() + " elements");
			}

			if (!preservePayload && JsonUtil.hasAJsonPayload(context)) {
				JsonUtil.removeJsonPayload(context);
			}

			int msgCount = splitElements.size();
			int msgNumber = 0;

			for (Object o : splitElements) {

				if (o == null) {
					handleException("Error splitting message with JSONPath : "
							+ expression + " - result not an Object", synapseContext);
				}

				if (synapseLog.isTraceOrDebugEnabled()) {
					synapseLog.traceOrDebug(
							"Submitting " + (msgNumber + 1) + " of " + msgCount +
									(target.isAsynchronous() ? " messages for processing in parallel" :
											" messages for processing in sequentially"));
				}

				MessageContext iteratedMsgCtx =
						getIteratedMessage(synapseContext, msgNumber++, msgCount, jsonPayload, o);
				ContinuationStackManager.
						addReliantContinuationState(iteratedMsgCtx, 0, getMediatorPosition());

				if (target.isAsynchronous()) {
					target.mediate(iteratedMsgCtx);
				} else {
					try {
						target.mediate(iteratedMsgCtx);
					} catch (SynapseException synEx) {
						copyFaultyIteratedMessage(synapseContext, iteratedMsgCtx);
						throw synEx;
					} catch (Exception e) {
						copyFaultyIteratedMessage(synapseContext, iteratedMsgCtx);
						handleException("Exception occurred while executing sequential iteration " +
								"in the Json Iterator Mediator", e, synapseContext);
					}
				}
			}
		} catch (AxisFault af) {
			handleException("Error creating an iterated copy of the message", af, synapseContext);
		} catch (SynapseException synEx) {
			throw synEx;
		} catch (Exception e) {
			handleException("Exception occurred while executing the Json Iterate Mediator", e, synapseContext);
		}

		OperationContext opCtx
				= ((Axis2MessageContext) synapseContext).getAxis2MessageContext().getOperationContext();
		if (!continueParent && opCtx != null) {
			opCtx.setProperty(Constants.RESPONSE_WRITTEN,"SKIP");
		}

		if (synapseLog.isTraceOrDebugEnabled()) {
			synapseLog.traceOrDebug("End : Json Iterate mediator");
		}

		// whether to continue mediation on the original message
		return continueParent;
	}

	private void copyFaultyIteratedMessage(MessageContext synapseContext, MessageContext iteratedMsgCtx) {
		synapseContext.getFaultStack().clear(); //remove original fault stack
		Stack<FaultHandler> faultStack = iteratedMsgCtx.getFaultStack();

		if (!faultStack.isEmpty()) {
			List<FaultHandler> newFaultStack = new ArrayList<FaultHandler>();
			newFaultStack.addAll(faultStack);
			for (FaultHandler faultHandler : newFaultStack) {
				if (faultHandler != null) {
					synapseContext.pushFaultHandler(faultHandler);
				}
			}
		}
		// copy all the String keyed synapse level properties to the Original synCtx
		for (Object keyObject : iteratedMsgCtx.getPropertyKeySet()) {
            /*
             * There can be properties added while executing the iterated sequential flow and
             * these may be accessed in the fault sequence, so updating string valued properties
             */
			if (keyObject instanceof String) {
				String stringKey = (String) keyObject;
				synapseContext.setProperty(stringKey, iteratedMsgCtx.getProperty(stringKey));
			}
		}
	}

	public boolean mediate(MessageContext synapseContext, ContinuationState continuationState) {
		SynapseLog synapseLog = getLog(synapseContext);

		if (synapseLog.isTraceOrDebugEnabled()) {
			synapseLog.traceOrDebug("Json Iterate mediator : Mediating from ContinuationState");
		}

		boolean result;
		SequenceMediator branchSequence = target.getSequence();

		if (!continuationState.hasChild()) {
			result = branchSequence.mediate(synapseContext, continuationState.getPosition() + 1);
		} else {
			FlowContinuableMediator mediator =
					(FlowContinuableMediator) branchSequence.getChild(continuationState.getPosition());

			result = mediator.mediate(synapseContext, continuationState.getChildContState());

		}

		return result;
	}

	private MessageContext getIteratedMessage(MessageContext synapseContext, int msgNumber, int msgCount, String jsonPayload, Object o) throws AxisFault {

		MessageContext newSynapseContext = MessageHelper.cloneMessageContext(synapseContext, false);

		// get a clone of the envelope to be attached
		newSynapseContext.setEnvelope(MessageHelper.cloneSOAPEnvelope(synapseContext.getEnvelope()));

		org.apache.axis2.context.MessageContext newContext = ((Axis2MessageContext) newSynapseContext).getAxis2MessageContext();

		if (id != null) {
			// set the parent correlation details to the cloned MC - for the use of aggregation like tasks
			newSynapseContext.setProperty(EIPConstants.AGGREGATE_CORRELATION + "." + id, synapseContext.getMessageID());

			// set the messageSequence property for possible aggregations
			newSynapseContext.setProperty(
					EIPConstants.MESSAGE_SEQUENCE + "." + id,
					msgNumber + EIPConstants.MESSAGE_SEQUENCE_DELEMITER + msgCount);
		} else {
			newSynapseContext.setProperty(
					EIPConstants.MESSAGE_SEQUENCE,
					msgNumber + EIPConstants.MESSAGE_SEQUENCE_DELEMITER + msgCount);
		}

		String newJsonPayload = "";

		// if payload should be preserved then attach the iteration element to the
		// node specified by the attachPath
		if (preservePayload) {

			if (attachPath.equalsIgnoreCase("$")) {
				newJsonPayload = JsonPath.using(configuration).parse(o).jsonString();
			} else {
				newJsonPayload = JsonPath.using(configuration).parse(jsonPayload).set(attachPath, o).jsonString();
			}

			if (newJsonPayload == null || newJsonPayload.isEmpty()) {
				handleException("Error in attaching the splitted elements :: " +
						"Unable to get the attach path specified by the expression " +
						attachPath, newSynapseContext);
			}

		} else if (o != null) {
			// if not preserve payload then attach the iteration element as the payload
			newJsonPayload = JsonPath.using(configuration).parse(o).jsonString();
		}

		// set the envelope and mediate as specified in the target
		JsonUtil.getNewJsonPayload(newContext, newJsonPayload, true, true);

		return newSynapseContext;

	}



	public void init(SynapseEnvironment se) {
		synapseEnv = se;
		if (target != null) {
			Endpoint endpoint = target.getEndpoint();
			if (endpoint != null) {
				endpoint.init(se);
			}

			ManagedLifecycle seq = target.getSequence();
			if (seq != null) {
				seq.init(se);
			} else if (target.getSequenceRef() != null) {
				SequenceMediator targetSequence =
						(SequenceMediator) se.getSynapseConfiguration().
								getSequence(target.getSequenceRef());

				if (targetSequence == null || targetSequence.isDynamic()) {
					se.addUnavailableArtifactRef(target.getSequenceRef());
				}
			}
		}
	}

	public void destroy() {
		if (target != null) {
			Endpoint endpoint = target.getEndpoint();
			if (endpoint != null && endpoint.isInitialized()) {
				endpoint.destroy();
			}

			ManagedLifecycle seq = target.getSequence();
			if (seq != null) {
				seq.destroy();
			} else if (target.getSequenceRef() != null) {
				SequenceMediator targetSequence =
						(SequenceMediator) synapseEnv.getSynapseConfiguration().
								getSequence(target.getSequenceRef());

				if (targetSequence == null || targetSequence.isDynamic()) {
					synapseEnv.removeUnavailableArtifactRef(target.getSequenceRef());
				}
			}
		}
	}

	public boolean isContinueParent() {
		return continueParent;
	}

	public void setContinueParent(boolean continueParent) {
		this.continueParent = continueParent;
	}

	public boolean isPreservePayload() {
		return preservePayload;
	}

	public void setPreservePayload(boolean preservePayload) {
		this.preservePayload = preservePayload;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getAttachPath() {
		return attachPath;
	}

	public void setAttachPath(String attachPath) {
		this.attachPath = attachPath;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Target getTarget() {
		return target;
	}

	public void setTarget(Target target) {
		this.target = target;
	}
}
