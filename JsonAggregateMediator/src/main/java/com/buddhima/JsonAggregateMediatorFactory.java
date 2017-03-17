package com.buddhima;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.config.xml.*;
import org.apache.synapse.mediators.base.SequenceMediator;
import org.apache.synapse.mediators.builtin.DropMediator;
import org.jaxen.JaxenException;

import javax.xml.namespace.QName;
import java.util.Properties;

public class JsonAggregateMediatorFactory extends AbstractMediatorFactory {

    /** Element QName definitions **/
    protected static final QName AGGREGATE_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "jsonAggregate");
    protected static final QName CORELATE_ON_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "correlateOn");
    protected static final QName COMPLETE_CONDITION_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "completeCondition");
    protected static final QName MESSAGE_COUNT_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "messageCount");
    protected static final QName ON_COMPLETE_Q
            = new QName(XMLConfigConstants.SYNAPSE_NAMESPACE, "onComplete");

    /** Attribute QName definitions **/
    private static final QName EXPRESSION_Q
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "expression");
    private static final QName TIMEOUT_Q
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "timeout");
    private static final QName MIN_Q
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "min");
    private static final QName MAX_Q
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "max");
    private static final QName SEQUENCE_Q
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "sequence");
    private static final QName ID_Q
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "id");
    private static final QName ENCLOSING_ELEMENT_PROPERTY
            = new QName(XMLConfigConstants.NULL_NAMESPACE, "enclosingElementProperty");

    protected Mediator createSpecificMediator(OMElement elem, Properties properties) {
        JsonAggregateMediator mediator = new JsonAggregateMediator();
        processAuditStatus(mediator, elem);

        OMAttribute id = elem.getAttribute(ID_Q);
        if (id != null) {
            mediator.setId(id.getAttributeValue());
        }

        OMElement corelateOn = elem.getFirstChildWithName(CORELATE_ON_Q);
        if (corelateOn != null) {
            OMAttribute corelateExpr = corelateOn.getAttribute(EXPRESSION_Q);
            if (corelateExpr != null) {
                mediator.setCorrelateExpression(corelateExpr.getAttributeValue());
            }
        }

        OMElement completeCond = elem.getFirstChildWithName(COMPLETE_CONDITION_Q);
        if (completeCond != null) {
            OMAttribute completeTimeout = completeCond.getAttribute(TIMEOUT_Q);
            if (completeTimeout != null) {
                mediator.setCompletionTimeoutMillis(
                        Long.parseLong(completeTimeout.getAttributeValue()) * 1000);
            }

            OMElement messageCount = completeCond.getFirstChildWithName(MESSAGE_COUNT_Q);
            if (messageCount != null) {
                OMAttribute min = messageCount.getAttribute(MIN_Q);
                if (min != null) {
                    mediator.setMinMessagesToComplete(new ValueFactory().createValue("min", messageCount));
                }

                OMAttribute max = messageCount.getAttribute(MAX_Q);
                if (max != null) {
                    mediator.setMaxMessagesToComplete(new ValueFactory().createValue("max", messageCount));
                }
            }
        }

        OMElement onComplete = elem.getFirstChildWithName(ON_COMPLETE_Q);
        if (onComplete != null) {

            OMAttribute aggregateExpr = onComplete.getAttribute(EXPRESSION_Q);
            if (aggregateExpr != null) {
                mediator.setAggregationExpression(aggregateExpr.getAttributeValue());
            }

            OMAttribute enclosingElementPropertyName = onComplete.getAttribute(ENCLOSING_ELEMENT_PROPERTY);
            if (enclosingElementPropertyName != null) {
                mediator.setEnclosingElementPropertyName(enclosingElementPropertyName.getAttributeValue());
            }

            OMAttribute onCompleteSequence = onComplete.getAttribute(SEQUENCE_Q);
            if (onCompleteSequence != null) {
                mediator.setOnCompleteSequenceRef(onCompleteSequence.getAttributeValue());
            } else if (onComplete.getFirstElement() != null) {
                mediator.setOnCompleteSequence((new SequenceMediatorFactory())
                        .createAnonymousSequence(onComplete, properties));
            } else {
                SequenceMediator sequence = new SequenceMediator();
                sequence.addChild(new DropMediator());
                mediator.setOnCompleteSequence(sequence);
            }
        }

        addAllCommentChildrenToList(elem, mediator.getCommentsList());

        return mediator;
    }

    public QName getTagQName() {
        return AGGREGATE_Q;
    }
}
