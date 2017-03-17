package com.buddhima;

import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.config.xml.*;

public class JsonAggregateMediatorSerializer extends AbstractListMediatorSerializer {
    protected OMElement serializeSpecificMediator(Mediator m) {
        JsonAggregateMediator mediator = null;
        if (!(m instanceof JsonAggregateMediator)) {
            handleException("Unsupported mediator passed in for serialization : " + m.getType());
        } else {
            mediator = (JsonAggregateMediator) m;
        }

        assert mediator != null;
        OMElement aggregator = fac.createOMElement("jsonAggregate", synNS);
        saveTracingState(aggregator, mediator);

        if (mediator.getId() != null) {
            aggregator.addAttribute("id", mediator.getId(), nullNS);
        }

        if (mediator.getCorrelateExpression() != null) {
            OMElement corelateOn = fac.createOMElement("correlateOn", synNS);
            corelateOn.addAttribute("expression", mediator.getCorrelateExpression(), nullNS);
            aggregator.addChild(corelateOn);
        }

        OMElement completeCond = fac.createOMElement("completeCondition", synNS);
        if (mediator.getCompletionTimeoutMillis() != 0) {
            completeCond.addAttribute("timeout",
                    Long.toString(mediator.getCompletionTimeoutMillis() / 1000), nullNS);
        }
        OMElement messageCount = fac.createOMElement("messageCount", synNS);
        if (mediator.getMinMessagesToComplete() != null) {
            new ValueSerializer().serializeValue(
                    mediator.getMinMessagesToComplete(), "min", messageCount);
        }
        if (mediator.getMaxMessagesToComplete() != null) {
            new ValueSerializer().serializeValue(
                    mediator.getMaxMessagesToComplete(), "max", messageCount);
        }
        completeCond.addChild(messageCount);
        aggregator.addChild(completeCond);

        OMElement onCompleteElem = fac.createOMElement("onComplete", synNS);
        if (mediator.getAggregationExpression() != null) {
            onCompleteElem.addAttribute("expression", mediator.getAggregationExpression(), nullNS);
        }
        if (mediator.getOnCompleteSequenceRef() != null) {
            onCompleteElem.addAttribute("sequence", mediator.getOnCompleteSequenceRef(), nullNS);
        } else if (mediator.getOnCompleteSequence() != null) {
            serializeChildren(onCompleteElem, mediator.getOnCompleteSequence().getList());
        }

        String enclosingElementPropertyName = mediator.getEnclosingElementPropertyName();
        if (enclosingElementPropertyName != null) {
            onCompleteElem.addAttribute("enclosingElementProperty", enclosingElementPropertyName,nullNS);
        }
        aggregator.addChild(onCompleteElem);

        serializeComments(aggregator, mediator.getCommentsList());

        return aggregator;
    }

    public String getMediatorClassName() {
        return JsonAggregateMediator.class.getName();
    }
}
