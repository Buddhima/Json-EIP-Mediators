package com.buddhima;

import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.config.xml.AbstractMediatorSerializer;
import org.apache.synapse.config.xml.TargetSerializer;

public class JsonIterateMediatorSerializer extends AbstractMediatorSerializer {
    protected OMElement serializeSpecificMediator(Mediator m) {

        if (!(m instanceof JsonIterateMediator)) {
            handleException("Unsupported mediator passed in for serialization : " + m.getType());
        }

        OMElement itrElem = fac.createOMElement("jsonIterate", synNS);
        saveTracingState(itrElem, m);

        JsonIterateMediator itrMed = (JsonIterateMediator) m;
        if (itrMed.isContinueParent()) {
            itrElem.addAttribute("continueParent", Boolean.toString(true), nullNS);
        }

        if (itrMed.getId() != null) {
            itrElem.addAttribute("id", itrMed.getId(), nullNS);
        }

        if (itrMed.isPreservePayload()) {
            itrElem.addAttribute("preservePayload", Boolean.toString(true), nullNS);
        }

        if (itrMed.getAttachPath() != null && !"$".equals(itrMed.getAttachPath().toString())) {
            itrElem.addAttribute("attachPath", itrMed.getAttachPath(), nullNS);
        }

        if (itrMed.getExpression() != null) {
            itrElem.addAttribute("expression", itrMed.getExpression(), nullNS);
        } else {
            handleException("Missing expression of the IterateMediator which is required.");
        }

        if (itrMed.getTarget() != null && !itrMed.getTarget().isAsynchronous()) {
            itrElem.addAttribute("sequential", "true", nullNS);
        }

        itrElem.addChild(TargetSerializer.serializeTarget(itrMed.getTarget()));

        serializeComments(itrElem, itrMed.getCommentsList());

        return itrElem;

    }

    public String getMediatorClassName() {
        return JsonIterateMediator.class.getName();
    }
}
