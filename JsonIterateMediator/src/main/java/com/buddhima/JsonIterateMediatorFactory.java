package com.buddhima;

import org.apache.axiom.om.OMAttribute;
import org.apache.axiom.om.OMElement;
import org.apache.synapse.Mediator;
import org.apache.synapse.SynapseConstants;
import org.apache.synapse.config.xml.AbstractMediatorFactory;
import org.apache.synapse.config.xml.TargetFactory;
import org.apache.synapse.mediators.eip.Target;

import javax.xml.namespace.QName;
import java.util.Properties;

public class JsonIterateMediatorFactory extends AbstractMediatorFactory {

    private static final QName ITERATE_Q = new QName(SynapseConstants.SYNAPSE_NAMESPACE, "jsonIterate");
    private static final QName ID_Q = new QName("id");
    private static final QName ATT_CONTPAR = new QName("continueParent");
    private static final QName ATT_PREPLD = new QName("preservePayload");
    private static final QName ATT_ATTACHPATH = new QName("attachPath");
    private static final QName ATT_SEQUENCIAL = new QName("sequential");

    protected Mediator createSpecificMediator(OMElement elem, Properties properties) {

        JsonIterateMediator mediator = new JsonIterateMediator();

        OMAttribute id = elem.getAttribute(ID_Q);
        if (id != null) {
            mediator.setId(id.getAttributeValue());
        }

        OMAttribute continueParent = elem.getAttribute(ATT_CONTPAR);
        if (continueParent != null) {
            mediator.setContinueParent(
                    Boolean.valueOf(continueParent.getAttributeValue()));
        }

        OMAttribute preservePayload = elem.getAttribute(ATT_PREPLD);
        if (preservePayload != null) {
            mediator.setPreservePayload(
                    Boolean.valueOf(preservePayload.getAttributeValue()));
        }

        OMAttribute expression = elem.getAttribute(ATT_EXPRN);
        if (expression != null) {
            mediator.setExpression(expression.getAttributeValue());
        } else {
            handleException("JSONPath expression is required " +
                    "for an JsonIterateMediator under the \"expression\" attribute");
        }

        OMAttribute attachPath = elem.getAttribute(ATT_ATTACHPATH);
        String attachPathValue = "$";
        if (attachPath != null && !mediator.isPreservePayload()) {
            handleException("Wrong configuration for the iterate mediator :: if the iterator " +
                    "should not preserve payload, then attachPath can not be present");
        } else if (attachPath != null) {
            attachPathValue = attachPath.getAttributeValue();
        }
        mediator.setAttachPath(attachPathValue);

        boolean asynchronous = true;
        OMAttribute asynchronousAttr = elem.getAttribute(ATT_SEQUENCIAL);
        if (asynchronousAttr != null && asynchronousAttr.getAttributeValue().equals("true")) {
            asynchronous = false;
        }

        OMElement targetElement = elem.getFirstChildWithName(TARGET_Q);
        if (targetElement != null) {
            Target target = TargetFactory.createTarget(targetElement, properties);
            if (target != null) {
                target.setAsynchronous(asynchronous);
                mediator.setTarget(target);
            }
        } else {
            handleException("Target for an json iterate mediator is required :: missing target");
        }

        addAllCommentChildrenToList(elem, mediator.getCommentsList());

        return mediator;
    }

    public QName getTagQName() {
        return ITERATE_Q;
    }
}
