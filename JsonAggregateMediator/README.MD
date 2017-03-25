# Json Aggregate Mediator

## Configuring Mediator
Build this repository with maven

Add following .jar files to *ESB_HOME/repository/components/dropins*

* Json Aggregate Mediator-1.0.0.jar
* json-path-2.1.0.jar (external)
* json-smart-2.2.jar (external)

## Syntax
```
<jsonAggregate>    
    <completeCondition [timeout="time-in-seconds"]>
        <messageCount min="int-min" max="int-max"/>?
    </completeCondition>?
    <onComplete expression="JSONPath" enclosingElementProperty="property-name" [sequence="sequence-ref"]>
        (mediator +)?
    </onComplete>
    <correlateOn expression="JSONPath"/>?
</jsonAggregate>
```

## Configurations
The following properties are available,

* **id** - Use to identify messages create by the iterate mediator. Useful if you have multiple iterators in the message flow and map them with specific aggregators.

* Complete Condition
    * **timeout** - The number of seconds taken by the Aggregate mediator to wait for messages. After reaching timeout, the aggregate onComplete is called with received messages.
    * **min** - Minimum number of messages required for the aggregation to complete. 
    * **max** - Maximum number of messages that can exist in an aggregation.

    For Completion Condition either one of the above is enough. If non of the above specified, Aggregate Mediator waits until all the split messages arrived.
    
    Precedence of checking complete condition is `min < timeout < max`
    
    For example; 
    * If timeout and max specified, the aggregation will complete if max no of messages arrived before timeout.
    * If timeout and min specified, the aggregation will complete if min no of messages not arrived before timeout.

* On Complete
    * **expression** - An JSONPath expression specifying which elements should be aggregated
    * **enclosingElementProperty** - The key value for attaching aggregated JSON payloads. Aggregated array of JSON payloads are set as a value against this key.
    * **sequence** - The sequence to run when the aggregation is complete. The sequence can be a one already saved in registry or can be specified inline between onComplete tags.

* Correlate On - (optional) This is an JSONPath expression which provides the basis on which response messages should be selected for aggregation.
    * **expression** - Incoming message should satisfy this JSONPath expression, to be a candidate for aggregation. For example; `$.store.book[?(@.price > 10)]`

## Example

```
<jsonAggregate>
    <completeCondition>
        <messageCount min="-1" max="-1"/>
    </completeCondition>
    <onComplete expression="$.getQuoteResponse">
        <send/>
    </onComplete>
</jsonAggregate>
```
