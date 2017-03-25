# Json Iterate Mediator

## Configuring Mediator
Build this repository with maven

Add following .jar files to *ESB_HOME/repository/components/dropins*

* Json Iterate Mediator-1.0.0.jar
* json-path-2.1.0.jar (external)
* json-smart-2.2.jar (external)

## Syntax

```
<jsonIterate id="identifier" [sequential=(true | false)] [continueParent=(true | false)] [preservePayload=(true | false)] expression="JSONPath" (attachPath="JSONPath")?>
   <target [to="uri"] [sequence="sequence_ref"] [endpoint="endpoint_ref"]>
     <sequence>
       (mediator)+
     </sequence>?
     <endpoint>
       endpoint
     </endpoint>?
   </target>+
</jsonIterate>
```

## Configurations
The following properties are available,

* **id** - Use to identify messages create by the iterate mediator. Useful if you have multiple iterators in the message flow and map them with specific aggregators.

* **sequential** - Specifies whether split messages should be processed sequentially (true) or in parallel (false).
    * true - Process messages sequentially
    * false - (default) Process messages in parallel

* **continueParent** - Specifies whether the original message should be continued or dropped.
    * true - Parent message continues the flow
    * false - (default) Drops the parent message

* **preservePayload** - Specifies whether the original message should be used as a template when creating the split messages.
    * true - Split message is set to the element in **attachPath** attribute
    * false - (default) Split message is set as the complete payload within iterator

* **expression** - JSONPath expression[1] that matches the elements, which you want to split the message from.

* **attachPath** - JSONPath expression[1] for elements that the split elements should be attached. Use when **preservePayload** attribute is true. If not specified "$" JSONPath is used by default.

The following are **target** configurations,

* **to** - The target endpoint address that split messages should be sent.

* **sequence** - Mediates the message through a sequence. You can either pick a named sequence from the registry or define an anonymous sequence by adding mediators to the current Target mediator as children.

* **endpoint** - Sends the message to an endpoint. Here you can pick a named endpoint from the registry or keep the endpoint as anonymous.

## Example

```
<jsonIterate continueParent="true" preservePayload="true" expression="$.country">
    <target>
        <sequence>
            <log level="full"/>
        </sequence>
    </target>
</jsonIterate>
```
## References

1. [JSONPath Document](https://github.com/jayway/JsonPath/blob/json-path-2.1.0/README.md)
