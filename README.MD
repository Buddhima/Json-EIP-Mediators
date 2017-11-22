# Mediators for Enterprise Integration Patterns
These mediators work with native JSON in WSO2 ESB.
Existing mediators do not support JSON manipulations, hence message payload is converted to XML.

Following mediators are implemented

* Json Iterate Mediator
* Json Aggregate Mediator

## Configuring Mediator
Build this repository with maven

Add following .jar files to *ESB_HOME/repository/components/dropins*

* Json (Iterate/Aggregate) Mediator-1.0.0.jar
* json-path-2.1.0.jar (external)
* json-smart-2.2.1.jar (external)
* accessors-smart-1.1.jar (external)

## Syntax

* [Json Iterate Mediator Syntax](https://github.com/Buddhima/Json-EIP-Mediators/tree/master/JsonIterateMediator#syntax)
* [Json Aggregate Mediator Syntax](https://github.com/Buddhima/Json-EIP-Mediators/tree/master/JsonAggregateMediator#syntax)

## Example

**Split-Aggregate Pattern**

This pattern describes that incoming single request split in to number of requests and sending to backend.
The responses from backend service(s) get aggregated before sending to the client.

Sample ESB configuration is as below;

```
<api xmlns="http://ws.apache.org/ns/synapse" name="sampleApi" context="/sample">
<resource methods="POST" uri-template="/*">
<inSequence>
    <log level="full"/>
    <jsonIterate continueParent="false" preservePayload="true" expression="$.messages" attachPath="$.messages">
        <target>
            <sequence>
                <log level="full"/>
                <header name="To" value="http://www.mocky.io/v2/58d6459b100000e601949cb7"/>
                <call/>
            </sequence>
        </target>
    </jsonIterate>
    <log level="full"/>
    <jsonAggregate>
        <completeCondition>
            <messageCount min="-1" max="-1"/>
        </completeCondition>
        <onComplete expression="$.message" enclosingElementProperty="responses">
            <log level="full"/>
            <send/>
        </onComplete>
    </jsonAggregate>
</inSequence>
</resource>
</api>
```

**Sample Request Payload from client**

```
{
	"originator" : "my-company",
	"messages" : [
		{
			"country" : "Sri Lanka",
			"code" : "94"
		},
		{
			"country" : "America",
			"code" : "01"
		},
		{
			"country" : "Australia",
			"code" : "61"
		}
	]
}
```

**Request sending to backend**

```
{
  "originator": "my-company",
  "messages": {
    "country": "Sri Lanka",
    "code": "94"
  }
}
```

**Response from backend**

```
{
  "type": "response",
  "message": {
    "subject": "This is the reply",
    "text": "Welcome back!!!"
  }
}
```

**Sample Response Payload to client**

```
{
    "responses": [
        {
            "subject": "This is the reply",
            "text": "Welcome back!!!"
        },
        {
            "subject": "This is the reply",
            "text": "Welcome back!!!"
        },
        {
            "subject": "This is the reply",
            "text": "Welcome back!!!"
        }
    ]
}
```

## Future Improvements

If you have suggestions or issues, please mention them at "issues" section at GitHub repository.
