# Streamee #

[![CircleCI](https://circleci.com/gh/moia-dev/streamee/tree/master.svg?style=svg)](https://circleci.com/gh/moia-dev/streamee/tree/master)

## Motivation

Streamee is a mirco library (micro all the things) for handling requests – most probably HTTP
requests, but any kind of request-response paradigm should be supported – with Akka Stream based
processors for domain logic which allow for back pressure and integration with custom shutdown.

Traditionally (sort of funny to speak of tradition after just a few years) routes built with Akka
HTTP directly interact with actors implementing domain logic via the ask pattern to complete
requests with the returned `Future`s. While this works fine for simple cases, there can be a number
of drawbacks when these actors are not self contained and hence cannot reply instantaneously.

One obvious potential issue is the lack of backpressure: when the route just fires requests at the
poor domain actors, these requests might pile up in the mailbox faster than they can get processed.
This is where Akka Streams – an implementation of Reactive Streams – shines: everything is bounded
and clear semantics are in place to deal with overload. While Streamee cannot actually backpressure
at the HTTP or network level, it allows to fail fast with the standard HTTP status code 503 – 
"Service Unavailable" – and will never overload the domain logic.

Another issue with implementing every domain logic with actors is, that actors are simply not a
silver bullet (Are there any? If so let me know, please!). Actually their most valuable use in
domain modeling is for long-lived – maybe even persistent – state which is used not only locally. In
fact many features of a typical business application can much better be expressed as processes in
which the domain logic is implemented as a flow through a series of stages (aka steps or tasks).
These processes can be expressed excellently as Akka Streams `Flow`s which accept
requests and emit responses – both domain objects. Streamee aims at making it easy to connect the
HTTP routes with these processors.

Finally – and this has shown to be highly relevant for MOIA – using actors to model long-lived
processes (technically this is perfectly possible, e.g. by using persistent state machines) might
lead to code which is hard to understand and maintain. Even worse, this implementation pattern
conflicts with frictionless rolling upgrades, i.e. it makes a graceful shutdown where all in-flight
requests are served before shutdown at least hard. Streamee on the other hand offers an easy way to
hooks into Akka's coordinated shutdown which makes sure that during shutdown no more requests are
accepted and – very important – all in-flight requests have been processed.

## Installation

Include streamee in your project by adding the following to your `build.sbt`:

```
libraryDependencies += "io.moia" %% "streamee" % "4.0.0"
```

Artifacts are hosted on Maven Central.

## Usage and API

In order to use Streamee we first have to define domain logic for each process. Streamee requires to
use the type `Flow[A, B, Any]` where `A` is the request type and `R` is the response type.

In the demo subproject "streamee-demo" one simple process is defined in the `DemoProcess` object:

``` scala
def apply()(implicit ec: ExecutionContext,
            scheduler: Scheduler): Flow[Request, Response, NotUsed] =
  Flow[Request]
    .mapAsync(1) {
      case request @ Request(_, question) =>
        after(2.seconds, scheduler) {
          Future.successful((request, question.length * 42))
        }
    }
    .mapAsync(1) {
      case (Request(correlationId, question), n) =>
        after(2.seconds, scheduler) {
          Future.successful(Response(correlationId, (n / question.length).toString))
        }
    }
``` 

Next we have to create the actual processor, i.e. the running stream into which the process is
embedded, by calling `Processor.apply` thereby giving the process and processor settings. Most
probably we also want to register with `CoordinatedShutdown`.

In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
val demoProcessor =
  Processor(DemoProcess(), "demo-processor", ProcessorSettings(untypedSystem))(
    _.correlationId,
    _.correlationId
  ).registerForCoordinatedShutdown(CoordinatedShutdown(untypedSystem))
```

Requests offered to the returned `Processor` via the `process` method are emitted into the given
process. Once a response is available, the returned `Future` is completed successfully. If the
process back-pressures, requests are dropped and the returned `Future`s are completed with a failure
of type `ProcessorUnavailable`.

Finally we have to connect each processor to its respective place in the Akka HTTP route with the
default `onSuccess` directive.

In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
post {
  entity(as[DemoProcess.Request]) { request =>
    onSuccess(demoProcessor.process(request, demoProcessorTimeout)) {
      case DemoProcess.Response(_, answer) => complete(StatusCodes.Created -> answer)
    }
  }
}
```  

The `process` method also takes a `timeout` that defines the maximum duration for requests to be
processed. After that a the returned `Future`s are completed with a failure of type
`PromiseExpired`.

We probably want to register a custom exception handler for `ProcessorUnavailable` exceptions. 
Streamee already comes with a ready to use one: `Processor.processorUnavailableHandler`.

In the demo subproject "streamee-demo" this happens in `Api` at the level where `bindAndHandle` is
called:

``` scala
import Processor.processorUnavailableHandler
```

## License

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

## Publishing

To publish a release to Maven Central follow these steps:

1. Create a tag/release on GitHub
2. Publish the artifact to the OSS Sonatype stage repository:
   ```
   sbt publishSigned
   ```  
   Note that your Sonatype credentials needs to be configured on your machine and you need to have access writes to publish artifacts to the group id `io.moia`.
3. Release artifact to Maven Central with:
   ```
   sbt sonatypeRelease
   ```
