# Streamee #

[![Maven Central](https://img.shields.io/maven-central/v/io.moia/streamee_2.12.svg)](https://maven-badges.herokuapp.com/maven-central/io.moia/streamee_2.12)
[![CircleCI](https://circleci.com/gh/moia-dev/streamee/tree/master.svg?style=svg)](https://circleci.com/gh/moia-dev/streamee/tree/master)

## Motivation

Streamee is a mirco library (micro all the things) for handling requests – most probably HTTP
requests, but any kind of request-response paradigm should be supported – with Akka Stream based
processors for domain logic.

Traditionally (sort of funny to speak of tradition after just a few years) routes built with Akka
HTTP directly interact with actors implementing domain logic via the ask pattern to complete
requests with the returned `Future`s. While this works fine for simple cases, there can be a number
of drawbacks when these actors are not self contained and hence cannot reply instantaneously.

The core issue with implementing every domain logic with actors is, that actors are simply not a
silver bullet (Are there any? If so let me know, please!). Actually their most valuable use in
domain modeling is for long-lived – maybe even persistent – state which is used not only locally. In
fact many features of a typical business application can much better be expressed as processes in
which the domain logic is implemented as a flow through a series of stages (aka steps or tasks).
These processes can be expressed excellently as Akka Streams `Flow`s which accept requests and emit
responses – both domain objects. Streamee aims at making it easy to connect the HTTP routes with
these processors.

Another potential issue is the lack of back pressure: when the route just fires requests at the poor
domain actors, these requests might pile up in the mailbox faster than they can get processed. This
is where Akka Streams – an implementation of Reactive Streams – shines: everything is bounded and
clear semantics are in place to deal with overload. While Streamee cannot actually back pressure at
the HTTP or network level, it offers permanant processors which allow to fail fast with the standard
HTTP status code 503 – "Service Unavailable" – and will never overload the domain logic.

Finally – and this has shown to be highly relevant for MOIA – using actors to model long-lived
processes (technically this is perfectly possible, e.g. by using persistent state machines) might
lead to code which is hard to understand and maintain. Even worse, this implementation pattern
conflicts with frictionless rolling upgrades, i.e. it makes a graceful shutdown where all in-flight
requests are served before shutdown at least hard. Streamee on the other hand offers an easy way to
hooks into Akka's Coordinated Shutdown which makes sure that during shutdown no more requests are
accepted and – very important – all in-flight requests have been processed.

## Dependencies

Include Streamee in your project by adding the following to your `build.sbt`:

```
libraryDependencies += "io.moia" %% "streamee" % "4.0.0" // find the latest version at the badge at the top  
```

Artifacts are hosted on Maven Central.

## Usage and API

In order to use Streamee we first have to define domain logic for each process. Streamee requires to
use the type `Flow[A, B, Any]` where `A` is the request type and `R` is the response type.

In the demo subproject "streamee-demo" one simple process is defined in the `FourtyTwo` object:

``` scala
type Process = Flow[Request, ErrorOr[Response], NotUsed]
type ErrorOr[A] = Either[Error, A]

def apply()(implicit ec: ExecutionContext, scheduler: Scheduler): Process =
  Flow[Request]
    // Lift into ErrorOr to make all stages look alike
    .map(Right[Error, Request])
    // Via fist stage
    .map(_.map {
      case Request(question) => ValidateQuestionIn(question)
    })
    .via(validateQuestion)
    // Via second stage
    .map(_.map {
      case ValidateQuestionOut(question) => LookupAnswersIn(question)
    })
    .via(lookupAnswersStage)
    // Via third stage
    .map(_.map {
      case LookupAnswersOut(answer) => FourtyTwoIn(answer)
    })
    .via(fourtyTwo)
    // To response
    .map(_.map {
      case FourtyTwoOut(fourtyTwo) => Response(fourtyTwo)
    })
``` 

Next we have to create the actual processor, i.e. the running stream into which the process is
embedded, by calling `Processor.perRequest` or `Processor.permanent`. See below for details about
these different kinds of processors. For `FourtyTwo` we use a per-request processor.
 
In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
val fourtyTwoProcessor = Processor.perRequest(FourtyTwo(),
                                              processorTimeout,
                                              "per-request",
                                              CoordinatedShutdown(untypedSystem))
```

Actually the above is just a short form for the below, i.e. already conveniently registering with
`CoordinatedShutdown`:

``` scala
val fourtyTwoProcessor = 
  Processor
    .perRequest(FourtyTwo(), processorTimeout, "per-request")
    .registerWithCoordinatedShutdown(CoordinatedShutdown(untypedSystem))
```

Requests given to a `Processor` via the `process` method are emitted into the given process. Once
the response is available, the returned `Future` is either completed successfully with the response
or failed with `PromiseExpired` if the processor does not create the response without its `timeout`.

Finally we have to connect each processor to its respective place in the Akka HTTP route with the
default `onSuccess` directive.

In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
post {
  entity(as[Request]) {
    case Request(question) =>
      onSuccess(fourtyTwoProcessor.process(FourtyTwo.Request(question))) {
        case Left(FourtyTwo.Error.EmptyQuestion) =>
          complete(StatusCodes.BadRequest -> "Empty question not allowed!")

        case Left(_) =>
          complete(StatusCodes.InternalServerError -> "Oops, something bad happended :-(")

        case Right(FourtyTwo.Response(answer)) =>
          complete(StatusCodes.Created -> s"The answer is $answer")
      }
  }
}
```  

## Per-request and permanent Processors

So far we have used per-request processors. When the `process` method of such a per-request
processor is called, it runs the process in a sub-flow for the given single request. Notice that
there is no back pressure over requests due to using sub-flows.

We can also use permanent processors. When the `process` method of such a permanent processor is
called, it emits the request into the process. If the process back pressures the returned `Future`
is failed with `ProcessorUnavailable`.

Notice that for permanent processors a buffer size and correlation functions between request and
response need to be given.

In "streamee-demo" this is how it looks like in `FourtyTwoCorrelated`:

``` scala
final case class Request(question: String, correlationId: UUID = UUID.randomUUID())
final case class Response(answer: String, correlationId: UUID = UUID.randomUUID())
``` 

We probably want to register a custom exception handler for `ProcessorUnavailable` exceptions. 
Streamee already comes with a ready to use one: `Processor.processorUnavailableHandler`.

In "streamee-demo" this happens in `Api` at the level where `bindAndHandle` is called:

``` scala
import Processor.processorUnavailableHandler
```

## License

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

## Publishing (for maintainers)

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
