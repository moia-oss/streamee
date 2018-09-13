# Streamee #

[![CircleCI](https://circleci.com/gh/moia-dev/streamee/tree/master.svg?style=svg)](https://circleci.com/gh/moia-dev/streamee/tree/master)

## Motivation

Streamee is a mirco library (micro all the things) for handling HTTP requests with Akka Stream based
processors for domain logic.

Traditionally (sort of funny to speak of tradition after just a few years) routes built with Akka
HTTP directly interact with actors implementing domain logic via the ask pattern to complete
requests with the returned `Future`s. While this works fine for simple cases, there can be a number
of drawbacks when these actors are not self contained and hence cannot reply instantaneously.

One obvious potential issue is the lack of backpressure: when the route just fires messages at the
poor domain actors, these messages might pile up in the mailbox faster than they can get processed.
This is where Akka Streams – an implementation of Reactive Streams – shines: everything is bounded
and clear semantics are in place to deal with overload. While Streamee cannot actually backpressure
at the HTTP or network level, it still fails fast with the standard HTTP status code 503 – "Service
Unavailable" – and does not overload the domain logic.

Another issue with implementing every domain logic with actors is, that actors are simply not a
silver bullet (Are there any? If so let me know, please!). Actually their most valuable use in
domain modeling is for long-lived – maybe even persistent – state which is used not only locally. In
fact many features of a typical business application can much better be expressed as processes in
which the domain logic is implemented as a flow through a series of stages (aka steps or tasks).
Ultimately these processes can be expressed as Akka Streams `Flow`s which accept
commands and emit results – both domain objects. Streamee aims at making it easy to connect the HTTP
routes with these processors.

Finally – and this has shown to be highly relevant for MOIA – using actors to model long-lived
processes (technically this is perfectly possible, e.g. by using persistent state machines) might
lead to code which is hard to understand and maintain. Even worse, this implementation pattern
conflicts with frictionless rolling upgrades, i.e. it makes a graceful shutdown where all in-flight
requests are served before shutdown at least hard. Streamee on the other hand automatically hooks
into Akka's coordinated shutdown: it makes sure that during shutdown no more commands are accepted
and all in-flight commands have been processed.

## Installation

Include streamee in your project by adding the following to your `build.sbt`:

```
libraryDependencies += "io.moia" %% "streamee" % "3.2.0"
```

Artifacts are hosted on Maven Central.

## Usage and API

In order to use Streamee we first have to define domain logic for each process. Streamee requires to
use the type `Flow[C, R, Any]` where `C` is the command type and `R` is the result type.

In the demo subproject "streamee-demo" one simple process is defined in the `DemoProcess` object:

``` scala
def apply(scheduler: Scheduler)(implicit ec: ExecutionContext): Flow[Request, Response, NotUsed] =
  Flow[Request]
    .mapAsync(1) {
      case Request(id, n) => after(2.seconds, scheduler)(Future.successful((id, n * 42)))
    }
    .mapAsync(1) {
      case (id, n) => after(2.seconds, scheduler)(Future.successful(Response(id, n)))
    }
``` 

Next we have to create the actual processor, i.e. the running stream into which the process is
embedded, by calling `Processor.apply` thereby giving the process, processor settings and the
reference to `CoordinatedShutdown`.

In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
val demoProcessor =
  Processor(DemoProcess(scheduler)(untypedSystem.dispatcher),
            ProcessorSettings(untypedSystem),
            CoordinatedShutdown(untypedSystem))
```

Commands offered via the returned queue are emitted into the given process. Once results are
available the promise given together with the command is completed with success. If the
process back-pressures, offered commands are dropped.

Finally we have to connect each processor to its respective place in the Akka HTTP route with the
`onProcessorSuccess` custom directive. It offers the given command to the given processor thereby
using an `ExpiringPromise` with the given timeout.

In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
post {
  entity(as[DemoProcess.Request]) { request =>
    onProcessorSuccess(request, demoProcessor, demoProcessorTimeout, scheduler) {
      case DemoProcess.Response(_, n) if n == 42 =>
        complete(StatusCodes.BadRequest -> "Request must not have n == 1!")

      case DemoProcess.Response(_, n) =>
        complete(StatusCodes.Created -> n)
    }
  }
}
```  

The `onProcessorSuccess` directive handles the result of offering to the processor: if `Enqueued`
(happiest path) it dispatches the associated result to the inner route via `onSuccess`, if `Dropped`
(not so happy path) it completes the HTTP request with `ServiceUnavailable` and else (failure case,
should not happen) completes the HTTP request with `InternalServerError`.

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
