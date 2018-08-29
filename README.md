# Streamee #

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

## Usage and API

In order to use Streamee we first has to define domain logic for each process. Streamee requires to
use the type `Flow[C, R, Any]` where `C` is the command type and `R` is the result type.

In the demo subproject "streamee-demo" one simple process is defined in the `DemoProcess` object:

``` scala
def apply(scheduler: Scheduler)(implicit ec: ExecutionContext): Flow[String, String, NotUsed] =
  Flow[String]
    .mapAsync(1)(step("step1", 2.seconds, scheduler))
    .mapAsync(1)(step("step2", 2.seconds, scheduler))
``` 

Next we have to create the actual processor, i.e. the running stream into which the process is
embedded, by calling `Processor.apply` thereby giving the process, processor settings and the
reference to `CoordinatedShutdown`.

In the demo subproject "streamee-demo" this happens in `Main`:

``` scala
val demoProcessor =
  Processor(DemoLogic(scheduler)(untypedSystem.dispatcher),
            ProcessorSettings(context.system),
            CoordinatedShutdown(context.system.toUntyped))
```

Commands offered via the returned queue are emitted into the given process. Once results are
available the promise given together with the command is completed with success. If the
process back-pressures, offered commands are dropped.

Finally we have to connect each processor to its respective place in the Akka HTTP route with the
`onProcessorSuccess` custom directive. It offers the given command to the given processor thereby
using an `ExpiringPromise` with the given timeout.

In the demo subproject "streamee-demo" this happens in `Api`:

``` scala
pathPrefix("accounts") {
  post {
    entity(as[Entity]) {
      case Entity(s) =>
        onProcessorSuccess(s, demoProcessor, demoProcessorTimeout, scheduler) {
          case s if s.isEmpty =>
            complete(StatusCodes.BadRequest -> "Empty entity!")
          case s if s.startsWith("taxi") =>
            complete(StatusCodes.Conflict -> "We don't like taxis ;-)")
          case s =>
            complete(StatusCodes.Created -> s)
        }
    }
  }
}
```  

The `onProcessSuccess` directive handles the result of offering to the processor: if `Enqueued`
(happiest path) it dispatches the associated result to the inner route via `onSuccess`, if `Dropped`
(not so happy path) it completes the HTTP request with `ServiceUnavailable` and else (failure case,
should not happen) completes the HTTP request with `InternalServerError`.
