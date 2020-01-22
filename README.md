# Streamee #

[![Maven Central](https://img.shields.io/maven-central/v/io.moia/streamee_2.12.svg)](https://maven-badges.herokuapp.com/maven-central/io.moia/streamee_2.12)
[![CircleCI](https://circleci.com/gh/moia-dev/streamee/tree/master.svg?style=svg)](https://circleci.com/gh/moia-dev/streamee/tree/master)

## Motivation

Streamee is a micro library (micro all the things) for handling requests – most probably HTTP
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
libraryDependencies += "io.moia" %% "streamee" % "5.0.0" // find the latest version at the badge at the top
```

Artifacts are hosted on Maven Central.

## Usage and API

In order to use Streamee we first have to define the domain logic for each process. Streamee uses
the type `FlowWithContext[Req, Respondee[Res], Res, Respondee[Res], Any]` where `Req` is the request
type, `Res` is the response type and `Respondee[Res]` is a typed actor providing something like an
expiring location transparent promise which is threaded through the process in the context object
and is used to complete the response.

There are some type aliases for your convenience:

``` scala
type Process[-Req, Res]   = Step[Req, Res, Respondee[Res]]
type Step[-In, +Out, Ctx] = FlowWithContext[In, Ctx, Out, Ctx, Any]
type Respondee[A]         = ActorRef[Respondee.Response[A]]
// More in the io.moia.streamee package object
```

An example process could look like this:

``` scala
// ...

val textShuffler: Process[ShuffleText, Either[Error, TextShuffled]] =
  Process[ShuffleText, Either[Error, TextShuffled]]
    .via(validateRequest)
    .errorTo(errorTap)
    .via(delayProcessing(delay))
    .via(randomError)
    .errorTo(errorTap)
    .via(keepSplitShuffle(wordShufflerSink, wordShufflerProcessorTimeout))
    .via(concat)
    .errorTo(errorTap) // not needed for finishing via(concat0)

def validateRequest[Ctx]: Step[ShuffleText, Either[Error, ShuffleText], Ctx] =
  Step[ShuffleText, Ctx].map {
    case ShuffleText(text) if text.trim.isEmpty                        => Left(Error.EmptyText)
    case ShuffleText(text) if !validText.pattern.matcher(text).matches => Left(Error.InvalidText)
    case shuffleText                                                   => Right(shuffleText)
  }

// ...
```

## FrontProcessor

In order to hook up a process to a service endpoint, e.g. a HTTP route, we use a `FrontProcessor`.
It internally runs a process and allows offering a request into the running process to get
a `Future` for the response.

A `FrontProcessor` is configured with a timeout and fails the `Future`s returned by `offer` in case
the running process cannot produce a response in time. If the running process executes back
pressure, `offer` fails fast by dropping the request with a failed `Future` carrying a
`ProcessorUnavailable` exception.

``` scala
val textShufflerProcessor =
  FrontProcessor(
    textShuffler, // see above
    textShufflerProcessorTimeout,
    "text-shuffler"
  )

// ...

onSuccess(textShufflerProcessor.offer(shuffleText)) {
  case Left(Error.InvalidText)               => complete(BadRequest -> "Invalid text!")
  case Left(Error.RandomError)               => complete(InternalServerError -> "Random error!")
  // ...
  case Right(TextShuffled(original, result)) => complete(s"$original -> $result")
}
```

We probably want to register a custom exception handler for `ProcessorUnavailable` exceptions.
Streamee already comes with a ready to use one: `FrontProcessor.processorUnavailableHandler`.

In "streamee-demo" this happens in `Api` at the level where `bindAndHandle` is called:

``` scala
import FrontProcessor.processorUnavailableHandler
// Same scope like calling Http().bindAndHandle(...)
```

## IntoableProcessor

TODO

## Dealing with errors

TODO

## License

This code is open source software licensed under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0.html).

## Publishing (for maintainers)

To publish a release to Maven Central follow these steps:

1. Create a tag/release on GitHub
2. Publish the artifact to the OSS Sonatype stage repository:
   ```
   sbt +publishSigned
   ```
   Note that your Sonatype credentials needs to be configured on your machine and you need to have access writes to publish artifacts to the group id `io.moia`.
3. Release artifact to Maven Central with:
   ```
   sbt sonatypeBundleRelease
   ```
