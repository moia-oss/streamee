# Streamee #

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
fact many features of a typical business application can much better be expressed as processes: 
linear pipelines in which the domain logic is implemented as a series of consecutive
steps/tasks/stages. Ultimately these pipelines can be viewed as processors which take commands and 
eventually output results – both domain objects. Streamee aims at making it easy to connect the HTTP
routes with these processors.

Finally – and this has shown to be highly relevant for MOIA – using actors to model long-lived
processes (technically this is perfectly possible, e.g. by using persistent state machines) might
lead to code which is hard to understand and maintain. Even worse, this implementation pattern
conflicts with frictionless rolling upgrades, i.e. it makes a graceful shutdown where all in-flight
requests are served before shutdown at least hard. Streamee on the other hand offers a
shutdown facility which can be hooked into Akka's coordinated shutdown: it makes sure all in-flight
commands have been processed, i.e. results have been created.

## Streamee API

TODO

## Steamee Demo

TODO
