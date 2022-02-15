# Twitter Stream Analysis With ZIO 

This project was created as a learning exercise for ZIO.
The functionality to be implemented is to process the Twitter
tweet sample stream (see:
https://developer.twitter.com/en/docs/twitter-api/v1/tweets/sample-realtime/overview)
and to generate a stream of statistics from the stream.
Currently, this implementation uses the v1.1 API, but should be
updated to the v2 endpoint.

The origins of the content was a take home test that was my first
foray into the world of pure functional programming, which
can be found at https://github.com/Andy-42/ssc-programming-assignment2.
That implementation was done using Cats, Cats Effect and FS2.
The purpose of this project is implement similar functionality
but using ZIO as much as possible. Some aspects of the original
take home assignment were pruned off, so it is not an exact reimplementation,
but I have kept the essential parts of it.

My main focus here is in using the streaming libraries. 
In the original implementation, I used FS2, which I have to say was
very impressive. Lately, I have become more interested in the
ZIO ecosystem, so I created this project to work through the mechanics
of implementing a system in ZIO. 

Although I have experience with Scala
and functional programming, I have found that both the Typelevel and
ZIO ecosystems present something of a conceptual barrier in making the
leap from documentation to actual implementation. That is the purpose
of this project to get an initial toe-hold.

Since streaming is central to this project,
using ZStream is of obvious importance. However, it turns out that some of
the other parts of the ZIO ecosystem are just as interesting. I will try
to capture notes about each component here.

## ZIO 1.x or 2.x?

I had originally thought that I would start this as a ZIO 2.0 project, but
had some issues that were difficult to solve given the state of ZIO 2.0 documentation
at the time. I did the initial implementation in ZIO 1.0, and I will convert
it to 2.0 shortly.

## Config

Using zio-config was a really nice surprise. Implementing a configuration layer
was simple - certainly less boilerplate than the original implementation.
Other than describing the configuration in case classes, there is little else
needed. I had to implement a Descriptor to specify how strings were converted to URLs,
and to specify that the HOCON configuration uses kebab casing. 

Other than that zio-config does its magic and creates the configuration layer automatically.
The complete lack of boilerplate is amazing. There is one issue that I had a problem with is
that I wanted to be able to access the configuration from business logic (i.e., the main program)
as opposed to from another layer.

The generated configuration trait is just the case class that represents the entire configuration.
The members of this configuration are all pure code (i.e., not effects). Since they are not effects,
they can't be accessed using an accessor (i.e., ZIO.serviceWith), so they can't be accessed from
outside of a service. I was able to work around this by adding a member to the top-level configuration
that wraps the result in a ZIO (`Config.streamParametersM'). This lets me create an accessor and use
the configuration from outside of a layer.

A layer that depends on Config can access the pure code directly (i.e., without flatmap-ing over the
configuration members). 

## JSON

The twitter sample stream provides a stream of bytes in JSON lines format
(i.e., JSON where each entity is separated by a newline).

In the original implementation, each line was parsed into a circe AST.
The filtering of stream (to only extract the tweet types of interest)
is done by examining the contents of the AST. In this implementation,
this filtering step is done by looking at the prefix string in each
line. This is done to avoid parsing the JSON into an AST, just to ignore
it. This is purely a bit of optimization (perhaps pre-optimization, but this
is my party, and I can do what I want).

The other difference is that a circe AST is not used at all. Instead, the
implementation uses zio-json to parse the subset of the JSON that is relevant
directly into a case class. Very little of the tweet contents is of interest
(in this exercise) so we can expect this to be more performant than
parsing the entire tweet into a JSON AST.

I got some weird build errors when I imported zio-json.
Could this be due to differences in ZIO dependency versions?

It should be possible to take this a step further and get extreme performance
by using techniques such as https://github.com/plokhotnyuk/jsoniter-scala.
This lets you parse directly from the byte stream. This would be an interesting
exercise, but out of scope for this exercise.

## HTTP

http4s + fs2 
using http4s to start with since the stream is easily converted to zstream.
TODO: convert later.

## ZStream

## Test

## TODO

* Migrate to ZIO 2.x
* Migrate HTTP tweet source from http4s to zio-http and implement OAuth signing.

