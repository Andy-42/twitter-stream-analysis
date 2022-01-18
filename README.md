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

I initially thought that I would implement this using ZIO 2.x since there
are many exciting improvements in ZIO 2.x. In particular, I really like
how ZLayer in ZIO 2.x is simplified. This is a big one for me, and one
that I would like to get to sooner rather than later. Using ZLayer
as an organizing principle was one of the ideas that made me interested
in ZIO - I am not aware of any similar idea in the Typelevel stack.

I quickly got frustrated with the lack of documentation for ZIO 2.x.
It was just too hard to get up to speed on ZIO without proper documentation.
Perhaps I can contribute something to that effort - but only after I get myself
going in ZIO, and I really need some sort of documentation for that.

I'm also not convinced that all the parts that I need for this project
are available in ZIO 2.x yet. I will leave the conversion to ZIO 2.x
to some later stage.

## Config

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

