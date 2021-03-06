# unix-join

Join streams of JSON objects using the unix join command.
This allows joining of very large data sets in a stream interface that Node normally can't handle
due to memory constraints.

**NOTE**: `unix-join`, like the join command that it relies on, assumes that the inputs are sorted
by the join key. If they are not sorted, the behavior is undefined. The output is unordered.

## Install

```
npm install unix-join
```

## Usage

`unix-join` takes three arguments: two Readable streams of JSON objects (left and right) and an
options object.
Valid options are:
* **type** - the type of join to perform. one of inner, left, right, full. default **inner**
* **on** - the fields to join on. can be a string, where it uses the value of that streams from both
streams, or an object of the form `key: val` where `key` is the field to join from in the left
stream and `val` is the field to join to in the right stream. the value of the field to join on must
be a primitive in all cases
