_             = require 'underscore'
async         = require 'async'
fs            = require 'fs'
os            = require 'os'
path          = require 'path'
{PassThrough} = require 'stream'
rimraf        = require 'rimraf'
understream   = require 'understream'
_.mixin understream.exports()
_.mixin require('underscore.string').exports()

# This function is synchronous, but the only way that map exposes to return an error is via
# an asynchronous callback
delimmed_key_and_obj = (field, delim, [key, obj], cb) ->
  return cb new Error "join key '#{field}' was not a primitive" if _(key).isObject()
  hash = JSON.stringify key
  cb null, hash + delim + obj + '\n'

negate = (predicate) -> (args...) -> not predicate args...

partition = (stream, predicate) ->
  [_.stream(stream).filter(predicate).stream(),
   _.stream(stream).filter(negate predicate).stream()]

understream.mixin fs.createWriteStream, 'writeFile', true
understream.mixin (test, msg) ->
  _.stream()
    .each (obj, cb) ->
      return cb() if test obj
      cb new Error msg
    .stream()
, 'assert', true

# join reference: http://www.albany.edu/~ig4895/join.htm
module.exports = (left, right, options={}) ->

  _(options).defaults delim: '\t', type: 'inner'
  for required in ['delim', 'type', 'on'] when not options[required]
    throw new Error "missing option '#{required}'"

  join_keys = if _(options.on).isObject() then _(options.on).pairs()[0] else [options.on, options.on]
  options.on = _.object ['left', 'right'], join_keys

  head = new PassThrough objectMode: true
  file_names = []
  streams = []
  _([[left, 'left'], [right, 'right']]).each ([stream, stream_type]) ->
    key = options.on[stream_type]
    file_name = path.join os.tmpdir(), "#{Date.now()}'-'#{Math.random().toString().split('.')[1]}.json"
    validated = _(stream).stream().assert(_.isObject, 'received non-object in stream')
      .map((obj) -> [obj[key], JSON.stringify obj]).stream()

    [have_join_key, dont_have_join_key] = partition validated, ([key, obj]) -> key?

    streams.push _(dont_have_join_key).stream()
      .map(([key, obj]) -> obj)
      # Objects without a join key can't pair with anything, so we only want to keep them if
      # we are keeping objects that don't pair
      .filter(-> options.type is 'full' or stream_type is options.type)
      # The string null gets turned into actual null later when these are parsed
      .map((obj) -> if stream_type is 'left' then [obj, 'null'] else ['null', obj])
      # Don't just pipe into head because we also pipe into head later and node streams don't
      # handle multiple sources for a stream very well. See understream.combine for how it
      # would need to be handled if we wanted to do it (which we don't because it adds
      # unnecessary complexity)
      .each (obj) -> head.push obj

    streams.push _(have_join_key).stream()
      .map((obj, cb) -> setImmediate delimmed_key_and_obj, key, options.delim, obj, cb)
      .writeFile file_name

    file_names.push file_name

  async.each streams, ((stream, cb_e) -> stream.run cb_e), (err) ->
    return head.emit 'error', err if err
    spawn_opts = [
      '-o', '1.2,2.2'     # Display only the stringified objects
      '-e', 'null'        # Replace any missing data fields with null
      '-1', '1'           # Join the first field from the left file...
      '-2', '1'           # ...to the first field from the right file
      '-t', options.delim # Use delim as the delimiter in input and output
    ]
    spawn_opts.push '-a1' if options.type in ['left', 'full'] # Keep unpaired lines from left
    spawn_opts.push '-a2' if options.type in ['right', 'full'] # Keep unpaired lines from right
    spawn_opts.push file_names... # Files to join

    _.stream()
      .spawn('join', spawn_opts)
      .split('\n')
      .filter((line) -> line) # Filter out trailing newline
      .map((line) -> line.split '\t') # Split out left and right into a pair
      .pipe(head)
      .run (err) ->
        async.each file_names, rimraf, -> # Swallow errors from deleting files
        head.emit 'error', err if err
  _(head).stream().map((line) -> _(line).map JSON.parse).stream() # Parse each object in the pair
