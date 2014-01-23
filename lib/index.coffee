_             = require 'underscore'
async         = require 'async'
fs            = require 'fs'
os            = require 'os'
path          = require 'path'
{PassThrough} = require 'stream'
rimraf        = require 'rimraf'
Understream   = require 'understream'
_.mixin require('underscore.string').exports()

negate = (predicate) -> (args...) -> not predicate args...
partition = (stream, predicate) ->
  [new Understream(stream).filter(predicate).stream(),
   new Understream(stream).filter(negate predicate).stream()]

Understream.mixin (test, msg) ->
  new Understream().each (obj, cb) ->
    return setImmediate cb if test obj
    setImmediate cb, new Error msg
  .stream()
, 'assert', true
Understream.mixin fs.createWriteStream, 'writeFile', true

# join reference: http://www.albany.edu/~ig4895/join.htm
module.exports = (left, right, options={}) ->

  _(options).defaults delim: '\t', type: 'inner'
  for required in ['delim', 'type', 'on'] when not options[required]
    throw new Error "missing option '#{required}'"

  join_keys = if _(options.on).isObject() then _(options.on).pairs()[0] else [options.on, options.on]
  options.on = _.object ['left', 'right'], join_keys

  out = new Understream().map((line) -> _(line).map JSON.parse).stream() # Parse each object in the pair
  file_names = []
  streams = []
  _([[left, 'left'], [right, 'right']]).each ([stream, stream_type]) ->
    field = options.on[stream_type]
    file_name = path.join os.tmpdir(), "#{Date.now()}'-'#{Math.random().toString().split('.')[1]}.json"
    validated = new Understream(stream).assert(_.isObject, 'received non-object in stream')
      .map((obj) -> [obj[field], JSON.stringify obj]).stream()

    [have_join_key, dont_have_join_key] = partition validated, ([key, obj]) -> key?

    streams.push new Understream(dont_have_join_key)
      .map(([key, obj]) -> obj)
      # Objects without a join key can't pair with anything, so we only want to keep them if
      # we are keeping objects that don't pair
      .filter(-> options.type is 'full' or stream_type is options.type)
      # The string null gets turned into actual null later when these are parsed
      .map((obj) -> if stream_type is 'left' then [obj, 'null'] else ['null', obj])
      # Don't just pipe into out because we also pipe into out later and node streams don't
      # handle multiple sources for a stream very well. See Understream.combine for how it
      # would need to be handled if we wanted to do it (which we don't because it adds
      # unnecessary complexity)
      .each (obj, cb) -> out.write obj, cb

    streams.push new Understream(have_join_key)
      .assert((([key, obj]) -> not _(key).isObject()), "join key '#{field}' was not a primitive")
      .map(([key, obj]) -> JSON.stringify(key) + options.delim + obj + '\n')
      .writeFile file_name

    file_names.push file_name

  async.each streams, ((stream, cb_e) -> stream.run cb_e), (err) ->
    return out.emit 'error', err if err
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

    new Understream()
      .spawn('join', spawn_opts)
      .split('\n')
      .filter((line) -> line) # Filter out trailing newline
      .map((line) -> line.split options.delim) # Split out left and right into a pair
      .pipe(out)
      .run (err) ->
        async.each file_names, rimraf, -> # Swallow errors from deleting files
        out.emit 'error', err if err
  out
