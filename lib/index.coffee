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

delimmed_key_and_obj = (key, delim, obj) ->
  # obj[key] is guaranteed to exist because we filtered out ones where it doesn't earlier
  hash = JSON.stringify obj[key]
  hash + delim + JSON.stringify(obj) + '\n'

negate = (predicate) -> (args...) -> not predicate args...

partition = (stream, predicate) ->
  [_.stream(stream).filter(predicate).stream(),
   _.stream(stream).filter(negate predicate).stream()]

understream.mixin fs.createWriteStream, 'writeFile', true

# join reference: http://www.albany.edu/~ig4895/join.htm
module.exports = (left, right, options={}) ->

  _(options).defaults delim: '\t', type: 'inner'
  for required in ['delim', 'type', 'on'] when not options[required]
    throw new Error "missing option '#{required}'"

  if _(options.on).isObject()
    left_on = _(options.on).keys()[0]
    right_on = _(options.on).values()[0]
  else
    left_on = right_on = options.on
  options.on = left: left_on, right: right_on

  out = new PassThrough objectMode: true

  async.map(
    [[left, 'left'], [right, 'right']]
    ([stream, type], cb_m) ->
      key = options.on[type]
      file_name = path.join os.tmpdir(), "#{Date.now()}'-'#{Math.random().toString().split('.')[1]}.json"
      # TODO: Must sort streams on the same key that we're joining on
      # "When the field delimiter characters are specified by the -t option, the collating sequence
      # should be the same as sort(1) without the -b option."
      [have_join_key, dont_have_join_key] = partition stream, (obj) -> obj[key]?
      async.parallel [
        (cb_p) ->
          _(dont_have_join_key).stream()
            # The other stream gets stringified and parsed since it goes to disk - do so here as
            # well for consistency
            .map((obj) -> JSON.stringify obj)
            .map((obj) -> JSON.parse obj)
            # Objects without a join key can't pair with anything, so we only want to keep them if
            # we are keeping objects that don't pair
            .filter((obj) -> options.type is 'full' or type is options.type)
            .map((obj) -> if type is 'left' then [obj, null] else [null, obj])
            # Don't just pipe into out because we also pipe into out later and node streams don't
            # handle multiple sources for a stream very well. See understream.combine for how it
            # would need to be handled if we wanted to do it (which we don't because it adds
            # unnecessary complexity)
            .each((obj) -> out.push obj)
            .run cb_p
        (cb_p) ->
          _(have_join_key).stream()
            .map((obj) -> delimmed_key_and_obj key, options.delim, obj)
            .writeFile(file_name)
            .run cb_p
      ], (err) ->
        return cb_m err if err
        cb_m null, file_name

    (err, file_names) ->
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

      # console.log "FILE NAMES:", file_names
      # console.log "SPAWN OPTS: join", spawn_opts.join ' '

      _.stream()
        .spawn('join', spawn_opts)
        .split('\n')
        .filter((line) -> line) # Filter out trailing newline
        .map((line) -> line.split '\t') # Split out left and right into a pair
        .map((line) -> _(line).map JSON.parse) # Parse each object in the pair
        .pipe(out)
        .run (err) ->
          async.each file_names, rimraf, -> # Swallow errors from deleting files
          out.emit 'error', err if err
  )
  out
