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
  hash = JSON.stringify obj[key] ? null # undefined doesn't stringify, treat it the same as null
  hash + delim + JSON.stringify(obj) + '\n'

understream.mixin fs.createWriteStream, 'writeFile', true

# join reference: http://www.albany.edu/~ig4895/join.htm
module.exports = (left, right, options={}) ->

  _(options).defaults delim: '\t', type: 'inner'
  for key in ['delim', 'type', 'on'] when not options[key]
    throw new Error "missing option '#{key}'"

  if _(options.on).isObject()
    left_on = _(options.on).keys()[0]
    right_on = _(options.on).values()[0]
  else
    left_on = right_on = options.on
  options.on = left: left_on, right: right_on

  out = new PassThrough objectMode: true

  async.map(
    [[left, options.on.left], [right, options.on.right]]
    ([stream, key], cb_m) ->
      file_name = path.join os.tmpdir(), "#{Date.now()}'-'#{Math.random().toString().split('.')[1]}.json"
      # TODO: Must sort streams on the same key that we're joining on
      # "When the field delimiter characters are specified by the -t option, the collating sequence
      # should be the same as sort(1) without the -b option."
      _(stream).stream()
        .map((obj) -> delimmed_key_and_obj key, options.delim, obj)
        .writeFile(file_name)
        .run (err) ->
          return cb_m err if err
          cb_m null, file_name
    (err, file_names) ->
      return out.emit 'error', err if err

      spawn_opts = [
        '-o', '0,1.2,2.2'   # Display the join field and the stringified objects
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
        .map (line) ->
          # If the join field is null, you shouldn't actually join on it. Unfortunately, join
          # doesn't have any concept of null since it works on text - it's treated the same as any
          # string, so it joins null to null.
          # Thus, we need to correct these ourselves.
          if line[0] is 'null'
            results = []
            results.push [line[1], null] if options.type in ['left', 'full']
            results.push [null, line[2]] if options.type in ['right', 'full']
            results
          else
            [[line[1], line[2]]]
        .flatten(true)
        .map((line) -> _(line).map JSON.parse) # Parse each object in the pair
        .pipe(out)
        .run (err) ->
          async.each file_names, rimraf, -> # Swallow errors from deleting files
          out.emit 'error', err if err
  )
  out
