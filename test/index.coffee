_           = require 'underscore'
assert      = require 'assert'
understream = require 'understream'
join        = require '../'
_.mixin understream.exports()
_.mixin require 'underscore.deep'

BASE_LEFT = ({a: i, b: i + 1} for i in [1..3])
BASE_RIGHT = ({a: i, b: i + 2} for i in [1..3])

configs = [
  on: 'a'
  left: BASE_LEFT
  right: BASE_RIGHT
  expected: _.zip BASE_LEFT, BASE_RIGHT
,
  on: 'b'
  left: BASE_LEFT
  right: BASE_RIGHT
  expected: [
    [{a: 1, b: 2}, null]
    [{a: 2, b: 3}, {a: 1, b: 3}]
    [{a: 3, b: 4}, {a: 2, b: 4}]
    [null, {a: 3, b: 5}]
  ]
,
  on: {'a': 'b'}
  left: BASE_LEFT
  right: BASE_RIGHT
  expected: [
    [{a: 1, b: 2}, null]
    [{a: 2, b: 3}, null]
    [{a: 3, b: 4}, {a: 1, b: 3}]
    [null, {a: 2, b: 4}]
    [null, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {a: '', b: 5}
  right: BASE_RIGHT.concat {a: '', b: 6}
  expected: [
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
    [{a: '', b: 5}, {a: '', b: 6}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {a: undefined, b: 5}
  right: BASE_RIGHT.concat {a: undefined, b: 6}
  expected: [
    [{b: 5}, null]
    [null, {b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {a: 'undefined', b: 5}
  right: BASE_RIGHT.concat {a: 'undefined', b: 6}
  expected: [
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
    [{a: 'undefined', b: 5}, {a: 'undefined', b: 6}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {a: null, b: 5}
  right: BASE_RIGHT.concat {a: null, b: 6}
  expected: [
    [{a: null, b: 5}, null]
    [null, {a: null, b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {a: 'null', b: 5}
  right: BASE_RIGHT.concat {a: 'null', b: 6}
  expected: [
    [{a: 'null', b: 5}, {a: 'null', b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {b: 5}
  right: BASE_RIGHT.concat {b: 6}
  expected: [
    [{b: 5}, null]
    [null, {b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: BASE_LEFT.concat {a: null, b: 5}
  right: BASE_RIGHT.concat {a: null, b: 6}, {a: null, b: 7}
  expected: [
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
    [{a: null, b: 5}, null]
    [null, {a: null, b: 6}]
    [null, {a: null, b: 7}]
  ]
,
  on: 'a'
  left: BASE_LEFT
  right: []
  expected: [
    [{a: 1, b: 2}, null]
    [{a: 2, b: 3}, null]
    [{a: 3, b: 4}, null]
  ]
,
  on: 'a'
  left: []
  right: []
  expected: []
,
  on: 'a'
  left: BASE_LEFT.concat {a: '3'}
  right: BASE_RIGHT
  expected: _.zip(BASE_LEFT, BASE_RIGHT).concat [[{a: '3'}, null]]
,
  on: 'a'
  left: [{a: {some_key: 'some_val'}}]
  right: []
  error: "join key 'a' was not a primitive"
,
  on: 'a'
  left: [4, 5]
  right: [1, 2]
  error: 'received non-object in stream'
]

# For consistency, sort pairs by the values of the keys of the pairs, in alphabetical order
sort_pairs = (pairs) ->
  _(pairs).sortBy (pair) ->
    _(pair).chain()
      .map((el={}) -> (JSON.stringify el[key] for key in _(el).keys().sort()))
      .flatten().value().join ';'

make_configs = (config) ->
  mod_configs = [config]
  # If on is a string, also try the hash version of on to verify it's the same
  mod_configs.push _.extend {}, config, {on: _.object config.on, config.on} if _(config.on).isString()
  # If type isn't specified, assume that the expected values are for type=full and create tests
  # for the other types by filtering it
  mod_configs = _(mod_configs).chain()
    .map (mod_config) ->
      # If we don't specify type, generate an expected array for each type based on the original
      type_spec =
        left: (expected_pair) -> expected_pair[0]?
        right: (expected_pair) -> expected_pair[1]?
        inner: (expected_pair) -> expected_pair[0]? and expected_pair[1]?
        full: -> true
      _(type_spec).map (filter, type) ->
        _.extend {}, mod_config, {type},
          if mod_config.expected then {expected: _(mod_config.expected).filter filter} else {}
    .flatten()
    .value()

describe 'joins', ->
  _(configs).each (config, i) ->
    _(make_configs config).each (mod_config, j) ->
      test_num = "#{i + 1}.#{j + 1}"
      if config.error
        asserts = (err, results) -> assert.equal err?.message, config.error
      else
        asserts = (err, results) ->
          assert.ifError err
          assert.deepEqual sort_pairs(results), sort_pairs(mod_config.expected)
      it "##{test_num} #{if config.error then 'fails' else 'joins'} #{JSON.stringify mod_config}", (done) ->
        [left, right] = _([config.left, config.right]).map (arr) -> _(arr).stream().stream()
        _(join left, right, {on: mod_config.on, type: mod_config.type}).stream().run (err, results) ->
          asserts err, results
          done()
