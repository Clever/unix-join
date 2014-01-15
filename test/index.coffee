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
]

# For consistency, sort pairs by the values of the keys of the pairs, in alphabetical order
sort_pairs = (pairs) ->
  _(pairs).sortBy (pair) ->
    _(pair).chain()
      .map((el={}) -> (el[key] for key in _(el).keys().sort()))
      .flatten().value().join ''

describe 'joins', ->
  _(configs).each (config, i) ->
    # If on is a string, also try the hash version of on to verify it's the same
    if _(config.on).isString()
      mod_configs = [_(config).deepClone(), _(config).deepClone()]
      mod_configs[1].on = _.object config.on, config.on
    else
      mod_configs = [_(config).deepClone()]
    # If type isn't specified, assume that the expected values are for type=full and create tests
    # for the other types by filtering it
    unless config.type
      mod_configs = _(mod_configs).chain()
        .map (mod_config) ->
          # If we don't specify type, generate an expected array for each type based on the original
          type_spec =
            left: (expected_pair) -> expected_pair[0]?
            right: (expected_pair) -> expected_pair[1]?
            inner: (expected_pair) -> expected_pair[0]? and expected_pair[1]?
            full: -> true
          _(type_spec).map (filter, type) ->
            new_config = _(mod_config).deepClone()
            _(new_config).extend {type}, expected: _(new_config.expected).filter filter
        .flatten()
        .value()
    _(mod_configs).each (mod_config, j) ->
      test_num = "#{i + 1}.#{j + 1}"
      it "joins config ##{test_num} - #{JSON.stringify mod_config}", (done) ->
        [left, right] = _([mod_config.left, mod_config.right]).map (arr) -> _(arr).stream().stream()
        _(join left, right, {on: mod_config.on, type: mod_config.type}).stream().run (err, results) ->
          assert.ifError err
          assert.deepEqual sort_pairs(results), sort_pairs(mod_config.expected)
          done()
