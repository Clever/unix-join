_           = require 'underscore'
assert      = require 'assert'
understream = require 'understream'
join        = require '../'
_.mixin understream.exports()
_.mixin require 'underscore.deep'

configs = [
  on: 'a'
  left: ({a: i, b: i + 1} for i in [1..3])
  right: ({a: i, b: i + 2} for i in [1..3])
  expected: _.zip ({a: i, b: i + 1} for i in [1..3]), ({a: i, b: i + 2} for i in [1..3])
,
  on: 'b'
  left: ({a: i, b: i + 1} for i in [1..3])
  right: ({a: i, b: i + 2} for i in [1..3])
  expected: [
    [{a: 1, b: 2}, null]
    [{a: 2, b: 3}, {a: 1, b: 3}]
    [{a: 3, b: 4}, {a: 2, b: 4}]
    [null, {a: 3, b: 5}]
  ]
,
  on: {'a': 'b'}
  left: ({a: i, b: i + 1} for i in [1..3])
  right: ({a: i, b: i + 2} for i in [1..3])
  expected: [
    [{a: 1, b: 2}, null]
    [{a: 2, b: 3}, null]
    [{a: 3, b: 4}, {a: 1, b: 3}]
    [null, {a: 2, b: 4}]
    [null, {a: 3, b: 5}]
  ]
  type: 'full'
,
  on: 'a'
  left: ({a: i, b: i + 1} for i in [1..3]).concat {a: '', b: 5}
  right: ({a: i, b: i + 2} for i in [1..3]).concat {a: '', b: 6}
  expected: [
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
    [{a: '', b: 5}, {a: '', b: 6}]
  ]
,
  on: 'a'
  left: ({a: i, b: i + 1} for i in [1..3]).concat {a: undefined, b: 5}
  right: ({a: i, b: i + 2} for i in [1..3]).concat {a: undefined, b: 6}
  expected: [
    [{b: 5}, null]
    [null, {b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: ({a: i, b: i + 1} for i in [1..3]).concat {a: null, b: 5}
  right: ({a: i, b: i + 2} for i in [1..3]).concat {a: null, b: 6}
  expected: [
    [{a: null, b: 5}, null]
    [null, {a: null, b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: 'a'
  left: ({a: i, b: i + 1} for i in [1..3]).concat {b: 5}
  right: ({a: i, b: i + 2} for i in [1..3]).concat {b: 6}
  expected: [
    [{b: 5}, null]
    [null, {b: 6}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
,
  on: {'a': 'a'}
  left: ({a: i, b: i + 1} for i in [1..3]).concat {a: null, b: 5}
  right: ({a: i, b: i + 2} for i in [1..3]).concat {a: null, b: 6}, {a: null, b: 7}
  expected: [
    [{a: null, b: 5}, null]
    [null, {a: null, b: 6}]
    [null, {a: null, b: 7}]
    [{a: 1, b: 2}, {a: 1, b: 3}]
    [{a: 2, b: 3}, {a: 2, b: 4}]
    [{a: 3, b: 4}, {a: 3, b: 5}]
  ]
]

describe 'joins', ->
  _(configs).each (config, i) ->
    # # If on is a string, also try the hash version of on to verify it's the same
    if _(config.on).isString()
      mod_configs = [_.deepClone(config), _.deepClone(config)]
      mod_configs[1].on = _.object config.on, config.on
    else
      mod_configs = [config]
    # If type isn't specified, assume that it's for type=full and create tests for the other types
    # by filtering the expected array
    unless config.type
      mod_configs = _(mod_configs).chain()
        .map (mod_config) ->
          # If we don't specify type, generate an expected array for each type based on the original
          left = _(mod_config).deepClone()
          left.expected = _(left.expected).chain()
            .deepClone()
            .filter((expected_pair) -> expected_pair[0]?).value()
          left.type = 'left'
          right = _(mod_config).deepClone()
          right.expected = _(right.expected).chain()
            .deepClone()
            .filter((expected_pair) -> expected_pair[1]?).value()
          right.type = 'right'
          inner = _(mod_config).deepClone()
          inner.expected = _(inner.expected).chain()
            .deepClone()
            .filter((expected_pair) -> expected_pair[0]? and expected_pair[1]?).value()
          inner.type = 'inner'
          full = _(mod_config).deepClone()
          full.expected = _(full.expected).deepClone()
          full.type = 'full'
          [left, right, inner, full]
        .flatten()
        .value()
    _(mod_configs).each (mod_config, j) ->
      test_num = "#{i + 1}.#{j + 1}"
      it "joins config ##{test_num} - #{JSON.stringify mod_config}", (done) ->
        [left, right] = _([mod_config.left, mod_config.right]).map (arr) -> _(arr).stream().stream()
        _(join left, right, {on: mod_config.on, type: mod_config.type}).stream().run (err, results) ->
          assert.ifError err
          assert.deepEqual results, mod_config.expected
          done()
