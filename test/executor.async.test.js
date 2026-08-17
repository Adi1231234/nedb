/* eslint-env mocha */
const testDb = 'workspace/test.db'
const { promises: fs } = require('fs')
const assert = require('assert').strict
const Datastore = require('../lib/datastore')
const Persistence = require('../lib/persistence')
const Executor = require('../lib/executor')
const { exists, wait } = require('./utils.test.js')

// Test that operations are executed in the right order
// We prevent Mocha from catching the exception we throw on purpose by remembering all current handlers, remove them and register them back after test ends
const testRightOrder = async d => {
  const docs = await d.findAsync({})
  assert.equal(docs.length, 0)

  await d.insertAsync({ a: 1 })
  await d.updateAsync({ a: 1 }, { a: 2 }, {})
  const docs2 = await d.findAsync({})
  assert.equal(docs2[0].a, 2)
  d.updateAsync({ a: 2 }, { a: 3 }, {}) // not awaiting
  d.executor.pushAsync(async () => { throw new Error('Some error') }) // not awaiting
  const docs3 = await d.findAsync({})
  assert.equal(docs3[0].a, 3)
}

// Note:  The following test does not have any assertion because it
// is meant to address the deprecation warning:
// (node) warning: Recursive process.nextTick detected. This will break in the next version of node. Please use setImmediate for recursive deferral.
// see
const testEventLoopStarvation = async d => {
  const times = 1001
  let i = 0
  while (i < times) {
    i++
    d.findAsync({ bogus: 'search' })
  }
  await d.findAsync({ bogus: 'search' })
}

// Test that operations are executed in the right order even with no callback
const testExecutorWorksWithoutCallback = async d => {
  d.insertAsync({ a: 1 })
  d.insertAsync({ a: 2 })
  const docs = await d.findAsync({})
  assert.equal(docs.length, 2)
}

describe('Executor async', function () {
  describe('With persistent database', async () => {
    let d

    beforeEach(async () => {
      d = new Datastore({ filename: testDb })
      assert.equal(d.filename, testDb)
      assert.equal(d.inMemoryOnly, false)
      await Persistence.ensureParentDirectoryExistsAsync(testDb)
      if (await exists(testDb)) await fs.unlink(testDb)
      await d.loadDatabaseAsync()
      assert.equal(d.getAllData().length, 0)
    })

    it('Operations are executed in the right order', () => testRightOrder(d))

    it('Does not starve event loop and raise warning when more than 1000 callbacks are in queue', () => testEventLoopStarvation(d))

    it('Works in the right order even with no supplied callback', () => testExecutorWorksWithoutCallback(d))
  })
}) // ==== End of 'With persistent database' ====

describe('With non persistent database', function () {
  let d

  beforeEach(async () => {
    d = new Datastore({ inMemoryOnly: true })
    assert.equal(d.inMemoryOnly, true)
    await d.loadDatabaseAsync()
    assert.equal(d.getAllData().length, 0)
  })

  it('Operations are executed in the right order', () => testRightOrder(d))

  it('Works in the right order even with no supplied callback', () => testExecutorWorksWithoutCallback(d))
}) // ==== End of 'With non persistent database' ====

describe('processBuffer chains the buffer into the main queue', function () {
  it('A task pushed after processBuffer does not start before the buffer has drained', async () => {
    const executor = new Executor()
    const events = []

    const buffered = executor.pushAsync(async () => {
      events.push('buffered start')
      await wait(50)
      events.push('buffered end')
    })

    executor.processBuffer()

    const queued = executor.pushAsync(async () => { events.push('queued start') })

    await Promise.all([buffered, queued])
    assert.deepEqual(events, ['buffered start', 'buffered end', 'queued start'])
  })

  it('processBuffer makes the main queue wait on the buffer', async () => {
    const executor = new Executor()
    executor.pushAsync(async () => wait(20))

    const guardianBefore = executor.queue.guardian
    executor.processBuffer()
    assert.notEqual(executor.queue.guardian, guardianBefore)

    await executor.queue.guardian
  })

  it('A remove queued while a buffered compaction runs is not dropped from the datafile', async () => {
    const d = new Datastore({ filename: testDb })
    await Persistence.ensureParentDirectoryExistsAsync(testDb)
    if (await exists(testDb)) await fs.unlink(testDb)
    await d.loadDatabaseAsync()
    for (let i = 0; i < 10; i++) await d.insertAsync({ _id: `doc${i}`, i })
    await d.compactDatafileAsync()

    let removal = null
    d.persistence.afterSerialization = async s => {
      if (removal === null) {
        removal = d.removeAsync({ _id: 'doc7' }, {})
        await wait(5)
      }
      return s
    }

    d.executor.ready = false
    d.executor.resetBuffer()

    const compaction = d.compactDatafileAsync()
    d.executor.processBuffer()

    await compaction
    await removal

    assert.equal(d.getAllData().length, 9)
    const reloaded = new Datastore({ filename: testDb })
    await reloaded.loadDatabaseAsync()
    assert.equal(reloaded.getAllData().length, 9)
    assert.equal((await reloaded.findAsync({ _id: 'doc7' })).length, 0)
  })
}) // ==== End of 'processBuffer chains the buffer into the main queue' ====
