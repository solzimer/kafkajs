jest.mock('../runner')

const { Kafka } = require('../../../index')
const Runner = require('../runner')
const { createErrorFromCode } = require('../../protocol/error')

describe('Consumer > static membership configuration', () => {
  let kafka

  beforeEach(() => {
    kafka = new Kafka({ brokers: ['localhost:9092'], logLevel: 0 })
    Runner.mockReset()
    Runner.mockImplementation(() => ({ start: jest.fn(), stop: jest.fn() }))
  })

  test.each(['worker-0', null, undefined])(
    'passes identity %p from the public API to the group',
    async groupInstanceId => {
      const consumer = kafka.consumer({ groupId: 'group', groupInstanceId })
      await consumer.run()
      expect(Runner.mock.calls[0][0].consumerGroup.groupInstanceId).toBe(groupInstanceId || null)
      await consumer.disconnect()
    }
  )

  test('does not restart after a fenced instance crashes', async () => {
    Runner.mockImplementation(({ onCrash }) => ({
      start: jest.fn().mockImplementation(() => onCrash(createErrorFromCode(82))),
      stop: jest.fn(),
    }))
    const consumer = kafka.consumer({ groupId: 'group', groupInstanceId: 'worker-0' })
    const onCrash = jest.fn()
    consumer.on(consumer.events.CRASH, onCrash)
    await consumer.run()
    // Instrumentation listeners run asynchronously.
    await new Promise(resolve => setImmediate(resolve))
    expect(onCrash).toHaveBeenCalledWith(
      expect.objectContaining({
        payload: expect.objectContaining({ restart: false }),
      })
    )
    expect(Runner).toHaveBeenCalledTimes(1)
  })
})
