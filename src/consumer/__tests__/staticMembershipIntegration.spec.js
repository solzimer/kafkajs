const net = require('net')
const { Kafka, logLevel } = require('../../../index')
const ConsumerGroup = require('../consumerGroup')
const { secureRandom, waitFor } = require('testHelpers')

const broker = process.env.KAFKA_STATIC_MEMBERSHIP_BROKER
const describeWithBroker = broker ? describe : describe.skip

describeWithBroker('Consumer > static membership integration', () => {
  let kafka, admin, topic, groupId, consumers, groups, joinSpy

  beforeEach(async () => {
    kafka = new Kafka({
      brokers: [broker],
      logLevel: logLevel.NOTHING,
      // A single-broker Docker environment may advertise a container-only hostname.
      ...(process.env.KAFKA_STATIC_MEMBERSHIP_HOST && {
        socketFactory: ({ host, port, onConnect }) =>
          net.connect({ host: process.env.KAFKA_STATIC_MEMBERSHIP_HOST, port }, onConnect),
      }),
    })
    topic = `static-membership-${secureRandom()}`
    groupId = `static-membership-${secureRandom()}`
    consumers = []
    groups = []
    const joinAndSync = ConsumerGroup.prototype.joinAndSync
    joinSpy = jest.spyOn(ConsumerGroup.prototype, 'joinAndSync').mockImplementation(function() {
      groups.push(this)
      return joinAndSync.call(this)
    })
    admin = kafka.admin()
    await admin.connect()
    await admin.createTopics({ topics: [{ topic, numPartitions: 2, replicationFactor: 1 }] })
  })

  afterEach(async () => {
    try {
      await Promise.all(consumers.map(consumer => consumer.disconnect()))
      // Static members stay in the group until their sessions expire.
      await admin.deleteTopics({ topics: [topic] })
    } finally {
      await admin.disconnect()
      joinSpy.mockRestore()
    }
  })

  /**
   * Start a real consumer with a stable identity and record it for cleanup.
   * @returns {Promise<import('../../../types').Consumer>} Running static consumer.
   */
  const startConsumer = async () => {
    const consumer = kafka.consumer({
      groupId,
      groupInstanceId: 'worker-0',
      sessionTimeout: 10000,
      heartbeatInterval: 500,
    })
    consumers.push(consumer)
    await consumer.connect()
    await consumer.subscribe({ topic, fromBeginning: true })
    await consumer.run({ eachMessage: async () => {} })
    return consumer
  }

  test('retains membership and generation through disconnect and restart', async () => {
    const first = await startConsumer()
    const generationId = groups[0].generationId
    const assignment = groups[0].assigned()
    await first.commitOffsets([{ topic, partition: 0, offset: '0' }])
    await first.disconnect()

    const description = await admin.describeGroups([groupId])
    expect(description.groups[0].members).toHaveLength(1)
    await startConsumer()
    expect(groups[1].generationId).toBe(generationId)
    expect(groups[1].assigned()).toEqual(assignment)
  })

  test('fences a running instance with the same identity without restarting it', async () => {
    const first = await startConsumer()
    const onCrash = jest.fn()
    first.on(first.events.CRASH, onCrash)
    await startConsumer()
    await waitFor(() => onCrash.mock.calls.length > 0, { delay: 100 })
    const { error, restart } = onCrash.mock.calls[0][0].payload
    let cause = error
    while (cause.cause) {
      cause = cause.cause
    }
    expect(cause.type).toBe('FENCED_INSTANCE_ID')
    expect(restart).toBe(false)
    expect(groups).toHaveLength(2)
  })

  test('rejoins with a new generation after its session expires', async () => {
    const first = await startConsumer()
    const generationId = groups[0].generationId
    await first.disconnect()
    await waitFor(
      async () => {
        const { groups } = await admin.describeGroups([groupId])
        return groups[0].members.length === 0
      },
      { delay: 500, maxWait: 20000 }
    )
    await startConsumer()
    expect(groups[1].generationId).toBeGreaterThan(generationId)
  })
})
