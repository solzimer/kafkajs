const ConsumerGroup = require('../consumerGroup')
const { MemberAssignment } = require('../assignerProtocol')
const InstrumentationEventEmitter = require('../../instrumentation/emitter')
const { createErrorFromCode } = require('../../protocol/error')
const { newLogger } = require('testHelpers')

describe('Consumer > static membership', () => {
  let group, coordinator, cluster

  beforeEach(() => {
    coordinator = {
      joinGroup: jest.fn().mockResolvedValue({
        memberId: 'member',
        leaderId: 'other',
        generationId: 1,
        members: [],
      }),
      syncGroup: jest.fn().mockResolvedValue({
        memberAssignment: MemberAssignment.encode({ version: 0, assignment: { topic: [0] } }),
      }),
      heartbeat: jest.fn(),
      leaveGroup: jest.fn(),
      offsetCommit: jest.fn(),
      isConnected: jest.fn().mockReturnValue(true),
    }
    cluster = {
      findGroupCoordinator: jest.fn().mockResolvedValue(coordinator),
      findTopicPartitionMetadata: jest.fn().mockReturnValue([{ partitionId: 0 }]),
      committedOffsets: jest.fn().mockReturnValue({}),
      defaultOffset: jest.fn().mockReturnValue('0'),
    }
    group = new ConsumerGroup({
      logger: newLogger(),
      cluster,
      topics: ['topic'],
      topicConfigurations: { topic: { fromBeginning: true } },
      assigners: [],
      groupId: 'group',
      groupInstanceId: 'worker-0',
      autoCommit: true,
      instrumentationEmitter: new InstrumentationEventEmitter(),
    })
  })

  test('propagates identity through join, sync, heartbeat and every offset commit path', async () => {
    await group.joinAndSync()
    await group.heartbeat({ interval: 0 })
    const topics = [{ topic: 'topic', partitions: [{ partition: 0, offset: '42' }] }]
    await group.commitOffsets({ topics })
    await group.offsetManager.seek({ topic: 'topic', partition: 0, offset: '1' })
    await group.offsetManager.setDefaultOffset({ topic: 'topic', partition: 0 })

    for (const method of ['joinGroup', 'syncGroup', 'heartbeat', 'offsetCommit']) {
      for (const [payload] of coordinator[method].mock.calls) {
        expect(payload).toMatchObject({ groupInstanceId: 'worker-0' })
      }
      expect(coordinator[method]).toHaveBeenCalled()
    }
    expect(coordinator.offsetCommit).toHaveBeenCalledTimes(3)
  })

  test('does not leave on stop and rejoins with the same instance and an empty member id', async () => {
    await group.joinAndSync()
    await group.leave()
    expect(coordinator.leaveGroup).not.toHaveBeenCalled()
    await group.joinAndSync()
    expect(coordinator.joinGroup).toHaveBeenLastCalledWith(
      expect.objectContaining({ memberId: '', groupInstanceId: 'worker-0' })
    )
  })

  test('dynamic members still leave the group', async () => {
    group.groupInstanceId = null
    await group.joinAndSync()
    await group.leave()
    expect(coordinator.leaveGroup).toHaveBeenCalledWith({ groupId: 'group', memberId: 'member' })
  })

  test('recovers from unknown member id while retaining its static identity', async () => {
    coordinator.joinGroup.mockRejectedValueOnce(createErrorFromCode(25))
    await group.joinAndSync()
    expect(coordinator.joinGroup).toHaveBeenCalledTimes(2)
    expect(coordinator.joinGroup).toHaveBeenLastCalledWith(
      expect.objectContaining({ memberId: '', groupInstanceId: 'worker-0' })
    )
  })

  test('does not retry fencing', async () => {
    coordinator.joinGroup.mockRejectedValueOnce(createErrorFromCode(82))
    await expect(group.joinAndSync()).rejects.toMatchObject({
      type: 'FENCED_INSTANCE_ID',
      retriable: false,
    })
    expect(coordinator.joinGroup).toHaveBeenCalledTimes(1)
  })
})
