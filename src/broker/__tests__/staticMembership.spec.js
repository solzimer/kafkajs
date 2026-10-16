const Broker = require('../index')
const { lookup } = require('../../protocol/requests')
const apiKeys = require('../../protocol/requests/apiKeys')
const { newLogger } = require('testHelpers')
const { createErrorFromCode } = require('../../protocol/error')
const { KafkaJSMemberIdRequired } = require('../../errors')

describe('Broker > static membership', () => {
  let broker, send, versions

  beforeEach(() => {
    versions = {
      [apiKeys.JoinGroup]: { minVersion: 0, maxVersion: 5 },
      [apiKeys.SyncGroup]: { minVersion: 0, maxVersion: 3 },
      [apiKeys.Heartbeat]: { minVersion: 0, maxVersion: 3 },
      [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion: 7 },
    }
    send = jest.fn().mockResolvedValue({})
    broker = new Broker({
      connectionPool: { send, connectionTimeout: 1000 },
      logger: newLogger(),
      versions,
    })
    broker.lookupRequest = lookup(versions)
  })

  test.each(['joinGroup', 'syncGroup', 'heartbeat', 'offsetCommit'])(
    '%s transmits the static identity',
    async method => {
      await broker[method]({
        groupId: 'group',
        groupInstanceId: 'worker-0',
        memberId: 'member',
        generationId: 1,
        groupGenerationId: 1,
        sessionTimeout: 30000,
        rebalanceTimeout: 60000,
        groupProtocols: [],
        groupAssignment: [],
        topics: [],
      })
      const { buffer } = await send.mock.calls[0][0].request.encode()
      expect(buffer.includes(Buffer.from('worker-0'))).toBe(true)
    }
  )

  test.each(['JoinGroup', 'SyncGroup', 'Heartbeat', 'OffsetCommit'])(
    'rejects an incompatible %s before joining',
    async apiName => {
      versions[apiKeys[apiName]].maxVersion--
      await expect(
        broker.joinGroup({ groupId: 'group', groupInstanceId: 'worker-0' })
      ).rejects.toMatchObject({ retriable: false })
      expect(send).not.toHaveBeenCalled()
    }
  )

  test('dynamic members can use older brokers', async () => {
    versions[apiKeys.JoinGroup].maxVersion = 0
    await broker.joinGroup({ groupId: 'group' })
    expect(send.mock.calls[0][0].request.apiVersion).toBe(0)
  })

  test('retains static identity when retrying with an assigned member id', async () => {
    send.mockRejectedValueOnce(
      new KafkaJSMemberIdRequired(createErrorFromCode(79), { memberId: 'assigned' })
    )
    await broker.joinGroup({ groupId: 'group', groupInstanceId: 'worker-0', groupProtocols: [] })
    const { buffer } = await send.mock.calls[1][0].request.encode()
    expect(buffer.includes(Buffer.from('worker-0'))).toBe(true)
    expect(buffer.includes(Buffer.from('assigned'))).toBe(true)
  })

  test('propagates fencing without retrying', async () => {
    send.mockRejectedValueOnce(createErrorFromCode(82))
    await expect(
      broker.joinGroup({ groupId: 'group', groupInstanceId: 'worker-0' })
    ).rejects.toMatchObject({ type: 'FENCED_INSTANCE_ID', retriable: false })
    expect(send).toHaveBeenCalledTimes(1)
  })
})
