const { lookup } = require('../index')
const apiKeys = require('../apiKeys')
const definition = require('./index')
const { decode, parse } = require('./v7/response')

describe('Protocol > OffsetCommit > static membership', () => {
  test.each([6, 7])('encodes the v%i wire format', async version => {
    const request = definition.protocol({ version })({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      groupInstanceId: 's',
      topics: [{ topic: 't', partitions: [{ partition: 0, offset: '42' }] }],
    }).request
    // group, generation, member, optional instance, topic, partition, offset, epoch, metadata
    const expected =
      '0001670000000100016d' +
      (version === 7 ? '000173' : '') +
      '000000010001740000000100000000000000000000002affffffffffff'
    expect((await request.encode()).buffer).toEqual(Buffer.from(expected, 'hex'))
    expect(request.apiVersion).toBe(version)
  })

  test('encodes null identity, explicit leader epoch and metadata', async () => {
    const request = definition.protocol({ version: 7 })({
      groupId: 'g',
      groupGenerationId: 1,
      memberId: 'm',
      topics: [
        { topic: 't', partitions: [{ partition: 0, offset: '42', leaderEpoch: 2, metadata: 'x' }] },
      ],
    }).request
    expect((await request.encode()).buffer).toEqual(
      Buffer.from(
        '0001670000000100016dffff000000010001740000000100000000000000000000002a00000002000178',
        'hex'
      )
    )
  })

  test.each([5, 6, 7])('negotiates OffsetCommit v%i', maxVersion => {
    const protocol = lookup({ [apiKeys.OffsetCommit]: { minVersion: 0, maxVersion } })(
      apiKeys.OffsetCommit,
      definition
    )
    expect(protocol({ topics: [] }).request.apiVersion).toBe(maxVersion)
  })

  test('decodes and propagates fencing on an offset commit', async () => {
    const data = await decode(Buffer.from('000000000000000100017400000001000000000052', 'hex'))
    await expect(parse(data)).rejects.toMatchObject({
      type: 'FENCED_INSTANCE_ID',
      retriable: false,
    })
  })
})
