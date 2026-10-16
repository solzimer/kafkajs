const Encoder = require('../../../encoder')
const { OffsetCommit: apiKey } = require('../../apiKeys')

/**
 * OffsetCommit v6 adds committed_leader_epoch to each partition.
 * @param {object} params Request fields.
 * @param {string} params.groupId Consumer group.
 * @param {number} params.groupGenerationId Group generation.
 * @param {string} params.memberId Coordinator-assigned member identity.
 * @param {Array} params.topics Topic offsets to commit.
 * @returns {object} Encodable request.
 */
module.exports = ({ groupId, groupGenerationId, memberId, topics }) => ({
  apiKey,
  apiVersion: 6,
  apiName: 'OffsetCommit',
  /** @returns {Promise<Encoder>} Encoded request body. */
  encode: async () =>
    new Encoder()
      .writeString(groupId)
      .writeInt32(groupGenerationId)
      .writeString(memberId)
      .writeArray(topics.map(encodeTopic)),
})

/**
 * Encode offsets for one topic.
 * @param {object} topic Topic and partition offsets.
 * @returns {Encoder} Encoded topic.
 */
const encodeTopic = ({ topic, partitions }) =>
  new Encoder().writeString(topic).writeArray(partitions.map(encodePartition))

/**
 * Encode a partition offset, using an unknown leader epoch when unspecified.
 * @param {object} partition Partition offset and optional metadata and leader epoch.
 * @returns {Encoder} Encoded partition.
 */
const encodePartition = ({ partition, offset, metadata = null, leaderEpoch = -1 }) =>
  new Encoder()
    .writeInt32(partition)
    .writeInt64(offset)
    .writeInt32(leaderEpoch)
    .writeString(metadata)
