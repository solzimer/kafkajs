const { KafkaJSNonRetriableError } = require('../errors')
const apiKeys = require('../protocol/requests/apiKeys')

/**
 * Reject static membership before sending requests that could discard its identity.
 * @param {import('../../types').ApiVersions} versions Broker API versions.
 * @param {string | null} groupInstanceId Configured static identity.
 * @returns {void}
 */
module.exports = (versions, groupInstanceId) => {
  if (groupInstanceId == null) {
    return
  }

  for (const [apiName, minimumVersion] of [
    ['JoinGroup', 5],
    ['SyncGroup', 3],
    ['Heartbeat', 3],
    ['OffsetCommit', 7],
  ]) {
    const version = versions && versions[apiKeys[apiName]]
    if (!version || version.maxVersion == null || version.maxVersion < minimumVersion) {
      throw new KafkaJSNonRetriableError(
        `Static membership requires ${apiName} v${minimumVersion} or later (groupInstanceId: ${groupInstanceId})`
      )
    }
  }
}
