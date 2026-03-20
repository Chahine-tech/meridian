// Pre-baked admin token for test-ns (seed=0x42*32, expires year 2286).
// Claims: { namespace: "test-ns", client_id: 1, expires_at: 9_999_999_999_999, admin: true }
// Signed with: TokenSigner::from_bytes(&[0x42u8; 32]).sign(&claims)
export const ADMIN_TOKEN =
  "hKluYW1lc3BhY2WndGVzdC1uc6ljbGllbnRfaWQBqmV4cGlyZXNfYXTPAAAJGE5yn_-rcGVybWlzc2lvbnODpHJlYWSRoSqld3JpdGWRoSqlYWRtaW7D.f4E4PZNShVkIXr8D7SfG4vIF4piaFwCZp5Di9Vs_7Ff2_3cTfdGcRYaLF0WFK_dol85IXcT-FUa24r1GbKn_AA";

export const TEST_NS = "test-ns";
export const BASE_URL = "http://worker";
