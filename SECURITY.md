# Security policy

## Supported versions

MeteorDB is pre-alpha. Released versions do not have a long-term security
support window, and the file format and API may change without migration
support. Security reports that reproduce against the current `main` branch are
reviewed.

## Report a vulnerability

Use GitHub's private vulnerability reporting for this repository:

[Report a vulnerability privately](https://github.com/shresthhh/MeteorDB/security/advisories/new)

Do not open a public issue, discussion, or pull request for a suspected
vulnerability. If private vulnerability reporting is unavailable, do not
publish the report; use a private contact method listed on a maintainer's
GitHub profile to ask for a secure reporting channel.

Include:

- the affected commit or version;
- the impact and conditions required to trigger it;
- minimal reproduction steps or a proof of concept;
- relevant platform and durability configuration; and
- any known mitigations.

Do not include credentials, access tokens, private database contents, or other
secrets. Redact sensitive logs and test data before submitting the report.

Maintainers will acknowledge reports, assess impact against current `main`,
and coordinate disclosure and remediation through the private advisory.
