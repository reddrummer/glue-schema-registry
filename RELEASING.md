# Releasing `@drumwave/glue-schema-registry`

Releases are automated with [release-please](https://github.com/googleapis/release-please)
and published to the **AWS CodeArtifact** npm repository `common-library-npm`
in domain `drumwave-prod` (account `127266044500`, region `us-east-1`).

## How a release happens

1. Merge conventional-commit changes (`fix:`, `feat:`, …) to `main`.
2. release-please opens/updates a **release PR** that bumps the version and
   updates the changelog.
3. Merging that release PR creates a GitHub Release and tag. The same workflow
   then assumes an AWS IAM role via OIDC, authenticates npm to CodeArtifact, and
   runs `npm publish` — publishing the new version to `common-library-npm`.

## One-time setup (required before the first CodeArtifact publish)

### Repository variable

Settings ▸ Secrets and variables ▸ Actions ▸ **Variables**:

| Variable               | Value                                                       |
| ---------------------- | ----------------------------------------------------------- |
| `AWS_PUBLISH_ROLE_ARN` | `arn:aws:iam::127266044500:role/<github-oidc-publish-role>` |

### IAM role for GitHub OIDC

A role in account `127266044500` whose trust policy allows
`sts:AssumeRoleWithWebIdentity` from `token.actions.githubusercontent.com` with
`token.actions.githubusercontent.com:sub` like
`repo:reddrummer/glue-schema-registry:*`, and whose permissions allow
`codeartifact:GetAuthorizationToken`, `GetRepositoryEndpoint`,
`ReadFromRepository`, `PublishPackageVersion` on the `drumwave-prod` domain /
`common-library-npm` repo, plus `sts:GetServiceBearerToken`. (The same role
shape is documented in the `data-logistics.contract-first-kafka` repo; reuse one
role for both by including both repos in the trust `sub` condition.)
