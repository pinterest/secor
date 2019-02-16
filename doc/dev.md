# Secor developer notes

## Dependencies

### Bumping to new default kafka version 

  - Make new profile if it does not exist, matching kafka version as id tag.

  - Ensure the new profile is the only one active pr default with
    `activation->activeByDefault`.

  - Change release profile to have the same stuff in it's profile. (used for
    buildservers who signs artifacts with GPG and attach javadocs and pushes to
    public M2 repository).

  - Update MVN_PROFILE in `Makefile`

  - Update .travis and add it into the matrix, so CI runs tests for your kafka
    version profile.

### Profit: scala versions

Ensure you check `mvn dependency:tree` and ensure you don't pull in libraries
compiled with different scala versions, this is bad.
