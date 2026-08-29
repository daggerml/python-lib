## REMOVED Requirements

### Requirement: Shared public flag names do not create ambiguous CLI behavior

**Reason**: The generated CLI will no longer keep duplicate same-name/same-type classmethod options when a constructor-derived root option already provides the value. This removes the ambiguity rather than preserving two distinguishable parser scopes.

**Migration**: Use the constructor-derived root option before the classmethod command. For example, use `dml --remote-root <uri> init` instead of `dml init --remote-root <uri>`.
