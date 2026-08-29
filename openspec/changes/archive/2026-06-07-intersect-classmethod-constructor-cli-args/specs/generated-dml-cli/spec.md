## ADDED Requirements

### Requirement: Root classmethods share matching constructor parameters dynamically

The generated CLI SHALL derive root classmethod command arguments from runtime-visible signatures and SHALL intersect classmethod parameters with constructor parameters when the parameter names match and their resolved base types match.

Intersected classmethod parameters SHALL be exposed only through the constructor-derived root option surface and SHALL NOT be exposed again on the classmethod command parser. Non-intersecting classmethod parameters SHALL continue to be exposed on the classmethod command parser according to the normal generated argument rules.

#### Scenario: Same-name same-type classmethod parameters are omitted from command-local help

- **WHEN** a root classmethod has parameters with the same names and resolved base types as constructor parameters
- **THEN** the generated classmethod command help omits those parameters from its command-local arguments and options
- **AND** the generated root help continues to expose the corresponding constructor-derived root options

#### Scenario: Same-name different-type classmethod parameters remain command-local

- **WHEN** a root classmethod parameter has the same name as a constructor parameter but a different resolved base type
- **THEN** the generated classmethod command keeps that parameter as a command-local argument or option

#### Scenario: Intersected classmethod values come from root options

- **WHEN** a user invokes a root classmethod command and supplies an intersected parameter through the root option surface
- **THEN** the CLI invokes the classmethod with that parsed value using the classmethod parameter name

#### Scenario: Init remote_root is supplied from root remote-root

- **WHEN** `Dml.__init__` and `Dml.init` both expose `remote_root` with the same resolved base type
- **THEN** `dml init --remote-root <uri>` is not part of the generated command grammar
- **AND** `dml --remote-root <uri> init` invokes `Dml.init(remote_root=<uri>, ...)`

#### Scenario: Init project_home remains command-local

- **WHEN** `Dml.__init__` exposes `project_home` as `str | None` and `Dml.init` exposes `project_home` as `str`
- **THEN** `project_home` does not intersect
- **AND** `dml init --project-home <path>` remains part of the generated command grammar

### Requirement: Constructor option metavars hide internal destinations

The generated CLI SHALL NOT expose internal constructor destination prefixes such as `_init_` in user-visible help or usage metavars. Internal parser destinations MAY remain prefixed or otherwise distinct when needed to avoid parser namespace collisions.

#### Scenario: Root constructor option usage shows public metavar

- **WHEN** a user views root `dml --help`
- **THEN** constructor-derived options show public metavars based on the option name, such as `REMOTE_ROOT`
- **AND** constructor-derived options do not show `_INIT_REMOTE_ROOT` or other internal destination names

#### Scenario: Internal destination choices do not affect public help

- **WHEN** the CLI uses an internal destination to distinguish root constructor options from command-local options
- **THEN** generated help and usage still display only public option names and public metavars
