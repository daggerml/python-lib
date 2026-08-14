## ADDED Requirements

### Requirement: Dagclass method dependencies are inferred syntactically
Compilation SHALL infer a dependency edge from a dagclass method to each dagclass member named by a direct, non-reserved `self.<name>` attribute load in that method body. Compilation SHALL validate that the method and referenced member are both present in the dagclass member collection, and SHALL fail rather than create an edge with an unknown endpoint.

#### Scenario: Method loads a declared member
- **WHEN** a dagclass method body loads `self.transform` and `transform` is a non-reserved member of the dagclass collection
- **THEN** compilation adds an edge from the method to `transform`

#### Scenario: Method loads an undeclared member
- **WHEN** a dagclass method body loads non-reserved `self.missing`, contains no assignment to `self.missing`, and `missing` is absent from the dagclass member collection
- **THEN** compilation fails with an error identifying the unknown member reference

### Requirement: Dag-resolved attributes are excluded from dependency topology
Compilation SHALL treat a name as reserved when normal attribute lookup on the worker `daggerml.api.Dag` resolves that name before named-node fallback through `Dag.__getattr__`. A direct `self.<reserved-name>` access SHALL NOT create a dependency edge. A dagclass declaration SHALL NOT define a member with a reserved name, and a method SHALL NOT assign to a reserved `self.<name>` as though it were a named node.

#### Scenario: Method calls a Dag operation
- **WHEN** a dagclass method calls `self.put(value)`
- **THEN** compilation adds no dependency edge for `put` and runtime Python lookup resolves `self.put` as `Dag.put`

#### Scenario: Method accesses a Dag field or property
- **WHEN** a dagclass method accesses a public field or property resolved directly by its worker `Dag`
- **THEN** compilation adds no dependency edge for that attribute

#### Scenario: Dagclass declares a reserved member
- **WHEN** a dagclass declaration uses a name resolved directly by the worker `Dag`
- **THEN** compilation fails with an error identifying the reserved name

#### Scenario: Method assigns a reserved attribute
- **WHEN** a dagclass method assigns to `self.put` or another name resolved directly by the worker `Dag`
- **THEN** compilation fails rather than treating the assignment as named-node creation

### Requirement: Any method-body assignment excludes the assigned name
If a dagclass method body contains any direct assignment to non-reserved `self.<name>`, compilation SHALL exclude that name from the method's dependency edges even when the same method also loads the attribute. Compilation SHALL apply this exclusion across the complete method body without evaluating source ordering, reachability, branches, loops, or definite assignment. The assigned name SHALL NOT need to be declared as a dagclass member.

#### Scenario: Method assigns and then loads a named node
- **WHEN** a method assigns `self.output = value` and later loads `self.output`
- **THEN** compilation adds no dependency edge for `output`

#### Scenario: Assignment occurs on only one control-flow path
- **WHEN** any control-flow path in a method body contains an assignment to `self.output` and another expression loads `self.output`
- **THEN** compilation adds no dependency edge for `output` without proving that the assignment executes first

#### Scenario: Method assigns an undeclared name
- **WHEN** a method assigns to non-reserved `self.output` and `output` is absent from the dagclass member collection
- **THEN** compilation accepts the name without creating or validating a dependency edge for it

#### Scenario: Runtime order violates the compiler assumption
- **WHEN** a method loads a name before the method has created that named node at runtime but contains an assignment to the same name elsewhere in its body
- **THEN** compilation adds no dependency edge and the runtime named-node lookup reports the missing node

### Requirement: Item access is outside dagclass dependency inference
Compilation SHALL ignore every `self[...]` item load and item assignment when inferring dagclass dependency edges and assignment exclusions. Compilation SHALL NOT interpret a literal item key as a member name.

#### Scenario: Method loads a declared member through item access
- **WHEN** a method uses `self["transform"]` and `transform` is a declared dagclass member
- **THEN** compilation adds no dependency edge for `transform`

#### Scenario: Method assigns through item access
- **WHEN** a method assigns `self["output"] = value`
- **THEN** compilation records neither a dependency edge nor an attribute-assignment exclusion for `output`
