## Purpose

Defines dagclass instances as self-contained compiled namespaces whose members and delayed references have stable meaning outside the DAG where they are later executed.

## ADDED Requirements

### Requirement: Dagclass instantiation compiles the instance
Creating a dagclass instance SHALL evaluate its declared attributes and compile its members before returning the instance. Compilation SHALL NOT be deferred until `api.run()` or until a compiled member is staged in another DAG.

#### Scenario: Instance is compiled before direct member use
- **WHEN** a user instantiates a valid dagclass and directly obtains one of its methods
- **THEN** the obtained member is already compiled and can be staged without invoking `api.run()`

### Requirement: Dagclass members form a self-contained namespace
Compilation SHALL initialize a member namespace from the instance's evaluated attributes. It SHALL then process dependent members in topological order, resolve their dagclass-local references against members already present in that namespace, and add each compiled member to the namespace as compilation proceeds.

#### Scenario: Method references evaluated attributes
- **WHEN** a dagclass method references evaluated attributes of its instance
- **THEN** the compiled method binds those exact attribute values into its self-contained member graph

#### Scenario: Method references an earlier compiled member
- **WHEN** a dagclass method depends on another member that precedes it in dependency order
- **THEN** compilation binds the already compiled member from the dagclass namespace and adds the dependent method afterward

#### Scenario: Transitive member graph is exported
- **WHEN** a compiled member depends transitively on other dagclass members
- **THEN** direct use of that member carries the complete transitive member graph without requiring those names in a caller DAG

### Requirement: Dagclass references do not resolve against caller DAGs
Every `api.ref(name)` within a dagclass member graph SHALL be interpreted as a reference to the dagclass namespace. Staging or calling a compiled member in another DAG SHALL NOT resolve that reference through a same-named node in the caller DAG.

#### Scenario: Caller contains colliding names
- **WHEN** a compiled dagclass method is staged in a DAG containing nodes whose names match dagclass attributes
- **THEN** execution uses the values captured from the dagclass instance rather than the caller DAG nodes

#### Scenario: Caller omits dagclass names
- **WHEN** a compiled dagclass method is staged in a DAG without nodes named for its internal dependencies
- **THEN** staging and execution do not require those caller nodes

### Requirement: Unknown or unavailable dagclass references fail compilation
Compilation SHALL fail when a member contains an `api.ref(name)` for a name that is not present in the partially built dagclass namespace at the point that member is compiled. Dependency cycles SHALL also fail compilation rather than producing an open or caller-resolved graph.

#### Scenario: External funk references an undeclared name
- **WHEN** an externally defined funk assigned to a dagclass attribute contains `api.ref(name)` and `name` is not an available dagclass member
- **THEN** instantiating the dagclass raises a compilation error identifying the invalid reference

#### Scenario: External funk references a known attribute
- **WHEN** an externally defined funk assigned to a dagclass attribute contains `api.ref(name)` and `name` is already available in the dagclass namespace
- **THEN** compilation binds that reference to the dagclass member

#### Scenario: Member dependency cycle exists
- **WHEN** dagclass members form a reference cycle
- **THEN** instantiating the dagclass raises a compilation error identifying the cycle

### Requirement: api.run executes the compiled entrypoint
`api.run(instance, ...)` SHALL require a compiled dagclass instance and SHALL execute its configured compiled entrypoint. It SHALL NOT compile or reinterpret the instance's member references against the runtime DAG.

#### Scenario: Run a compiled dagclass
- **WHEN** `api.run()` receives a valid compiled dagclass instance
- **THEN** it executes and commits the configured entrypoint using the namespace bindings established at instantiation

#### Scenario: Run receives an uncompiled instance
- **WHEN** `api.run()` receives a dagclass-marked object that has not completed compilation
- **THEN** it raises an error rather than compiling the object implicitly
