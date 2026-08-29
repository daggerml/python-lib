## Purpose
Define the supported typed Python transaction boundary over the native database.

## Requirements

### Requirement: DB Python APIs SHALL not expose raw payload read or write helpers
The supported Python DB transaction surfaces SHALL expose only typed object reads and writes. Their runtime signatures and type stubs SHALL agree and SHALL NOT expose raw payload flags, raw helper operations, or handle-era exception types that no current native operation can return.

#### Scenario: Typed transaction surface exposes only typed object reads and writes
- **WHEN** callers use the supported Python DB transaction APIs
- **THEN** reads decode persisted payloads through normal typed object handling
- **AND** writes persist values through normal typed object serialization
- **AND** no supported `get_raw`, `put_raw`, or `raw=True` transaction API exists

#### Scenario: Type declarations match runtime operations
- **WHEN** a caller or type checker inspects the DB transaction interface
- **THEN** every declared argument and exception corresponds to a reachable runtime operation

#### Scenario: Current Python validation error remains
- **WHEN** Python-to-native value validation encounters an unsupported value type
- **THEN** the current directly raised typed validation error remains available even though the unreachable native type-invalid return code is removed
