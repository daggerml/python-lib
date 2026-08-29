## Context

`src/daggerml/_cli.py` builds the `dml` command tree dynamically from the `Dml` class, its constructor, public methods, root classmethods, and namespace properties. Constructor parameters are currently added as root-level options using internal destinations prefixed with `_init_`, while classmethod parameters are generated independently on the command parser.

For `Dml.init`, this means parameters such as `remote_root`, `user`, and `config_home` appear both as constructor-derived root options and as command-local `init` options. The duplicate grammar is not dynamic in a coherent way: the CLI generator treats matching constructor and classmethod parameters as unrelated even when they have the same name and resolved type.

## Goals / Non-Goals

**Goals:**

- Keep the CLI generated from the runtime-visible `Dml` signatures so future `Dml` constructor/classmethod changes automatically update the CLI surface.
- For root classmethods, dynamically intersect classmethod parameters with constructor parameters only when name and resolved type match.
- Expose intersected parameters only through constructor-derived root options.
- Invoke classmethods with intersected values sourced from the parsed constructor/root options.
- Keep internal parser destinations collision-safe while presenting clean user-visible metavars.
- Limit implementation scope to `src/daggerml/_cli.py` and corresponding contracts/docs.

**Non-Goals:**

- Do not hand-code special cases for `Dml.init` or any other specific classmethod.
- Do not change instance method behavior.
- Do not change type parsing, union priority, output serialization, or domain initialization behavior.
- Do not move CLI behavior into other modules.

## Decisions

### Compare Resolved Parameter Types

The intersection check will use the same resolved type information already used for CLI generation, including `typing.get_type_hints(..., include_extras=True)` and the existing `Annotated` split behavior. A classmethod parameter intersects a constructor parameter only when both the raw parameter name and resolved base type match.

Alternative considered: intersect by name only. That was rejected because `Dml.__init__.project_home` and `Dml.init.project_home` intentionally differ in type (`str | None` versus `str`), and name-only matching would incorrectly remove the command-local `init --project-home` option.

### Keep Constructor Options as the Only Public Spelling

When a classmethod parameter intersects with a constructor parameter, the command parser will omit that parameter. The root parser remains the only public place to provide the value.

Alternative considered: keep both root and command-local spellings with precedence rules. That was rejected because it preserves duplicate grammar and requires explicit conflict/default semantics. Option A intentionally favors one canonical spelling.

### Preserve Internal Destination Separation

Constructor arguments can keep collision-safe internal destinations distinct from command-local destinations. The user-visible help must not expose those internal names as metavars.

Alternative considered: remove the internal destination prefix entirely. That was rejected because `argparse` uses a shared namespace across parent and subparser actions; duplicate destinations allow child defaults to overwrite root values.

### Route Intersected Values at Dispatch Time

Dispatch should build constructor-derived data once, then pass the intersected subset into classmethod calls as keyword arguments. Instance method dispatch should continue instantiating `Dml` from constructor-derived data and invoking instance methods with only command-local arguments.

Alternative considered: instantiate `Dml` before calling classmethods and route through instance methods. That was rejected because root classmethods are intentionally class-level workflows such as repository initialization.

## Risks / Trade-offs

- **Breaking CLI grammar for duplicate classmethod options** -> Document the canonical root-option spelling and cover `dml --remote-root ... init` in contract tests.
- **Type comparison drift from CLI generation behavior** -> Reuse existing annotation resolution and `Annotated` normalization helpers rather than adding a separate type model.
- **Hidden argparse default collisions** -> Preserve distinct internal destinations for constructor options and only clean up public metavars.
- **Future constructor/classmethod signature changes create surprising intersections** -> Treat this as the desired dynamic behavior and require tests with small fixture classes in addition to `Dml.init` coverage.
