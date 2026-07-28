import daggerml as dml
from daggerml.contrib import api


@api.funkify
def err(dag, arg):
    return 1 / arg.value()


def main() -> None:
    dag = dml.new(name="examples/00-errors")
    with dag:
        # dag.error_instance = dml.Error("This is an error instance", origin="example", type="example-error")
        dag.err_fn = err
        dag.err_fn(0, name="bad")
        dag.commit(dag.err_fn(23, name="good"))  # should never run


if __name__ == "__main__":
    main()
